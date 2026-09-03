@TestOn("vm")
library;

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:test/test.dart";

import "support/scripted_nats_peer.dart";

void main() {
  test("publish encodes typed TTL and wildcard subject constraint", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final jetStream = JetStreamContext(harness.connection);
    final callerHeaders = NatsHeaders(
      entries: const [MapEntry("Caller", "unchanged")],
    );

    final result = jetStream.publish(
      "orders.customer.one",
      const [1, 2, 3],
      headers: callerHeaders,
      options: JetStreamPublishOptions(
        expectedLastSubjectSequence: BigInt.from(41),
        expectedLastSubjectSequenceSubject: "orders.customer.*",
        lifetime: JetStreamMessageLifetime.expiresAfter(
          const Duration(seconds: 2),
        ),
      ),
    );
    final publication = await harness.peer.nextPublication(
      "orders.customer.one",
    );
    final headers = NatsHeaders.parse(publication.headerBytes);
    expect(publication.command, "HPUB");
    expect(headers.first("Caller"), "unchanged");
    expect(headers.first("Nats-TTL"), "2s");
    expect(headers.first("Nats-Expected-Last-Subject-Sequence"), "41");
    expect(
      headers.first("Nats-Expected-Last-Subject-Sequence-Subject"),
      "orders.customer.*",
    );
    expect(callerHeaders.first("Nats-TTL"), isNull);
    expect(callerHeaders.first("Nats-Expected-Last-Subject-Sequence"), isNull);

    await harness.peer.sendMessage(
      publication.replySubject!,
      encodeExactJson({"stream": "ORDERS", "seq": BigInt.from(42)}),
    );
    expect((await result).sequence, BigInt.from(42));
  });

  test("typed TTL rejects caller ownership of Nats TTL", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final jetStream = JetStreamContext(harness.connection);

    await expectLater(
      jetStream.publish(
        "orders.one",
        const [],
        headers: NatsHeaders(entries: const [MapEntry("Nats-TTL", "5s")]),
        options: JetStreamPublishOptions(
          lifetime: const JetStreamMessageLifetime.never(),
        ),
      ),
      throwsArgumentError,
    );
    expect(harness.peer.publications, isEmpty);
  });

  test(
    "publish rejects another stream against a raw expectation header",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final result = JetStreamContext(harness.connection).publish(
        "orders.one",
        const [1],
        headers: NatsHeaders(
          entries: const [MapEntry("Nats-Expected-Stream", "ORDERS")],
        ),
      );
      final publication = await harness.peer.nextPublication("orders.one");
      expect(
        NatsHeaders.parse(
          publication.headerBytes,
        ).first("Nats-Expected-Stream"),
        "ORDERS",
      );
      await harness.peer.sendMessage(
        publication.replySubject!,
        encodeExactJson({"stream": "OTHER", "seq": BigInt.one}),
      );

      await expectLater(
        result,
        throwsA(
          isA<JetStreamProtocolException>().having(
            (error) => error.fieldPath,
            "fieldPath",
            "stream",
          ),
        ),
      );
    },
  );

  test(
    "async publish retains raw stream expectation and never counts mismatch",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final publisher = JetStreamContext(harness.connection)
          .createAsyncPublisher(
            options: JetStreamAsyncPublisherOptions(
              retryPolicy: JetStreamAsyncPublishRetryPolicy.none,
              maximumAttempts: 1,
            ),
          );
      final pending = publisher.publish(
        "orders.async",
        const [2],
        headers: NatsHeaders(
          entries: const [MapEntry("Nats-Expected-Stream", "ORDERS")],
        ),
      );
      final publication = await harness.peer.nextPublication("orders.async");
      await harness.peer.sendMessage(
        publication.replySubject!,
        encodeExactJson({"stream": "OTHER", "seq": BigInt.one}),
      );

      await expectLater(
        pending.acknowledgement,
        throwsA(
          isA<JetStreamAsyncPublishException>()
              .having(
                (error) => error.cause,
                "cause",
                isA<JetStreamProtocolException>().having(
                  (error) => error.fieldPath,
                  "fieldPath",
                  "stream",
                ),
              )
              .having(
                (error) => error.disposition,
                "disposition",
                JetStreamPublishDisposition.possiblyStored,
              ),
        ),
      );
      expect(pending.state, JetStreamAsyncPublishState.failed);
      try {
        await publisher.drain();
        fail("Publisher drain unexpectedly succeeded");
      } on JetStreamAsyncPublisherDrainException catch (error) {
        expect(error.acknowledged, 0);
        expect(error.definitelyRejected, 0);
        expect(error.ambiguous, 1);
        expect(error.firstCause, isA<JetStreamProtocolException>());
      }
    },
  );

  final specialized =
      <
        ({
          String name,
          String subject,
          Future<Object> Function(JetStreamContext context) start,
        })
      >[
        (
          name: "counter",
          subject: "counter.one",
          start: (context) => context.incrementCounter(
            "counter.one",
            BigInt.one,
            headers: _expectedOrders,
          ),
        ),
        (
          name: "schedule",
          subject: "schedule.one",
          start: (context) => context.scheduleMessage(
            "schedule.one",
            const [1],
            options: JetStreamScheduleOptions(
              schedule: JetStreamSchedule.every(const Duration(seconds: 1)),
              target: "target.one",
            ),
            headers: _expectedOrders,
          ),
        ),
        (
          name: "schedule cancellation",
          subject: "cancel.one",
          start: (context) => context.cancelSchedule(
            "cancel.one",
            "schedule.one",
            expectedScheduleSequence: BigInt.one,
            headers: _expectedOrders,
          ),
        ),
      ];
  for (final scenario in specialized) {
    test("${scenario.name} binds a raw expected stream", () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final result = scenario.start(JetStreamContext(harness.connection));
      final publication = await harness.peer.nextPublication(scenario.subject);
      expect(
        NatsHeaders.parse(
          publication.headerBytes,
        ).first("Nats-Expected-Stream"),
        "ORDERS",
      );
      await harness.peer.sendMessage(
        publication.replySubject!,
        encodeExactJson({
          "stream": "OTHER",
          "seq": BigInt.one,
          if (scenario.name == "counter") "val": "1",
        }),
      );

      await expectLater(
        result,
        throwsA(
          isA<JetStreamProtocolException>().having(
            (error) => error.fieldPath,
            "fieldPath",
            "stream",
          ),
        ),
      );
    });
  }

  test(
    "counter decimal bound is explicit and checked before publication",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final acceptedContext = JetStreamContext(
        harness.connection,
        maximumCounterDigits: 21,
      );
      final increment = BigInt.parse("100000000000000000000");
      final accepted = acceptedContext.incrementCounter(
        "counter.large",
        increment,
      );
      final publication = await harness.peer.nextPublication("counter.large");
      expect(
        NatsHeaders.parse(publication.headerBytes).first("Nats-Incr"),
        "+100000000000000000000",
      );
      await harness.peer.sendMessage(
        publication.replySubject!,
        encodeExactJson({
          "stream": "COUNTERS",
          "seq": BigInt.one,
          "val": "100000000000000000000",
        }),
      );
      expect((await accepted).value, increment);

      final rejectedContext = JetStreamContext(
        harness.connection,
        maximumCounterDigits: 20,
      );
      await expectLater(
        rejectedContext.incrementCounter("counter.rejected", increment),
        throwsArgumentError,
      );
      expect(
        harness.peer.publications.where(
          (publication) => publication.subject == "counter.rejected",
        ),
        isEmpty,
      );
    },
  );

  test("schedule cancellation rejects overflow before publication", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);

    await expectLater(
      JetStreamContext(harness.connection).cancelSchedule(
        "cancel.one",
        "schedule.one",
        expectedScheduleSequence: BigInt.one << 64,
      ),
      throwsArgumentError,
    );
    expect(harness.peer.publications, isEmpty);
  });
}

final NatsHeaders _expectedOrders = NatsHeaders(
  entries: const [MapEntry("Nats-Expected-Stream", "ORDERS")],
);

final class _Harness {
  const _Harness(this.peer, this.connection);

  static Future<_Harness> open() async {
    final peer = await ScriptedNatsPeer.start();
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [NatsServer.parse(peer.uri.toString())],
        reconnect: NatsReconnectPolicy(maximumAttempts: 0),
        pingInterval: const Duration(minutes: 10),
      ),
    );
    return _Harness(peer, connection);
  }

  final ScriptedNatsPeer peer;
  final NatsConnection connection;

  Future<void> close() async {
    await connection.close();
    await peer.close();
  }
}
