@TestOn("vm")
library;

import "dart:async";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:test/test.dart";

import "support/scripted_nats_peer.dart";

void main() {
  test("scoped watcher decodes one matching advisory", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final watcher = await harness.context.watchAtomicBatchAbandonments(
      stream: "EVENTS",
    );
    addTearDown(watcher.close);
    final advisories = StreamIterator(watcher.advisories);
    addTearDown(advisories.cancel);

    expect(
      watcher.subject,
      r"$JS.EVENT.ADVISORY.STREAM.BATCH_ABANDONED.EVENTS",
    );
    final pending = advisories.moveNext();
    await harness.sendAdvisory(batchId: "batch-1", reason: "timeout");

    expect(await pending.timeout(const Duration(seconds: 1)), isTrue);
    expect(advisories.current.stream, "EVENTS");
    expect(advisories.current.batchId, "batch-1");
    expect(
      advisories.current.reason,
      JetStreamAtomicBatchAbandonReason.timeout,
    );
  });

  test("global watcher uses the exact one stream wildcard", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);

    final watcher = await harness.context.watchAtomicBatchAbandonments();
    addTearDown(watcher.close);

    expect(watcher.subject, r"$JS.EVENT.ADVISORY.STREAM.BATCH_ABANDONED.*");
  });

  test("watcher reports subscription permission denial explicitly", () async {
    final harness = await _Harness.open(
      deniedSubscriptions: const {
        r"$JS.EVENT.ADVISORY.STREAM.BATCH_ABANDONED.EVENTS",
      },
    );
    addTearDown(harness.close);

    await expectLater(
      harness.context.watchAtomicBatchAbandonments(stream: "EVENTS"),
      throwsA(
        isA<NatsPermissionException>()
            .having(
              (error) => error.operation,
              "operation",
              NatsPermissionOperation.subscription,
            )
            .having(
              (error) => error.subject,
              "subject",
              r"$JS.EVENT.ADVISORY.STREAM.BATCH_ABANDONED.EVENTS",
            ),
      ),
    );
  });

  test("advisory received before failure proves noncommit", () async {
    final harness = await _Harness.open(
      timeout: const Duration(milliseconds: 50),
    );
    addTearDown(harness.close);
    final result = harness.publishConfirmed(
      confirmationTimeout: const Duration(milliseconds: 200),
    );
    final expectation = expectLater(
      result,
      throwsA(
        isA<JetStreamAtomicBatchAbandonedException>()
            .having((error) => error.advisory.batchId, "batchId", "expected")
            .having(
              (error) => error.advisory.reason.knownKind,
              "reason",
              JetStreamAtomicBatchAbandonKind.timeout,
            ),
      ),
    );
    await harness.peer.nextSubscription(
      r"$JS.EVENT.ADVISORY.STREAM.BATCH_ABANDONED.EVENTS",
    );
    await harness.peer.nextPublication("events");
    await harness.sendAdvisory(batchId: "expected", reason: "timeout");

    await expectation;
  });

  test("advisory received after failure proves noncommit", () async {
    final harness = await _Harness.open(
      timeout: const Duration(milliseconds: 100),
    );
    addTearDown(harness.close);
    final result = harness.publishConfirmed(
      confirmationTimeout: const Duration(milliseconds: 500),
    );
    final expectation = expectLater(
      result,
      throwsA(
        isA<JetStreamAtomicBatchAbandonedException>().having(
          (error) => error.advisory.reason.knownKind,
          "reason",
          JetStreamAtomicBatchAbandonKind.incomplete,
        ),
      ),
    );
    await harness.peer.nextSubscription(
      r"$JS.EVENT.ADVISORY.STREAM.BATCH_ABANDONED.EVENTS",
    );
    final staged = await harness.peer.nextPublication("events");
    await harness.peer.sendMessage(staged.replySubject!, const [123]);
    await Future<void>(() {});
    await harness.sendAdvisory(batchId: "expected", reason: "incomplete");

    await expectation;
  });

  test(
    "lost final acknowledgement without advisory remains ambiguous",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final result = harness.publishConfirmed(
        confirmationTimeout: const Duration(milliseconds: 50),
      );
      final expectation = expectLater(
        result,
        throwsA(
          isA<JetStreamAtomicBatchAmbiguousException>()
              .having((error) => error.stream, "stream", "EVENTS")
              .having((error) => error.batchId, "batchId", "expected")
              .having(
                (error) => error.cause,
                "cause",
                isA<NatsTimeoutException>(),
              ),
        ),
      );
      await harness.peer.nextSubscription(
        r"$JS.EVENT.ADVISORY.STREAM.BATCH_ABANDONED.EVENTS",
      );
      for (var index = 0; index < 2; index += 1) {
        final staged = await harness.peer.nextPublication("events");
        await harness.peer.sendMessage(staged.replySubject!, const []);
      }
      await harness.peer.nextPublication("events");

      await expectation;
    },
  );

  test("nonmatching advisory cannot resolve a lost acknowledgement", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final result = harness.publishConfirmed(
      confirmationTimeout: const Duration(milliseconds: 50),
    );
    final expectation = expectLater(
      result,
      throwsA(isA<JetStreamAtomicBatchAmbiguousException>()),
    );
    await harness.peer.nextSubscription(
      r"$JS.EVENT.ADVISORY.STREAM.BATCH_ABANDONED.EVENTS",
    );
    final staged = await harness.peer.nextPublication("events");
    await harness.sendAdvisory(batchId: "other", reason: "timeout");
    await harness.peer.sendMessage(staged.replySubject!, const []);
    final second = await harness.peer.nextPublication("events");
    await harness.peer.sendMessage(second.replySubject!, const []);
    await harness.peer.nextPublication("events");

    await expectation;
  });

  test(
    "confirmed atomic batch rejects another stream acknowledgement",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final result = harness.publishConfirmed(
        confirmationTimeout: const Duration(milliseconds: 100),
      );
      await harness.peer.nextSubscription(
        r"$JS.EVENT.ADVISORY.STREAM.BATCH_ABANDONED.EVENTS",
      );
      for (var index = 0; index < 2; index += 1) {
        final staged = await harness.peer.nextPublication("events");
        await harness.peer.sendMessage(staged.replySubject!, const []);
      }
      final commit = await harness.peer.nextPublication("events");
      await harness.peer.sendMessage(
        commit.replySubject!,
        encodeExactJson({
          "stream": "OTHER",
          "seq": BigInt.from(2),
          "batch": "expected",
          "count": BigInt.from(2),
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
    },
  );

  for (final scenario in [
    (name: "65 byte ASCII", value: "a" * 65),
    (name: "66 byte multibyte", value: "가" * 22),
  ]) {
    test("rejects ${scenario.name} identifier before request", () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);

      expect(
        () => harness.context.publishAtomicBatch([
          JetStreamAtomicMessage("events", const [1]),
        ], batchId: scenario.value),
        throwsArgumentError,
      );
      expect(harness.peer.publications, isEmpty);
    });
  }

  for (final validId in ["a" * 64, "가" * 21 + "a"]) {
    test("accepts an exact 64 byte identifier", () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final result = harness.context.publishAtomicBatch([
        JetStreamAtomicMessage("events", const [1]),
      ], batchId: validId);
      final staged = await harness.peer.nextPublication("events");
      await harness.peer.sendMessage(staged.replySubject!, const []);
      final commit = await harness.peer.nextPublication("events");
      await harness.peer.sendMessage(
        commit.replySubject!,
        encodeExactJson({
          "stream": "EVENTS",
          "seq": BigInt.one,
          "batch": validId,
          "count": BigInt.one,
        }),
      );

      expect((await result).batchId, validId);
    });
  }
}

final class _Harness {
  const _Harness(this.peer, this.connection, this.context);

  static Future<_Harness> open({
    Duration timeout = const Duration(seconds: 1),
    Set<String> deniedSubscriptions = const {},
  }) async {
    final peer = await ScriptedNatsPeer.start(
      deniedSubscriptions: deniedSubscriptions,
    );
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [NatsServer.parse(peer.uri.toString())],
        reconnect: NatsReconnectPolicy(maximumAttempts: 0),
        pingInterval: const Duration(minutes: 10),
      ),
    );
    return _Harness(
      peer,
      connection,
      JetStreamContext(connection, timeout: timeout),
    );
  }

  final ScriptedNatsPeer peer;
  final NatsConnection connection;
  final JetStreamContext context;

  Future<JetStreamAtomicBatchAck> publishConfirmed({
    required Duration confirmationTimeout,
  }) {
    return context.publishAtomicBatchConfirmed(
      "EVENTS",
      [
        JetStreamAtomicMessage("events", const [1]),
        JetStreamAtomicMessage("events", const [2]),
      ],
      batchId: "expected",
      confirmationTimeout: confirmationTimeout,
    );
  }

  Future<void> sendAdvisory({required String batchId, required String reason}) {
    return peer.sendMessage(
      r"$JS.EVENT.ADVISORY.STREAM.BATCH_ABANDONED.EVENTS",
      encodeExactJson({
        "type": "io.nats.jetstream.advisory.v1.stream_batch_abandoned",
        "id": "event-1",
        "timestamp": "2026-09-02T00:00:00Z",
        "stream": "EVENTS",
        "batch": batchId,
        "reason": reason,
      }),
    );
  }

  Future<void> close() async {
    await connection.close();
    await peer.close();
  }
}
