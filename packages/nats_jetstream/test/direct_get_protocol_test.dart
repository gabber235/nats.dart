@TestOn("vm")
library;

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:test/test.dart";

import "support/scripted_nats_peer.dart";

void main() {
  test("exact direct reads reject a different peer sequence", () async {
    final peer = await ScriptedNatsPeer.start();
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [NatsServer.parse(peer.uri.toString())],
        reconnect: NatsReconnectPolicy(maximumAttempts: 0),
        pingInterval: const Duration(minutes: 10),
      ),
    );
    addTearDown(() async {
      await connection.close();
      await peer.close();
    });
    final direct = JetStreamDirectReader(
      connection,
      r"$JS.API",
      const Duration(seconds: 2),
    );

    final messageResult = direct.get("EVENTS", BigInt.from(7));
    final messageExpectation = expectLater(
      messageResult,
      _sequenceProtocolFailure,
    );
    final messageRequest = await peer.nextPublication(
      r"$JS.API.DIRECT.GET.EVENTS",
    );
    expect(decodeExactJsonObject(messageRequest.body), {"seq": BigInt.from(7)});
    await peer.sendHeaderMessage(messageRequest.replySubject!, const [
      1,
    ], _directHeaders("8").encode());
    await messageExpectation;

    final payloadResult = direct.getPayload("EVENTS", BigInt.from(9));
    final payloadExpectation = expectLater(
      payloadResult,
      _sequenceProtocolFailure,
    );
    final payloadRequest = await peer.nextPublication(
      r"$JS.API.DIRECT.GET.EVENTS",
    );
    expect(decodeExactJsonObject(payloadRequest.body), {"seq": BigInt.from(9)});
    await peer.sendHeaderMessage(payloadRequest.replySubject!, const [
      2,
    ], _directHeaders("10").encode());
    await payloadExpectation;

    final nextResult = direct.getNext(
      "EVENTS",
      "events.>",
      startSequence: BigInt.from(11),
    );
    final nextRequest = await peer.nextPublication(
      r"$JS.API.DIRECT.GET.EVENTS",
    );
    await peer.sendHeaderMessage(nextRequest.replySubject!, const [
      3,
    ], _directHeaders("13", subject: "events.eu.created").encode());
    expect((await nextResult).sequence, BigInt.from(13));

    final earlyResult = direct.getNextPayload(
      "EVENTS",
      "events.>",
      startSequence: BigInt.from(15),
    );
    final earlyExpectation = expectLater(earlyResult, _sequenceProtocolFailure);
    final earlyRequest = await peer.nextPublication(
      r"$JS.API.DIRECT.GET.EVENTS",
    );
    await peer.sendHeaderMessage(earlyRequest.replySubject!, const [
      4,
    ], _directHeaders("14", subject: "events.eu").encode());
    await earlyExpectation;

    final wrongSubjectResult = direct.getNext(
      "EVENTS",
      "events.*",
      startSequence: BigInt.from(16),
    );
    final wrongSubjectExpectation = expectLater(
      wrongSubjectResult,
      _subjectProtocolFailure,
    );
    final wrongSubjectRequest = await peer.nextPublication(
      r"$JS.API.DIRECT.GET.EVENTS",
    );
    await peer.sendHeaderMessage(wrongSubjectRequest.replySubject!, const [
      5,
    ], _directHeaders("17", subject: "metrics.cpu").encode());
    await wrongSubjectExpectation;

    final lastResult = direct.getLast("EVENTS", "events.*");
    final lastExpectation = expectLater(lastResult, _subjectProtocolFailure);
    final lastRequest = await peer.nextPublication(
      r"$JS.API.DIRECT.GET.EVENTS",
    );
    await peer.sendHeaderMessage(lastRequest.replySubject!, const [
      6,
    ], _directHeaders("18", subject: "events.eu.extra").encode());
    await lastExpectation;

    final endpointResult = direct.getLastBySubject("EVENTS", "events.eu");
    final endpointExpectation = expectLater(
      endpointResult,
      _subjectProtocolFailure,
    );
    final endpointRequest = await peer.nextPublication(
      r"$JS.API.DIRECT.GET.EVENTS.events.eu",
    );
    await peer.sendHeaderMessage(endpointRequest.replySubject!, const [
      7,
    ], _directHeaders("19", subject: "events.us").encode());
    await endpointExpectation;
  });

  test("direct batches bind every returned subject selector", () async {
    final peer = await ScriptedNatsPeer.start();
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [NatsServer.parse(peer.uri.toString())],
        reconnect: NatsReconnectPolicy(maximumAttempts: 0),
        pingInterval: const Duration(minutes: 10),
      ),
    );
    addTearDown(() async {
      await connection.close();
      await peer.close();
    });
    final direct = JetStreamDirectReader(
      connection,
      r"$JS.API",
      const Duration(seconds: 2),
    );

    final wrongResult = direct.batch(
      "EVENTS",
      JetStreamDirectBatchRequest(
        maximumMessages: 1,
        maximumBufferedBytes: 4096,
        startSequence: BigInt.from(10),
        nextBySubject: "events.>",
      ),
    );
    final wrongExpectation = expectLater(
      wrongResult,
      throwsA(
        isA<JetStreamDirectBatchException>().having(
          (error) => error.cause,
          "cause",
          isA<JetStreamProtocolException>().having(
            (error) => error.fieldPath,
            "fieldPath",
            "Nats-Subject",
          ),
        ),
      ),
    );
    final wrongRequest = await peer.nextPublication(
      r"$JS.API.DIRECT.GET.EVENTS",
    );
    await peer.sendHeaderMessage(
      wrongRequest.replySubject!,
      const [1],
      _directHeaders(
        "11",
        subject: "metrics.cpu",
        lastSequence: "0",
        pending: "0",
      ).encode(),
    );
    await wrongExpectation;

    final anyResult = direct.batch(
      "EVENTS",
      JetStreamDirectBatchRequest(
        maximumMessages: 1,
        maximumBufferedBytes: 4096,
        multipleLastSubjects: ["events.*", "metrics.*"],
      ),
    );
    final anyRequest = await peer.nextPublication(r"$JS.API.DIRECT.GET.EVENTS");
    await peer.sendHeaderMessage(
      anyRequest.replySubject!,
      const [2],
      _directHeaders(
        "12",
        subject: "metrics.cpu",
        lastSequence: "0",
        pending: "0",
      ).encode(),
    );
    await peer.sendHeaderMessage(
      anyRequest.replySubject!,
      const [],
      NatsHeaders(
        statusCode: 204,
        statusDescription: "EOB",
        entries: const [
          MapEntry("Nats-Num-Pending", "0"),
          MapEntry("Nats-Last-Sequence", "12"),
          MapEntry("Nats-UpTo-Sequence", "12"),
        ],
      ).encode(),
    );
    expect((await anyResult).messages.single.subject, "metrics.cpu");
  });

  test("direct selectors reject uint64 overflow before publication", () async {
    final peer = await ScriptedNatsPeer.start();
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [NatsServer.parse(peer.uri.toString())],
        reconnect: NatsReconnectPolicy(maximumAttempts: 0),
        pingInterval: const Duration(minutes: 10),
      ),
    );
    addTearDown(() async {
      await connection.close();
      await peer.close();
    });
    final direct = JetStreamContext(connection).streams.direct;
    final overflow = BigInt.one << 64;

    for (final call in <Object Function()>[
      () => direct.get("EVENTS", overflow),
      () => direct.getPayload("EVENTS", overflow),
      () => direct.getNext("EVENTS", "events.>", startSequence: overflow),
      () =>
          direct.getNextPayload("EVENTS", "events.>", startSequence: overflow),
    ]) {
      expect(call, throwsArgumentError);
    }
    expect(peer.publications, isEmpty);
  });
}

final _sequenceProtocolFailure = throwsA(
  isA<JetStreamProtocolException>().having(
    (error) => error.fieldPath,
    "fieldPath",
    "Nats-Sequence",
  ),
);

final _subjectProtocolFailure = throwsA(
  isA<JetStreamProtocolException>().having(
    (error) => error.fieldPath,
    "fieldPath",
    "Nats-Subject",
  ),
);

NatsHeaders _directHeaders(
  String sequence, {
  String subject = "events.one",
  String? lastSequence,
  String? pending,
}) => NatsHeaders(
  entries: [
    const MapEntry("Nats-Stream", "EVENTS"),
    MapEntry("Nats-Subject", subject),
    MapEntry("Nats-Sequence", sequence),
    const MapEntry("Nats-Time-Stamp", "2026-09-02T00:00:00Z"),
    if (lastSequence != null) MapEntry("Nats-Last-Sequence", lastSequence),
    if (pending != null) MapEntry("Nats-Num-Pending", pending),
  ],
);
