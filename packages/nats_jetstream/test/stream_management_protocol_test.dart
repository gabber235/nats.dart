@TestOn("vm")
library;

import "dart:convert";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:test/test.dart";

import "support/scripted_nats_peer.dart";

void main() {
  test(
    "message deletion keeps normal default and encodes secure erase",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final jetStream = JetStreamContext(harness.connection);

      final normalResult = jetStream.deleteMessage("EVENTS", BigInt.one);
      final normal = await harness.peer.nextPublication(
        r"$JS.API.STREAM.MSG.DELETE.EVENTS",
      );
      expect(decodeExactJsonObject(normal.body), {
        "seq": BigInt.one,
        "no_erase": true,
      });
      await harness.peer.sendMessage(
        normal.replySubject!,
        encodeExactJson({"success": true}),
      );
      await normalResult;

      final secureResult = jetStream.deleteMessage(
        "EVENTS",
        BigInt.two,
        secure: true,
      );
      final secure = await harness.peer.nextPublication(
        r"$JS.API.STREAM.MSG.DELETE.EVENTS",
      );
      expect(decodeExactJsonObject(secure.body), {"seq": BigInt.two});
      await harness.peer.sendMessage(
        secure.replySubject!,
        encodeExactJson({"success": true}),
      );
      await secureResult;
    },
  );

  test("exact stored reads reject a different or zero sequence", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final jetStream = JetStreamContext(harness.connection);

    for (final returned in [BigInt.two, BigInt.zero]) {
      final result = jetStream.getMessage("EVENTS", BigInt.one);
      final expectation = expectLater(
        result,
        throwsA(
          isA<JetStreamProtocolException>().having(
            (error) => error.fieldPath,
            "fieldPath",
            "message.seq",
          ),
        ),
      );
      final request = await harness.peer.nextPublication(
        r"$JS.API.STREAM.MSG.GET.EVENTS",
      );
      expect(decodeExactJsonObject(request.body), {"seq": BigInt.one});
      await harness.peer.sendMessage(
        request.replySubject!,
        encodeExactJson(_stored("events.one", returned)),
      );
      await expectation;
    }
  });

  test("stored subject selectors bind wildcard patterns", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final jetStream = JetStreamContext(harness.connection);

    final matchingLastResult = jetStream.getLastMessage("EVENTS", "events.*");
    final matchingLast = await harness.peer.nextPublication(
      r"$JS.API.STREAM.MSG.GET.EVENTS",
    );
    await harness.peer.sendMessage(
      matchingLast.replySubject!,
      encodeExactJson(_stored("events.eu", BigInt.one)),
    );
    expect((await matchingLastResult).subject, "events.eu");

    final matchingNextResult = jetStream.getNextMessage(
      "EVENTS",
      "events.>",
      BigInt.two,
    );
    final matchingNext = await harness.peer.nextPublication(
      r"$JS.API.STREAM.MSG.GET.EVENTS",
    );
    await harness.peer.sendMessage(
      matchingNext.replySubject!,
      encodeExactJson(_stored("events.eu.created", BigInt.two)),
    );
    expect((await matchingNextResult).subject, "events.eu.created");

    final lastResult = jetStream.getLastMessage("EVENTS", "events.*");
    final lastExpectation = expectLater(lastResult, _subjectProtocolFailure);
    final last = await harness.peer.nextPublication(
      r"$JS.API.STREAM.MSG.GET.EVENTS",
    );
    expect(decodeExactJsonObject(last.body), {"last_by_subj": "events.*"});
    await harness.peer.sendMessage(
      last.replySubject!,
      encodeExactJson(_stored("events.eu.extra", BigInt.one)),
    );
    await lastExpectation;

    final nextResult = jetStream.getNextMessage(
      "EVENTS",
      "events.>",
      BigInt.from(4),
    );
    final nextExpectation = expectLater(nextResult, _subjectProtocolFailure);
    final next = await harness.peer.nextPublication(
      r"$JS.API.STREAM.MSG.GET.EVENTS",
    );
    expect(decodeExactJsonObject(next.body), {
      "next_by_subj": "events.>",
      "seq": BigInt.from(4),
    });
    await harness.peer.sendMessage(
      next.replySubject!,
      encodeExactJson(_stored("other.eu", BigInt.from(5))),
    );
    await nextExpectation;
  });

  test("stream pages and purge reject schema integer overflow", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final streams = JetStreamContext(harness.connection).streams;
    final int64Overflow = BigInt.one << 63;
    final uint64Overflow = BigInt.one << 64;

    final listResult = streams.listPage();
    final listExpectation = expectLater(
      listResult,
      throwsA(_protocolField("total")),
    );
    final listRequest = await harness.peer.nextPublication(
      r"$JS.API.STREAM.LIST",
    );
    await harness.peer.sendMessage(
      listRequest.replySubject!,
      encodeExactJson({
        "total": int64Overflow,
        "offset": BigInt.zero,
        "limit": BigInt.one,
        "streams": const <Object?>[],
      }),
    );
    await listExpectation;

    final namesResult = streams.namesPage();
    final namesExpectation = expectLater(
      namesResult,
      throwsA(_protocolField("limit")),
    );
    final namesRequest = await harness.peer.nextPublication(
      r"$JS.API.STREAM.NAMES",
    );
    await harness.peer.sendMessage(
      namesRequest.replySubject!,
      encodeExactJson({
        "total": BigInt.zero,
        "offset": BigInt.zero,
        "limit": int64Overflow,
        "streams": const <String>[],
      }),
    );
    await namesExpectation;

    final purgeResult = streams.purge("EVENTS");
    final purgeExpectation = expectLater(
      purgeResult,
      throwsA(_protocolField("purged")),
    );
    final purgeRequest = await harness.peer.nextPublication(
      r"$JS.API.STREAM.PURGE.EVENTS",
    );
    await harness.peer.sendMessage(
      purgeRequest.replySubject!,
      encodeExactJson({"success": true, "purged": uint64Overflow}),
    );
    await purgeExpectation;
  });

  test(
    "outbound stream sequences reject overflow before publication",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final jetStream = JetStreamContext(harness.connection);
      final overflow = BigInt.one << 64;

      expect(
        () => jetStream.getMessage("EVENTS", overflow),
        throwsArgumentError,
      );
      expect(
        () => jetStream.getNextMessage("EVENTS", "events.>", overflow),
        throwsArgumentError,
      );
      await expectLater(
        jetStream.deleteMessage("EVENTS", overflow),
        throwsArgumentError,
      );
      expect(
        () => JetStreamPurgeOptions(sequence: overflow),
        throwsArgumentError,
      );
      expect(() => JetStreamPurgeOptions(keep: overflow), throwsArgumentError);
      expect(harness.peer.publications, isEmpty);
    },
  );
}

Matcher _protocolField(String field) => isA<JetStreamProtocolException>()
    .having((error) => error.fieldPath, "fieldPath", field);

final _subjectProtocolFailure = throwsA(
  isA<JetStreamProtocolException>().having(
    (error) => error.fieldPath,
    "fieldPath",
    "message.subject",
  ),
);

Map<String, Object?> _stored(String subject, BigInt sequence) => {
  "message": {
    "subject": subject,
    "seq": sequence,
    "data": base64Encode(const [1]),
    "time": "2026-09-02T00:00:00Z",
  },
};

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
