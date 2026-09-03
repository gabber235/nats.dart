@TestOn("vm")
library;

import "dart:convert";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:test/test.dart";

import "support/scripted_nats_peer.dart";

void main() {
  test("getRevision rejects a different or zero stored revision", () async {
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
    final bucket = JetStreamKeyValue.verified(
      connection,
      r"$JS.API",
      const Duration(seconds: 2),
      "BUCKET",
      allowPerKeyTtl: false,
      backingStreamName: "KV_BUCKET",
      readSubjectPrefix: r"$KV.BUCKET.",
      editStreamName: "KV_BUCKET",
      editSubjectPrefix: r"$KV.BUCKET.",
      editApiPrefix: r"$JS.API",
    );

    for (final returned in [BigInt.from(8), BigInt.zero]) {
      final result = bucket.getRevision("key", BigInt.from(7));
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
      final request = await peer.nextPublication(
        r"$JS.API.STREAM.MSG.GET.KV_BUCKET",
      );
      expect(decodeExactJsonObject(request.body), {"seq": BigInt.from(7)});
      await peer.sendMessage(
        request.replySubject!,
        encodeExactJson({
          "message": {
            "subject": r"$KV.BUCKET.key",
            "seq": returned,
            "data": base64Encode(const [1]),
            "time": "2026-09-02T00:00:00Z",
          },
        }),
      );
      await expectation;
    }
  });
}
