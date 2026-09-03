@TestOn("vm")
library;

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/object_store_protocol.dart";
import "package:test/test.dart";

import "support/scripted_nats_peer.dart";

void main() {
  test("stored Object Store sequence requires positive uint64", () async {
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
    final protocol = JetStreamObjectProtocol(
      connection,
      r"$JS.API",
      const Duration(seconds: 1),
      "FILES",
    );

    for (final sequence in [BigInt.zero, BigInt.one << 64]) {
      final result = protocol.getStored({"seq": BigInt.one});
      final request = await peer.nextPublication(
        r"$JS.API.STREAM.MSG.GET.OBJ_FILES",
      );
      await peer.sendMessage(
        request.replySubject!,
        encodeExactJson(_stored(sequence)),
      );
      await expectLater(
        result,
        throwsA(
          isA<JetStreamProtocolException>().having(
            (error) => error.fieldPath,
            "fieldPath",
            "message.seq",
          ),
        ),
      );
    }

    final maximum = (BigInt.one << 64) - BigInt.one;
    final accepted = protocol.getStored({"seq": maximum});
    final request = await peer.nextPublication(
      r"$JS.API.STREAM.MSG.GET.OBJ_FILES",
    );
    await peer.sendMessage(
      request.replySubject!,
      encodeExactJson(_stored(maximum)),
    );
    expect((await accepted)!.sequence, maximum);
    expect(peer.publications, hasLength(3));
  });

  test("Object Store purge rejects overflow before publication", () async {
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
    final protocol = JetStreamObjectProtocol(
      connection,
      r"$JS.API",
      const Duration(seconds: 1),
      "FILES",
    );

    await expectLater(
      protocol.purgeSubject(
        r"$O.FILES.C.identifier",
        beforeSequence: BigInt.one << 64,
      ),
      throwsArgumentError,
    );
    expect(peer.publications, isEmpty);
  });
}

Map<String, Object?> _stored(BigInt sequence) => {
  "message": {
    "subject": r"$O.FILES.M.name",
    "seq": sequence,
    "data": "",
    "time": "2026-09-02T00:00:00Z",
  },
};
