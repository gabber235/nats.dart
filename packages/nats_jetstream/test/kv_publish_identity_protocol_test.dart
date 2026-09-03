@TestOn("vm")
library;

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:test/test.dart";

import "support/scripted_nats_peer.dart";

void main() {
  test("KV mutations bind request and acknowledgement to one stream", () async {
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
      const Duration(seconds: 1),
      "BUCKET",
      allowPerKeyTtl: false,
      backingStreamName: "KV_BUCKET",
      readSubjectPrefix: r"$KV.BUCKET.",
      editStreamName: "KV_BUCKET",
      editSubjectPrefix: r"$KV.BUCKET.",
      editApiPrefix: r"$JS.API",
    );

    final result = bucket.put("key", const [1]);
    final publication = await peer.nextPublication(r"$KV.BUCKET.key");
    final headers = NatsHeaders.parse(publication.headerBytes);
    expect(headers.first("Nats-Expected-Stream"), "KV_BUCKET");
    await peer.sendMessage(
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
  });

  test("KV mutation rejects acknowledgement sequence zero", () async {
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
      const Duration(seconds: 1),
      "BUCKET",
      allowPerKeyTtl: false,
      backingStreamName: "KV_BUCKET",
      readSubjectPrefix: r"$KV.BUCKET.",
      editStreamName: "KV_BUCKET",
      editSubjectPrefix: r"$KV.BUCKET.",
      editApiPrefix: r"$JS.API",
    );

    final result = bucket.put("key", const [1]);
    final publication = await peer.nextPublication(r"$KV.BUCKET.key");
    await peer.sendMessage(
      publication.replySubject!,
      encodeExactJson({"stream": "KV_BUCKET", "seq": BigInt.zero}),
    );

    await expectLater(
      result,
      throwsA(
        isA<JetStreamProtocolException>().having(
          (error) => error.fieldPath,
          "fieldPath",
          "seq",
        ),
      ),
    );
  });

  test("domain KV expectation names the stored subject", () async {
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
      r"$JS.DOMAIN.API",
      const Duration(seconds: 1),
      "BUCKET",
      allowPerKeyTtl: false,
      backingStreamName: "KV_BUCKET",
      readSubjectPrefix: r"$KV.BUCKET.",
      editStreamName: "KV_BUCKET",
      editSubjectPrefix: r"$JS.DOMAIN.API.$KV.BUCKET.",
      editApiPrefix: r"$JS.DOMAIN.API",
    );

    final result = bucket.update("key", const [1], BigInt.from(3));
    final publication = await peer.nextPublication(
      r"$JS.DOMAIN.API.$KV.BUCKET.key",
    );
    final headers = NatsHeaders.parse(publication.headerBytes);
    expect(headers.first("Nats-Expected-Last-Subject-Sequence"), "3");
    expect(
      headers.first("Nats-Expected-Last-Subject-Sequence-Subject"),
      r"$KV.BUCKET.key",
    );
    await peer.sendMessage(
      publication.replySubject!,
      encodeExactJson({"stream": "KV_BUCKET", "seq": BigInt.from(4)}),
    );

    expect(await result, BigInt.from(4));
  });

  test("KV mutations reject revision overflow before publication", () async {
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
      const Duration(seconds: 1),
      "BUCKET",
      allowPerKeyTtl: false,
      backingStreamName: "KV_BUCKET",
      readSubjectPrefix: r"$KV.BUCKET.",
      editStreamName: "KV_BUCKET",
      editSubjectPrefix: r"$KV.BUCKET.",
      editApiPrefix: r"$JS.API",
    );
    final overflow = BigInt.one << 64;

    for (final operation in <Future<Object> Function()>[
      () => bucket.update("key", const [1], overflow),
      () => bucket.delete("key", expectedRevision: overflow),
      () => bucket.purge("key", expectedRevision: overflow),
    ]) {
      await expectLater(Future<Object>.sync(operation), throwsArgumentError);
    }
    expect(peer.publications, isEmpty);
  });
}
