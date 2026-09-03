@TestOn("vm")
library;

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/object_store_stream.dart";
import "package:nats_jetstream/src/stream_wire.dart";
import "package:nats_jetstream/src/wire_response.dart";
import "package:test/test.dart";

import "support/scripted_nats_peer.dart";

void main() {
  test(
    "update preserves unknown configuration and reserved metadata",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final manager = JetStreamContext(harness.connection).objectStore;
      final update = manager.update(
        JetStreamObjectStoreConfig(
          name: "OBJECTS",
          storage: JetStreamStorage.memory,
          maximumAge: JetStreamNanoseconds(BigInt.from(5000000000)),
          maximumBytes: BigInt.from(4096),
          replicas: 2,
          metadata: const {"owner": "new"},
          compression: true,
          placement: JetStreamPlacement(cluster: "C2", tags: const ["ssd"]),
        ),
      );

      final info = await harness.peer.nextPublication(
        r"$JS.API.STREAM.INFO.OBJ_OBJECTS",
      );
      await harness.peer.sendMessage(
        info.replySubject!,
        encodeExactJson(
          _streamInfo(
            "OBJECTS",
            config: {
              "description": "old",
              "metadata": {"owner": "old", "_nats.ver": "2.14.6"},
              "future_config": {"limit": BigInt.parse("18446744073709551615")},
            },
          ),
        ),
      );

      final request = await harness.peer.nextPublication(
        r"$JS.API.STREAM.UPDATE.OBJ_OBJECTS",
      );
      final body = decodeExactJsonObject(request.body);
      expect(body["description"], isNull);
      expect(body["max_age"], BigInt.from(5000000000));
      expect(body["max_bytes"], BigInt.from(4096));
      expect(body["storage"], "memory");
      expect(body["num_replicas"], BigInt.two);
      expect(body["compression"], "s2");
      expect(body["subjects"], objectStreamSubjects("OBJECTS"));
      expect(body["future_config"], {
        "limit": BigInt.parse("18446744073709551615"),
      });
      expect(body["metadata"], {"_nats.ver": "2.14.6", "owner": "new"});
      expect(body["placement"], {
        "cluster": "C2",
        "tags": ["ssd"],
      });
      await harness.peer.sendMessage(
        request.replySubject!,
        encodeExactJson(_streamInfo("OBJECTS", config: body)),
      );

      expect((await update).name, "OBJECTS");
    },
  );

  test("list and names use the Object Store chunk subject filter", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final manager = JetStreamContext(harness.connection).objectStore;

    final statusesFuture = manager.list().toList();
    final first = await harness.peer.nextPublication(r"$JS.API.STREAM.LIST");
    expect(decodeExactJsonObject(first.body), {
      "offset": BigInt.zero,
      "subject": r"$O.*.C.>",
    });
    await harness.peer.sendMessage(
      first.replySubject!,
      encodeExactJson({
        "total": BigInt.two,
        "offset": BigInt.zero,
        "limit": BigInt.one,
        "streams": [
          _streamInfo(
            "ONE",
            config: {
              "placement": {
                "cluster": "C",
                "tags": ["ssd"],
              },
            },
          ),
        ],
      }),
    );
    final second = await harness.peer.nextPublication(r"$JS.API.STREAM.LIST");
    expect(decodeExactJsonObject(second.body)["offset"], BigInt.one);
    await harness.peer.sendMessage(
      second.replySubject!,
      encodeExactJson({
        "total": BigInt.two,
        "offset": BigInt.one,
        "limit": BigInt.one,
        "streams": [_streamInfo("TWO")],
      }),
    );
    final statuses = await statusesFuture;
    expect(statuses.map((value) => value.name), ["ONE", "TWO"]);
    expect(statuses.first.placement?.cluster, "C");
    expect(statuses.first.placement?.tags, ["ssd"]);

    final namesFuture = manager.names().toList();
    final namesRequest = await harness.peer.nextPublication(
      r"$JS.API.STREAM.LIST",
    );
    expect(decodeExactJsonObject(namesRequest.body)["subject"], r"$O.*.C.>");
    await harness.peer.sendMessage(
      namesRequest.replySubject!,
      encodeExactJson({
        "total": BigInt.one,
        "offset": BigInt.zero,
        "limit": BigInt.one,
        "streams": [_streamInfo("ONE")],
      }),
    );
    expect(await namesFuture, ["ONE"]);
  });

  test("list rejects malformed backing stream identity", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final manager = JetStreamContext(harness.connection).objectStore;

    final result = manager.list().toList();
    final request = await harness.peer.nextPublication(r"$JS.API.STREAM.LIST");
    await harness.peer.sendMessage(
      request.replySubject!,
      encodeExactJson({
        "total": BigInt.one,
        "offset": BigInt.zero,
        "limit": BigInt.one,
        "streams": [
          _streamInfo(
            "BAD",
            config: {
              "subjects": [r"$O.OTHER.C.>", r"$O.BAD.M.>"],
            },
          ),
        ],
      }),
    );

    await expectLater(result, throwsA(isA<JetStreamProtocolException>()));
  });

  test("status rejects malformed age and compression values", () {
    for (final config in [
      {"max_age": BigInt.from(-1)},
      {"compression": "future"},
    ]) {
      final response = JetStreamWireResponse.decode(
        encodeExactJson(_streamInfo("BAD", config: config)),
        r"$JS.API.STREAM.INFO.OBJ_BAD",
      );
      expect(
        () => decodeObjectStoreStatus(
          decodeStreamInfo(response),
          subject: response.subject,
        ),
        throwsA(isA<JetStreamProtocolException>()),
      );
    }
  });

  test("createOrUpdate creates only after exact absence", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final manager = JetStreamContext(harness.connection).objectStore;
    final result = manager.createOrUpdate(
      JetStreamObjectStoreConfig(name: "NEW", placement: JetStreamPlacement()),
    );

    final info = await harness.peer.nextPublication(
      r"$JS.API.STREAM.INFO.OBJ_NEW",
    );
    await harness.peer.sendMessage(
      info.replySubject!,
      encodeExactJson({
        "error": {
          "code": 404,
          "err_code": 10059,
          "description": "stream not found",
        },
      }),
    );
    final create = await harness.peer.nextPublication(
      r"$JS.API.STREAM.CREATE.OBJ_NEW",
    );
    final body = decodeExactJsonObject(create.body);
    expect(body["name"], "OBJ_NEW");
    expect(body["subjects"], objectStreamSubjects("NEW"));
    expect(body["placement"], isEmpty);
    await harness.peer.sendMessage(
      create.replySubject!,
      encodeExactJson(_streamInfo("NEW", config: body)),
    );

    expect((await result).name, "NEW");
  });

  test("createOrUpdate preserves nonabsence API failures", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final manager = JetStreamContext(harness.connection).objectStore;
    final result = manager.createOrUpdate(
      JetStreamObjectStoreConfig(name: "DENIED"),
    );

    final info = await harness.peer.nextPublication(
      r"$JS.API.STREAM.INFO.OBJ_DENIED",
    );
    await harness.peer.sendMessage(
      info.replySubject!,
      encodeExactJson({
        "error": {
          "code": 403,
          "err_code": 10058,
          "description": "stream name already in use",
        },
      }),
    );

    await expectLater(
      result,
      throwsA(
        isA<JetStreamApiException>().having(
          (error) => error.errorCode,
          "errorCode",
          10058,
        ),
      ),
    );
    expect(
      harness.peer.publications.where(
        (value) => value.subject == r"$JS.API.STREAM.CREATE.OBJ_DENIED",
      ),
      isEmpty,
    );
  });

  test("seal accepts the server normalized Object Store identity", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final bucket = JetStreamObjectStore(
      harness.connection,
      r"$JS.API",
      const Duration(seconds: 2),
      "OBJECTS",
    );

    final seal = bucket.seal();
    final info = await harness.peer.nextPublication(
      r"$JS.API.STREAM.INFO.OBJ_OBJECTS",
    );
    await harness.peer.sendMessage(
      info.replySubject!,
      encodeExactJson(
        _streamInfo(
          "OBJECTS",
          config: {"max_age": BigInt.from(1000), "sealed": false},
        ),
      ),
    );
    final update = await harness.peer.nextPublication(
      r"$JS.API.STREAM.UPDATE.OBJ_OBJECTS",
    );
    final requested = decodeExactJsonObject(update.body);
    expect(requested["sealed"], isTrue);
    await harness.peer.sendMessage(
      update.replySubject!,
      encodeExactJson(
        _streamInfo(
          "OBJECTS",
          config: {
            ...requested,
            "max_age": BigInt.zero,
            "allow_rollup_hdrs": false,
            "deny_delete": true,
            "deny_purge": true,
          },
        ),
      ),
    );

    final status = await seal;
    expect(status.sealed, isTrue);
    expect(status.maximumAge.nanoseconds, BigInt.zero);
  });
}

Map<String, Object?> _streamInfo(
  String bucket, {
  Map<String, Object?> config = const {},
}) {
  final streamConfig = <String, Object?>{
    "name": objectStreamName(bucket),
    "subjects": objectStreamSubjects(bucket),
    "retention": "limits",
    "max_age": BigInt.zero,
    "max_bytes": BigInt.from(-1),
    "storage": "file",
    "num_replicas": BigInt.one,
    "discard": "new",
    "allow_rollup_hdrs": true,
    "allow_direct": true,
    "compression": "none",
    ...config,
  };
  return {
    "config": streamConfig,
    "created": "2026-09-02T00:00:00Z",
    "state": {
      "messages": BigInt.zero,
      "bytes": BigInt.zero,
      "first_seq": BigInt.zero,
      "first_ts": "0001-01-01T00:00:00Z",
      "last_seq": BigInt.zero,
      "last_ts": "0001-01-01T00:00:00Z",
      "consumer_count": BigInt.zero,
    },
    "ts": "2026-09-02T00:00:01Z",
  };
}

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
