@TestOn("vm")
library;

import "dart:convert";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/kv_status.dart";
import "package:nats_jetstream/src/stream_wire.dart";
import "package:nats_jetstream/src/wire_response.dart";
import "package:test/test.dart";

import "support/scripted_nats_peer.dart";

void main() {
  test("KV configuration snapshots and accepts explicit source mappings", () {
    final source = JetStreamStreamSource(name: "SOURCE");
    final sources = [source];
    final config = JetStreamKeyValueBucketConfig(
      name: "TARGET",
      sources: sources,
    );
    sources.clear();

    expect(config.sources, [same(source)]);
    expect(config.sources.clear, throwsUnsupportedError);
    expect(source.name, "SOURCE");
    expect(source.subjectTransforms, isEmpty);
    expect(
      () => JetStreamKeyValueBucketConfig(
        name: "TARGET",
        mirror: source,
        sources: [source],
      ),
      throwsArgumentError,
    );
    final transform = JetStreamSubjectTransform(
      source: "events.processed.>",
      destination: r"$KV.TARGET.>",
    );
    final custom = JetStreamStreamSource(
      name: "EVENTS",
      subjectTransforms: [transform],
    );
    final customConfig = JetStreamKeyValueBucketConfig(
      name: "TARGET",
      sources: [custom],
    );
    expect(customConfig.sources.single, same(custom));
    expect(custom.name, "EVENTS");
    expect(custom.subjectTransforms.single, same(transform));
    expect(
      () => JetStreamKeyValueBucketConfig(
        name: "TARGET",
        metadata: const {"_nats.ver": "caller"},
      ),
      throwsArgumentError,
    );
  });

  test("status rejects malformed mirror and source wire values", () {
    for (final config in [
      {
        "mirror": {"name": "SOURCE"},
        "mirror_direct": true,
      },
      {"sources": "not an array"},
    ]) {
      final response = JetStreamWireResponse.decode(
        encodeExactJson(_streamInfo("BAD", config: config)),
        r"$JS.API.STREAM.INFO.KV_BAD",
      );
      expect(
        () => decodeKeyValueStatus(
          decodeStreamInfo(response),
          subject: response.subject,
        ),
        throwsA(isA<JetStreamProtocolException>()),
      );
    }
  });

  test("mirror create normalizes wire and routes reads and edits", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final manager = JetStreamContext(harness.connection).keyValue;
    final caller = JetStreamStreamSource(
      name: "SOURCE",
      startSequence: BigInt.one,
    );
    final result = manager.createBucket(
      JetStreamKeyValueBucketConfig(name: "MIRROR", mirror: caller),
    );

    final create = await harness.peer.nextPublication(
      r"$JS.API.STREAM.CREATE.KV_MIRROR",
    );
    final body = decodeExactJsonObject(create.body);
    expect(body.containsKey("subjects"), isFalse);
    expect(body["mirror_direct"], isTrue);
    expect(body["mirror"], {"name": "KV_SOURCE", "opt_start_seq": BigInt.one});
    expect(caller.name, "SOURCE");
    expect(caller.subjectTransforms, isEmpty);
    await harness.peer.sendMessage(
      create.replySubject!,
      encodeExactJson(_streamInfo("MIRROR", config: body)),
    );

    final sourceInfo = await harness.peer.nextPublication(
      r"$JS.API.STREAM.INFO.KV_SOURCE",
    );
    await harness.peer.sendMessage(
      sourceInfo.replySubject!,
      encodeExactJson(
        _streamInfo("SOURCE", config: const {"allow_msg_ttl": true}),
      ),
    );
    final mirror = await result;
    expect(mirror.backingStreamName, "KV_MIRROR");
    expect(mirror.readSubjectPrefix, r"$KV.SOURCE.");
    expect(mirror.editStreamName, "KV_SOURCE");
    expect(mirror.editSubjectPrefix, r"$KV.SOURCE.");
    expect(mirror.allowPerKeyTtl, isTrue);

    final read = mirror.getString("text");
    final get = await harness.peer.nextPublication(
      r"$JS.API.STREAM.MSG.GET.KV_MIRROR",
    );
    expect(decodeExactJsonObject(get.body)["last_by_subj"], r"$KV.SOURCE.text");
    await harness.peer.sendMessage(
      get.replySubject!,
      encodeExactJson(
        _stored("SOURCE", "text", utf8.encode("한글 🙂"), BigInt.one),
      ),
    );
    expect(await read, "한글 🙂");

    final write = mirror.putString("edit", "value");
    final put = await harness.peer.nextPublication(r"$KV.SOURCE.edit");
    expect(put.body, utf8.encode("value"));
    await harness.peer.sendMessage(
      put.replySubject!,
      encodeExactJson({"stream": "KV_SOURCE", "seq": BigInt.two}),
    );
    expect(await write, BigInt.two);

    final malformed = mirror.getString("invalid");
    final invalid = await harness.peer.nextPublication(
      r"$JS.API.STREAM.MSG.GET.KV_MIRROR",
    );
    await harness.peer.sendMessage(
      invalid.replySubject!,
      encodeExactJson(
        _stored("SOURCE", "invalid", const [0xc3, 0x28], BigInt.two),
      ),
    );
    await expectLater(malformed, throwsFormatException);
  });

  test("sources normalize without mutating callers", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final manager = JetStreamContext(harness.connection).keyValue;
    final local = JetStreamStreamSource(name: "SOURCE");
    final external = JetStreamStreamSource(
      name: "TARGET",
      external: JetStreamExternalStream(
        apiPrefix: r"$JS.EXT.API",
        deliverPrefix: "EXT.DELIVER",
      ),
    );
    final custom = JetStreamStreamSource(
      name: "EVENTS",
      subjectTransforms: [
        JetStreamSubjectTransform(
          source: "events.processed.>",
          destination: r"$KV.TARGET.>",
        ),
      ],
    );
    final result = manager.createBucket(
      JetStreamKeyValueBucketConfig(
        name: "TARGET",
        sources: [local, external, custom],
      ),
    );

    final create = await harness.peer.nextPublication(
      r"$JS.API.STREAM.CREATE.KV_TARGET",
    );
    final body = decodeExactJsonObject(create.body);
    expect(body["subjects"], [r"$KV.TARGET.>"]);
    expect(body["sources"], [
      {
        "name": "KV_SOURCE",
        "subject_transforms": [
          {"src": r"$KV.SOURCE.>", "dest": r"$KV.TARGET.>"},
        ],
      },
      {
        "name": "KV_TARGET",
        "external": {"api": r"$JS.EXT.API", "deliver": "EXT.DELIVER"},
      },
      {
        "name": "EVENTS",
        "subject_transforms": [
          {"src": "events.processed.>", "dest": r"$KV.TARGET.>"},
        ],
      },
    ]);
    expect(local.name, "SOURCE");
    expect(local.subjectTransforms, isEmpty);
    expect(external.name, "TARGET");
    expect(external.subjectTransforms, isEmpty);
    expect(custom.name, "EVENTS");
    expect(custom.subjectTransforms.single.source, "events.processed.>");
    await harness.peer.sendMessage(
      create.replySubject!,
      encodeExactJson(_streamInfo("TARGET", config: body)),
    );

    final bucket = await result;
    final status = await _status(harness, bucket);
    expect(status.sources.map((source) => source.name), [
      "KV_SOURCE",
      "KV_TARGET",
      "EVENTS",
    ]);
    expect(
      status.sources.first.subjectTransforms.single.source,
      r"$KV.SOURCE.>",
    );
    expect(status.sources[1].subjectTransforms, isEmpty);
    expect(
      status.sources.last.subjectTransforms.single.destination,
      r"$KV.TARGET.>",
    );
    expect(status.sources.clear, throwsUnsupportedError);
  });

  test(
    "external mirror reads locally and edits through source prefix",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final manager = JetStreamContext(harness.connection).keyValue;
      final result = manager.bucket("MIRROR");
      final info = await harness.peer.nextPublication(
        r"$JS.API.STREAM.INFO.KV_MIRROR",
      );
      await harness.peer.sendMessage(
        info.replySubject!,
        encodeExactJson(
          _streamInfo(
            "MIRROR",
            config: {
              "mirror": {
                "name": "KV_SOURCE",
                "external": {"api": r"$JS.EXT.API", "deliver": "EXT.DELIVER"},
              },
              "mirror_direct": true,
            },
          ),
        ),
      );
      final source = await harness.peer.nextPublication(
        r"$JS.EXT.API.STREAM.INFO.KV_SOURCE",
      );
      await harness.peer.sendMessage(
        source.replySubject!,
        encodeExactJson(_streamInfo("SOURCE")),
      );
      final mirror = await result;
      expect(mirror.readSubjectPrefix, r"$KV.SOURCE.");
      expect(mirror.editSubjectPrefix, r"$JS.EXT.API.$KV.SOURCE.");

      final write = mirror.put("key", const [1]);
      final publication = await harness.peer.nextPublication(
        r"$JS.EXT.API.$KV.SOURCE.key",
      );
      await harness.peer.sendMessage(
        publication.replySubject!,
        encodeExactJson({"stream": "KV_SOURCE", "seq": BigInt.one}),
      );
      expect(await write, BigInt.one);
    },
  );

  test("custom domain prefixes ordinary and local mirror edits", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final manager = JetStreamContext(
      harness.connection,
      apiPrefix: r"$JS.DOMAIN.API",
    ).keyValue;

    final ordinaryResult = manager.bucket("ORDINARY");
    final ordinaryInfo = await harness.peer.nextPublication(
      r"$JS.DOMAIN.API.STREAM.INFO.KV_ORDINARY",
    );
    await harness.peer.sendMessage(
      ordinaryInfo.replySubject!,
      encodeExactJson(_streamInfo("ORDINARY")),
    );
    final ordinary = await ordinaryResult;
    expect(ordinary.editSubjectPrefix, r"$JS.DOMAIN.API.$KV.ORDINARY.");
    final ordinaryPut = ordinary.put("key", const [1]);
    final ordinaryPublication = await harness.peer.nextPublication(
      r"$JS.DOMAIN.API.$KV.ORDINARY.key",
    );
    await harness.peer.sendMessage(
      ordinaryPublication.replySubject!,
      encodeExactJson({"stream": "KV_ORDINARY", "seq": BigInt.one}),
    );
    expect(await ordinaryPut, BigInt.one);

    final mirrorResult = manager.bucket("MIRROR");
    final mirrorInfo = await harness.peer.nextPublication(
      r"$JS.DOMAIN.API.STREAM.INFO.KV_MIRROR",
    );
    await harness.peer.sendMessage(
      mirrorInfo.replySubject!,
      encodeExactJson(
        _streamInfo(
          "MIRROR",
          config: const {
            "mirror": {"name": "KV_SOURCE"},
            "mirror_direct": true,
          },
        ),
      ),
    );
    final sourceInfo = await harness.peer.nextPublication(
      r"$JS.DOMAIN.API.STREAM.INFO.KV_SOURCE",
    );
    await harness.peer.sendMessage(
      sourceInfo.replySubject!,
      encodeExactJson(_streamInfo("SOURCE")),
    );
    final mirror = await mirrorResult;
    expect(mirror.readSubjectPrefix, r"$KV.SOURCE.");
    expect(mirror.editSubjectPrefix, r"$JS.DOMAIN.API.$KV.SOURCE.");
    final mirrorPut = mirror.put("key", const [2]);
    final mirrorPublication = await harness.peer.nextPublication(
      r"$JS.DOMAIN.API.$KV.SOURCE.key",
    );
    await harness.peer.sendMessage(
      mirrorPublication.replySubject!,
      encodeExactJson({"stream": "KV_SOURCE", "seq": BigInt.two}),
    );
    expect(await mirrorPut, BigInt.two);
  });

  test("update preserves unknown fields and reserved metadata", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final manager = JetStreamContext(harness.connection).keyValue;
    final result = manager.update(
      JetStreamKeyValueBucketConfig(
        name: "UPDATE",
        description: "new",
        storage: JetStreamStorage.memory,
        metadata: const {"owner": "new"},
      ),
    );
    final info = await harness.peer.nextPublication(
      r"$JS.API.STREAM.INFO.KV_UPDATE",
    );
    await harness.peer.sendMessage(
      info.replySubject!,
      encodeExactJson(
        _streamInfo(
          "UPDATE",
          config: {
            "metadata": {"owner": "old", "_nats.ver": "2.14.6"},
            "future": {"limit": BigInt.parse("18446744073709551615")},
          },
        ),
      ),
    );
    final update = await harness.peer.nextPublication(
      r"$JS.API.STREAM.UPDATE.KV_UPDATE",
    );
    final body = decodeExactJsonObject(update.body);
    expect(body["future"], {"limit": BigInt.parse("18446744073709551615")});
    expect(body["metadata"], {"_nats.ver": "2.14.6", "owner": "new"});
    await harness.peer.sendMessage(
      update.replySubject!,
      encodeExactJson(_streamInfo("UPDATE", config: body)),
    );
    expect((await result).name, "UPDATE");
  });

  test("createOrUpdate creates only for exact absence", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final manager = JetStreamContext(harness.connection).keyValue;
    final result = manager.createOrUpdate(
      JetStreamKeyValueBucketConfig(name: "NEW"),
    );
    final info = await harness.peer.nextPublication(
      r"$JS.API.STREAM.INFO.KV_NEW",
    );
    await harness.peer.sendMessage(
      info.replySubject!,
      _apiError(404, 10059, "stream not found"),
    );
    final create = await harness.peer.nextPublication(
      r"$JS.API.STREAM.CREATE.KV_NEW",
    );
    final body = decodeExactJsonObject(create.body);
    await harness.peer.sendMessage(
      create.replySubject!,
      encodeExactJson(_streamInfo("NEW", config: body)),
    );
    expect((await result).name, "NEW");

    final denied = manager.createOrUpdate(
      JetStreamKeyValueBucketConfig(name: "DENIED"),
    );
    final deniedInfo = await harness.peer.nextPublication(
      r"$JS.API.STREAM.INFO.KV_DENIED",
    );
    await harness.peer.sendMessage(
      deniedInfo.replySubject!,
      _apiError(403, 10058, "stream name already in use"),
    );
    await expectLater(
      denied,
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
        (publication) =>
            publication.subject == r"$JS.API.STREAM.CREATE.KV_DENIED",
      ),
      isEmpty,
    );
  });
}

Future<JetStreamKeyValueStatus> _status(
  _Harness harness,
  JetStreamKeyValue bucket,
) async {
  final result = bucket.status();
  final request = await harness.peer.nextPublication(
    r"$JS.API.STREAM.INFO.KV_TARGET",
  );
  final sources = harness.peer.publications
      .firstWhere(
        (publication) =>
            publication.subject == r"$JS.API.STREAM.CREATE.KV_TARGET",
      )
      .body;
  await harness.peer.sendMessage(
    request.replySubject!,
    encodeExactJson(
      _streamInfo("TARGET", config: decodeExactJsonObject(sources)),
    ),
  );
  return result;
}

Map<String, Object?> _streamInfo(
  String bucket, {
  Map<String, Object?> config = const {},
}) {
  final streamConfig = <String, Object?>{
    "name": "KV_$bucket",
    "subjects": ["\$KV.$bucket.>"],
    "retention": "limits",
    "max_msgs_per_subject": BigInt.one,
    "max_age": BigInt.zero,
    "max_bytes": BigInt.from(-1),
    "max_msg_size": BigInt.from(-1),
    "storage": "memory",
    "num_replicas": BigInt.one,
    "discard": "new",
    "allow_rollup_hdrs": true,
    "deny_delete": true,
    "allow_direct": true,
    "compression": "none",
    ...config,
  };
  if (streamConfig["mirror"] != null && !config.containsKey("subjects")) {
    streamConfig.remove("subjects");
  }
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

Map<String, Object?> _stored(
  String bucket,
  String key,
  List<int> value,
  BigInt sequence,
) {
  return {
    "message": {
      "subject": "\$KV.$bucket.$key",
      "seq": sequence,
      "data": base64Encode(value),
      "time": "2026-09-02T00:00:00Z",
    },
  };
}

List<int> _apiError(int code, int errorCode, String description) {
  return encodeExactJson({
    "error": {"code": code, "err_code": errorCode, "description": description},
  });
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
