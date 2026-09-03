import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/kv_status.dart";
import "package:nats_jetstream/src/stream_wire.dart";
import "package:nats_jetstream/src/wire_response.dart";
import "package:test/test.dart";

void main() {
  test("KV configuration snapshots placement republish and metadata", () {
    final metadata = {"owner": "before"};
    final tags = ["ssd"];
    final config = JetStreamKeyValueBucketConfig(
      name: "CONFIG",
      metadata: metadata,
      placement: JetStreamPlacement(cluster: "C", tags: tags),
      republish: JetStreamRepublish(
        source: r"$KV.CONFIG.>",
        destination: "events.>",
        headersOnly: true,
      ),
    );
    metadata["owner"] = "after";
    tags.add("new");

    expect(config.metadata, {"owner": "before"});
    expect(config.placement?.tags, ["ssd"]);
    expect(config.republish?.source, r"$KV.CONFIG.>");
    expect(config.republish?.destination, "events.>");
    expect(config.republish?.headersOnly, isTrue);
  });

  test("watch filters are immutable unique and mutually exclusive", () {
    final source = ["users.*", "orders.>"];
    final options = JetStreamKeyValueWatchOptions(filters: source);
    source[0] = "changed";

    expect(options.filters, ["users.*", "orders.>"]);
    expect(options.filter, isNull);
    expect(() => options.filters.add("new"), throwsUnsupportedError);
    expect(
      () => JetStreamKeyValueWatchOptions(
        filter: "users.*",
        filters: const ["orders.*"],
      ),
      throwsArgumentError,
    );
    expect(
      () =>
          JetStreamKeyValueWatchOptions(filters: const ["users.*", "users.*"]),
      throwsArgumentError,
    );
    expect(
      () => JetStreamKeyValueWatchOptions(filters: const ["users.>.bad"]),
      throwsArgumentError,
    );
  });

  test("status preserves future compression and immutable metadata", () {
    final info = _info(
      name: "KV_STATUS",
      subjects: const [r"$KV.STATUS.>"],
      compression: "future",
    );
    final decoded = decodeKeyValueStatus(
      info,
      subject: r"$JS.API.STREAM.INFO.KV_STATUS",
    );
    final metadata = {"owner": "before"};
    final status = JetStreamKeyValueStatus(
      bucket: decoded.bucket,
      values: decoded.values,
      sizeBytes: decoded.sizeBytes,
      history: decoded.history,
      maximumAge: decoded.maximumAge,
      markerLifetime: decoded.markerLifetime,
      storageWireValue: decoded.storageWireValue,
      knownStorage: decoded.knownStorage,
      replicas: decoded.replicas,
      description: decoded.description,
      maximumBytes: decoded.maximumBytes,
      maximumValueBytes: decoded.maximumValueBytes,
      placement: decoded.placement,
      republish: decoded.republish,
      metadata: metadata,
      compressionWireValue: decoded.compressionWireValue,
      streamInfo: decoded.streamInfo,
    );
    metadata["owner"] = "after";

    expect(status.bucket, "STATUS");
    expect(status.values, BigInt.two);
    expect(status.history, 3);
    expect(status.maximumAge?.nanoseconds, BigInt.from(5000000000));
    expect(status.markerLifetime?.nanoseconds, BigInt.from(2000000000));
    expect(status.compressionWireValue, "future");
    expect(status.compression, isNull);
    expect(status.metadata, {"owner": "before"});
    expect(() => status.metadata["new"] = "value", throwsUnsupportedError);
  });

  test("status distinguishes malformed KV identity and duration", () {
    expect(
      () => decodeKeyValueStatus(
        _info(name: "KV_WRONG", subjects: const [r"$KV.OTHER.>"]),
        subject: r"$JS.API.STREAM.INFO.KV_WRONG",
      ),
      throwsA(isA<JetStreamProtocolException>()),
    );
    expect(
      () => _info(
        name: "KV_NEGATIVE",
        subjects: const [r"$KV.NEGATIVE.>"],
        maximumAge: BigInt.from(-1),
      ),
      throwsA(isA<JetStreamProtocolException>()),
    );
  });

  test("mutation results snapshot completed key revisions", () {
    final revisions = {"a": BigInt.one};
    final result = JetStreamKeyValueMutationResult(revisions);
    revisions["b"] = BigInt.two;

    expect(result.matched, 1);
    expect(result.revisions, {"a": BigInt.one});
    expect(
      () => result.revisions["c"] = BigInt.from(3),
      throwsUnsupportedError,
    );
  });
}

JetStreamStreamInfo _info({
  required String name,
  required List<String> subjects,
  String compression = "s2",
  BigInt? maximumAge,
}) {
  final response = JetStreamWireResponse.decode(
    encodeExactJson({
      "config": {
        "name": name,
        "subjects": subjects,
        "retention": "limits",
        "max_msgs_per_subject": BigInt.from(3),
        "max_age": maximumAge ?? BigInt.from(5000000000),
        "max_bytes": BigInt.from(1024),
        "max_msg_size": BigInt.from(128),
        "storage": "file",
        "num_replicas": BigInt.one,
        "discard": "new",
        "allow_rollup_hdrs": true,
        "deny_delete": true,
        "allow_direct": true,
        "allow_msg_ttl": true,
        "subject_delete_marker_ttl": BigInt.from(2000000000),
        "compression": compression,
        "metadata": {"source": "wire"},
        "placement": {
          "cluster": "C",
          "tags": ["ssd"],
        },
        "republish": {
          "src": subjects.single,
          "dest": "events.>",
          "headers_only": true,
        },
      },
      "created": "2026-09-02T00:00:00Z",
      "state": {
        "messages": BigInt.two,
        "bytes": BigInt.from(42),
        "first_seq": BigInt.one,
        "first_ts": "2026-09-02T00:00:00Z",
        "last_seq": BigInt.two,
        "last_ts": "2026-09-02T00:00:01Z",
        "consumer_count": BigInt.zero,
      },
      "ts": "2026-09-02T00:00:02Z",
    }),
    r"$JS.API.STREAM.INFO.TEST",
  );
  return decodeStreamInfo(response);
}
