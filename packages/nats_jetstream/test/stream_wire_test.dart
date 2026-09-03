import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/stream_wire.dart";
import "package:nats_jetstream/src/wire_response.dart";
import "package:test/test.dart";

void main() {
  test(
    "stream info preserves complete state, topology, and unknown fields",
    () {
      final response = JetStreamWireResponse.decode(
        encodeExactJson({
          "config": {
            "name": "EVENTS",
            "subjects": ["events.>"],
            "storage": "future_storage",
            "retention": "limits",
            "discard": "old",
            "compression": "none",
            "future_config": {"enabled": true},
          },
          "created": "2026-09-01T00:00:00Z",
          "state": {
            "messages": BigInt.from(3),
            "bytes": BigInt.from(99),
            "first_seq": BigInt.one,
            "first_ts": "2026-09-01T00:00:01Z",
            "last_seq": BigInt.from(4),
            "last_ts": "2026-09-01T00:00:04Z",
            "consumer_count": BigInt.from(2),
            "deleted": [BigInt.from(2)],
            "num_deleted": BigInt.one,
            "num_subjects": BigInt.from(2),
            "subjects": {"events.a": BigInt.from(2), "events.b": BigInt.one},
            "future_state": "preserved",
          },
          "cluster": {
            "name": "C",
            "raft_group": "R",
            "leader": "A",
            "leader_since": "2026-09-01T00:00:00Z",
            "system_account": true,
            "traffic_account": "SYS",
            "replicas": [
              {
                "name": "B",
                "current": true,
                "offline": false,
                "active": BigInt.from(12),
                "lag": BigInt.zero,
              },
            ],
          },
          "mirror": {
            "name": "ORIGIN",
            "lag": BigInt.one,
            "active": BigInt.from(-1),
          },
          "sources": [
            {
              "name": "SOURCE",
              "lag": BigInt.from(2),
              "active": BigInt.from(5),
              "filter_subject": "source.>",
              "subject_transforms": [
                {"src": "source.*", "dest": r"events.{{wildcard(1)}}"},
              ],
            },
          ],
          "ts": "2026-09-01T00:00:05Z",
          "future_info": BigInt.from(9007199254740993),
        }),
        r"$JS.API.STREAM.INFO.EVENTS",
      );

      final info = decodeStreamInfo(response);

      expect(info.messages, BigInt.from(3));
      expect(info.state.bytes, BigInt.from(99));
      expect(info.state.deleted, [BigInt.from(2)]);
      expect(info.state.subjects, {
        "events.a": BigInt.from(2),
        "events.b": BigInt.one,
      });
      expect(info.cluster?.leader, "A");
      expect(info.cluster?.replicas.single.name, "B");
      expect(info.mirror?.activeNanoseconds, BigInt.from(-1));
      expect(
        info.sources.single.subjectTransforms.single.destination,
        r"events.{{wildcard(1)}}",
      );
      expect(info.config.raw["future_config"], {"enabled": true});
      expect(info.config.storageWireValue, "future_storage");
      expect(info.config.knownStorage, isNull);
      expect(info.state.raw["future_state"], "preserved");
      expect(info.raw["future_info"], BigInt.from(9007199254740993));
      expect(
        () => requireStreamIdentity(response, info, "OTHER"),
        throwsA(
          isA<JetStreamProtocolException>().having(
            (error) => error.fieldPath,
            "fieldPath",
            "config.name",
          ),
        ),
      );
    },
  );

  test("stream state rejects uint64 and signed int64 overflow", () {
    final uint64Overflow = BigInt.one << 64;
    final int64Overflow = BigInt.one << 63;
    final cases = <(String, void Function(Map<String, Object?>))>[
      for (final field in [
        "messages",
        "bytes",
        "first_seq",
        "last_seq",
        "num_deleted",
        "num_subjects",
      ])
        (
          "state.$field",
          (body) =>
              (body["state"]! as Map<String, Object?>)[field] = uint64Overflow,
        ),
      (
        "state.deleted",
        (body) => (body["state"]! as Map<String, Object?>)["deleted"] = [
          uint64Overflow,
        ],
      ),
      (
        "state.subjects",
        (body) => (body["state"]! as Map<String, Object?>)["subjects"] = {
          "events.one": uint64Overflow,
        },
      ),
      (
        "state.consumer_count",
        (body) => (body["state"]! as Map<String, Object?>)["consumer_count"] =
            int64Overflow,
      ),
      (
        "sources.active",
        (body) => body["sources"] = [
          {"name": "SOURCE", "lag": BigInt.zero, "active": int64Overflow},
        ],
      ),
    ];

    for (final (path, mutate) in cases) {
      final body = _minimalStreamInfo();
      mutate(body);
      final response = JetStreamWireResponse.decode(
        encodeExactJson(body),
        r"$JS.API.STREAM.INFO.EVENTS",
      );
      expect(
        () => decodeStreamInfo(response),
        throwsA(
          isA<JetStreamProtocolException>().having(
            (error) => error.fieldPath,
            "fieldPath",
            path,
          ),
        ),
        reason: path,
      );
    }
  });

  test("stream state rejects negative counts and peer activity", () {
    final cases = <(String, void Function(Map<String, Object?>))>[
      (
        "state.consumer_count",
        (body) => (body["state"]! as Map<String, Object?>)["consumer_count"] =
            BigInt.from(-1),
      ),
      (
        "cluster.replicas.active",
        (body) => body["cluster"] = {
          "replicas": [
            {"name": "B", "current": true, "active": BigInt.from(-1)},
          ],
        },
      ),
    ];

    for (final (path, mutate) in cases) {
      final body = _minimalStreamInfo();
      mutate(body);
      final response = JetStreamWireResponse.decode(
        encodeExactJson(body),
        r"$JS.API.STREAM.INFO.EVENTS",
      );
      expect(
        () => decodeStreamInfo(response),
        throwsA(
          isA<JetStreamProtocolException>().having(
            (error) => error.fieldPath,
            "fieldPath",
            path,
          ),
        ),
      );
    }
  });

  test("stream response overlap failures remain protocol exceptions", () {
    final body = _minimalStreamInfo();
    (body["config"]! as Map<String, Object?>)["subjects"] = [
      "events.>",
      "events.one",
    ];
    final response = JetStreamWireResponse.decode(
      encodeExactJson(body),
      r"$JS.API.STREAM.INFO.EVENTS",
    );

    expect(
      () => decodeStreamInfo(response),
      throwsA(isA<JetStreamProtocolException>()),
    );
  });

  test("stream response validates every RFC 3339 field", () {
    final cases = <(String, void Function(Map<String, Object?>))>[
      ("created", (body) => body["created"] = "2026-09-42T00:00:00Z"),
      ("ts", (body) => body["ts"] = "2026-09-03T24:00:00Z"),
      (
        "state.first_ts",
        (body) => (body["state"]! as Map<String, Object?>)["first_ts"] =
            "2025-02-29T00:00:00Z",
      ),
      (
        "state.last_ts",
        (body) => (body["state"]! as Map<String, Object?>)["last_ts"] =
            "2026-09-03T00:00:00+24:00",
      ),
      (
        "cluster.leader_since",
        (body) => body["cluster"] = {
          "leader_since": "2026-09-03T00:00:00.1234567890Z",
        },
      ),
    ];

    for (final (path, mutate) in cases) {
      final body = _minimalStreamInfo();
      mutate(body);
      final response = JetStreamWireResponse.decode(
        encodeExactJson(body),
        r"$JS.API.STREAM.INFO.EVENTS",
      );
      expect(
        () => decodeStreamInfo(response),
        throwsA(
          isA<JetStreamProtocolException>().having(
            (error) => error.fieldPath,
            "fieldPath",
            path,
          ),
        ),
        reason: path,
      );
    }
  });
}

Map<String, Object?> _minimalStreamInfo() => {
  "config": {
    "name": "EVENTS",
    "subjects": ["events.>"],
    "storage": "memory",
    "retention": "limits",
    "discard": "old",
  },
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
  "ts": "2026-09-02T00:00:00Z",
};
