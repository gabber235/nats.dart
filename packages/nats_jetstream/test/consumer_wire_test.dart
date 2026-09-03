import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/consumer_wire.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/wire_response.dart";
import "package:test/test.dart";

void main() {
  test("consumer info preserves every stable field and unknown JSON", () {
    final maximumUint64 = BigInt.parse("18446744073709551615");
    final response = JetStreamWireResponse.decode(
      encodeExactJson({
        "stream_name": "EVENTS",
        "name": "WORKER",
        "created": "2026-09-01T00:00:00Z",
        "config": {
          "name": "WORKER",
          "durable_name": "WORKER",
          "description": "latest fixture",
          "deliver_policy": "by_start_sequence",
          "opt_start_seq": BigInt.from(7),
          "opt_start_time": "2026-08-31T00:00:00Z",
          "ack_policy": "explicit",
          "ack_wait": BigInt.from(30000000000),
          "max_deliver": BigInt.from(8),
          "backoff": [BigInt.from(1000000000), BigInt.from(2000000000)],
          "filter_subjects": ["events.one", "events.two"],
          "replay_policy": "original",
          "rate_limit_bps": maximumUint64,
          "sample_freq": "42%",
          "max_waiting": BigInt.from(512),
          "max_ack_pending": BigInt.from(-1),
          "max_batch": BigInt.from(32),
          "max_expires": BigInt.from(5000000000),
          "max_bytes": BigInt.from(65536),
          "idle_heartbeat": BigInt.from(100000000),
          "inactive_threshold": BigInt.from(60000000000),
          "headers_only": true,
          "flow_control": true,
          "mem_storage": true,
          "num_replicas": BigInt.from(3),
          "metadata": {"owner": "workers", "_nats.level": "4"},
          "pause_until": "2026-09-01T01:00:00Z",
          "priority_policy": "pinned_client",
          "priority_groups": ["primary", "spill"],
          "priority_timeout": BigInt.from(120000000000),
          "future_config": {"exact": BigInt.parse("18446744073709551615")},
        },
        "delivered": {
          "consumer_seq": BigInt.from(11),
          "stream_seq": BigInt.from(17),
          "last_active": "2026-09-01T00:00:01Z",
        },
        "ack_floor": {
          "consumer_seq": BigInt.from(9),
          "stream_seq": BigInt.from(15),
        },
        "num_ack_pending": BigInt.from(2),
        "num_redelivered": BigInt.one,
        "num_waiting": BigInt.from(4),
        "num_pending": BigInt.from(6),
        "push_bound": true,
        "paused": true,
        "pause_remaining": BigInt.from(500000000),
        "ts": "2026-09-01T00:00:02Z",
        "priority_groups": [
          {
            "group": "primary",
            "pinned_client_id": "client-1",
            "pinned_ts": "2026-09-01T00:00:01Z",
          },
        ],
        "future_info": [
          {"exact": BigInt.parse("18446744073709551615")},
        ],
      }),
      r"$JS.API.CONSUMER.INFO.EVENTS.WORKER",
    );

    final info = decodeConsumerInfo(response);
    expect(info.streamName, "EVENTS");
    expect(info.config.description, "latest fixture");
    expect(info.config.startSequence, BigInt.from(7));
    expect(info.config.startTime, "2026-08-31T00:00:00Z");
    expect(info.config.ackWait?.nanoseconds, BigInt.from(30000000000));
    expect(info.config.backoff.map((value) => value.nanoseconds), [
      BigInt.from(1000000000),
      BigInt.from(2000000000),
    ]);
    expect(info.config.filterSubjects, ["events.one", "events.two"]);
    expect(info.config.rateLimitBitsPerSecond, maximumUint64);
    expect(info.config.maximumAckPending, -1);
    expect(info.config.sampleFrequency, "42%");
    expect(info.config.memoryStorage, isTrue);
    expect(info.config.replicas, 3);
    expect(info.config.metadata, {"owner": "workers", "_nats.level": "4"});
    expect(
      info.config.priorityPolicy,
      JetStreamConsumerPriorityPolicy.pinnedClient,
    );
    expect(info.config.priorityGroups, ["primary", "spill"]);
    expect(info.pushBound, isTrue);
    expect(info.pauseRemaining?.nanoseconds, BigInt.from(500000000));
    expect(info.priorityGroups.single.group, "primary");
    expect(info.priorityGroups.single.pinnedClientId, "client-1");
    expect(
      (info.config.raw["future_config"] as Map<String, Object?>)["exact"],
      maximumUint64,
    );
    expect(
      () => (info.config.raw["future_config"] as Map<String, Object?>).clear(),
      throwsUnsupportedError,
    );
    expect(
      () => (info.raw["future_info"] as List<Object?>).clear(),
      throwsUnsupportedError,
    );
    expect(
      () => requireConsumerIdentity(
        response,
        info,
        expectedStream: "OTHER",
        expectedConsumer: "WORKER",
      ),
      throwsA(
        isA<JetStreamProtocolException>().having(
          (error) => error.fieldPath,
          "fieldPath",
          "stream_name",
        ),
      ),
    );
    expect(
      () => requireConsumerIdentity(
        response,
        info,
        expectedStream: "EVENTS",
        expectedConsumer: "OTHER",
      ),
      throwsA(
        isA<JetStreamProtocolException>().having(
          (error) => error.fieldPath,
          "fieldPath",
          "name",
        ),
      ),
    );

    final mismatchedBody = Map<String, Object?>.of(response.body);
    final mismatchedConfig = Map<String, Object?>.of(
      mismatchedBody["config"]! as Map<String, Object?>,
    )..["name"] = "OTHER";
    mismatchedBody["config"] = mismatchedConfig;
    final mismatchedResponse = JetStreamWireResponse.decode(
      encodeExactJson(mismatchedBody),
      response.subject,
    );
    final mismatchedInfo = decodeConsumerInfo(mismatchedResponse);
    expect(
      () => requireConsumerIdentity(
        mismatchedResponse,
        mismatchedInfo,
        expectedStream: "EVENTS",
        expectedConsumer: "WORKER",
      ),
      throwsA(
        isA<JetStreamProtocolException>().having(
          (error) => error.fieldPath,
          "fieldPath",
          "config.name",
        ),
      ),
    );
  });

  test("consumer positions and counts enforce schema integer ranges", () {
    final uint64Overflow = BigInt.one << 64;
    final int64Overflow = BigInt.one << 63;
    final cases = <(String, void Function(Map<String, Object?>))>[
      for (final container in ["delivered", "ack_floor"])
        for (final field in ["consumer_seq", "stream_seq"])
          (
            field,
            (body) => (body[container]! as Map<String, Object?>)[field] =
                uint64Overflow,
          ),
      ("num_pending", (body) => body["num_pending"] = uint64Overflow),
      for (final field in ["opt_start_seq", "rate_limit_bps"])
        (
          "config.$field",
          (body) =>
              (body["config"]! as Map<String, Object?>)[field] = uint64Overflow,
        ),
      for (final field in ["num_ack_pending", "num_redelivered", "num_waiting"])
        (field, (body) => body[field] = int64Overflow),
      for (final field in [
        "ack_wait",
        "max_expires",
        "idle_heartbeat",
        "inactive_threshold",
        "priority_timeout",
      ])
        (
          "config.$field",
          (body) =>
              (body["config"]! as Map<String, Object?>)[field] = int64Overflow,
        ),
      (
        "config.backoff",
        (body) => (body["config"]! as Map<String, Object?>)["backoff"] = [
          int64Overflow,
        ],
      ),
      ("pause_remaining", (body) => body["pause_remaining"] = int64Overflow),
    ];

    for (final (path, mutate) in cases) {
      final body = _minimalConsumerInfo();
      mutate(body);
      final response = JetStreamWireResponse.decode(
        encodeExactJson(body),
        r"$JS.API.CONSUMER.INFO.EVENTS.WORKER",
      );
      expect(
        () => decodeConsumerInfo(response),
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

  test("consumer configuration enforces schema semantic ranges", () {
    final cases = <(String, BigInt)>[
      ("ack_wait", BigInt.zero),
      ("max_waiting", BigInt.from(-1)),
      ("max_batch", BigInt.from(-1)),
      ("max_bytes", BigInt.from(-1)),
      ("num_replicas", BigInt.from(-1)),
      ("num_replicas", BigInt.from(6)),
    ];

    for (final (field, value) in cases) {
      final body = _minimalConsumerInfo();
      (body["config"]! as Map<String, Object?>)[field] = value;
      final response = JetStreamWireResponse.decode(
        encodeExactJson(body),
        r"$JS.API.CONSUMER.INFO.EVENTS.WORKER",
      );
      expect(
        () => decodeConsumerInfo(response),
        throwsA(
          isA<JetStreamProtocolException>().having(
            (error) => error.fieldPath,
            "fieldPath",
            "config.$field",
          ),
        ),
        reason: field,
      );
    }
  });

  test("consumer response validates every RFC 3339 field", () {
    final cases = <(String, void Function(Map<String, Object?>))>[
      ("created", (body) => body["created"] = "2026-09-42T00:00:00Z"),
      ("ts", (body) => body["ts"] = "2026-09-03T24:00:00Z"),
      (
        "config.opt_start_time",
        (body) => (body["config"]! as Map<String, Object?>)["opt_start_time"] =
            "2025-02-29T00:00:00Z",
      ),
      (
        "config.pause_until",
        (body) => (body["config"]! as Map<String, Object?>)["pause_until"] =
            "2026-09-03T00:00:00+00:60",
      ),
      (
        "last_active",
        (body) => (body["delivered"]! as Map<String, Object?>)["last_active"] =
            "2026-09-03T00:00:00.Z",
      ),
      (
        "priority_groups.pinned_ts",
        (body) => body["priority_groups"] = [
          {"group": "primary", "pinned_ts": "2026-09-03T00:00:00.1234567890Z"},
        ],
      ),
    ];

    for (final (path, mutate) in cases) {
      final body = _minimalConsumerInfo();
      mutate(body);
      final response = JetStreamWireResponse.decode(
        encodeExactJson(body),
        r"$JS.API.CONSUMER.INFO.EVENTS.WORKER",
      );
      expect(
        () => decodeConsumerInfo(response),
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

Map<String, Object?> _minimalConsumerInfo() => {
  "stream_name": "EVENTS",
  "name": "WORKER",
  "created": "2026-09-02T00:00:00Z",
  "config": <String, Object?>{
    "name": "WORKER",
    "deliver_policy": "all",
    "ack_policy": "explicit",
    "replay_policy": "instant",
  },
  "delivered": <String, Object?>{
    "consumer_seq": BigInt.zero,
    "stream_seq": BigInt.zero,
  },
  "ack_floor": <String, Object?>{
    "consumer_seq": BigInt.zero,
    "stream_seq": BigInt.zero,
  },
  "num_ack_pending": BigInt.zero,
  "num_redelivered": BigInt.zero,
  "num_waiting": BigInt.zero,
  "num_pending": BigInt.zero,
  "ts": "2026-09-02T00:00:00Z",
};
