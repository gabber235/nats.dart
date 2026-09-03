import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  test("consumer config deeply snapshots caller raw JSON", () {
    final nestedObject = <String, Object?>{"value": "original"};
    final nestedList = <Object?>[nestedObject];
    final raw = <String, Object?>{"future": nestedList};

    final config = _config(raw);
    raw["added"] = true;
    nestedList.add("mutated");
    nestedObject["value"] = "mutated";

    expect(config.raw, {
      "future": [
        {"value": "original"},
      ],
    });
    _expectDeepRawIsUnmodifiable(config.raw);
  });

  test("consumer info deeply snapshots caller raw JSON", () {
    final nestedObject = <String, Object?>{"value": "original"};
    final nestedList = <Object?>[nestedObject];
    final raw = <String, Object?>{"future": nestedList};

    final info = _info(raw);
    raw["added"] = true;
    nestedList.add("mutated");
    nestedObject["value"] = "mutated";

    expect(info.raw, {
      "future": [
        {"value": "original"},
      ],
    });
    _expectDeepRawIsUnmodifiable(info.raw);
  });
}

JetStreamConsumerConfigSnapshot _config(Map<String, Object?> raw) =>
    JetStreamConsumerConfigSnapshot(
      name: "WORKER",
      durableName: "WORKER",
      kind: JetStreamConsumerKind.pull,
      deliverSubject: null,
      deliverGroup: null,
      deliverPolicy: "all",
      ackPolicy: JetStreamConsumerAckPolicy.explicit,
      replayPolicy: JetStreamConsumerReplayPolicy.instant,
      filterSubjects: const ["events.>"],
      description: null,
      startSequence: null,
      startTime: null,
      ackWait: null,
      maximumDeliveries: null,
      backoff: const [],
      maximumAckPending: null,
      maximumWaiting: null,
      maximumBatch: null,
      maximumExpires: null,
      maximumBytes: null,
      rateLimitBitsPerSecond: null,
      sampleFrequency: null,
      idleHeartbeat: null,
      inactiveThreshold: null,
      headersOnly: false,
      flowControl: false,
      memoryStorage: false,
      replicas: 0,
      metadata: const {},
      pauseUntil: null,
      priorityPolicy: JetStreamConsumerPriorityPolicy.none,
      priorityGroups: const [],
      priorityTimeout: null,
      raw: raw,
    );

JetStreamConsumerInfo _info(Map<String, Object?> raw) => JetStreamConsumerInfo(
  streamName: "EVENTS",
  name: "WORKER",
  created: "2026-09-02T00:00:00Z",
  config: _config(const {}),
  delivered: JetStreamConsumerSequence(
    consumer: BigInt.zero,
    stream: BigInt.zero,
  ),
  acknowledgementFloor: JetStreamConsumerSequence(
    consumer: BigInt.zero,
    stream: BigInt.zero,
  ),
  acknowledgementsPending: BigInt.zero,
  redelivered: BigInt.zero,
  waiting: BigInt.zero,
  pending: BigInt.zero,
  paused: false,
  pushBound: false,
  pauseRemaining: null,
  timestamp: "2026-09-02T00:00:01Z",
  priorityGroups: const [],
  raw: raw,
);

void _expectDeepRawIsUnmodifiable(Map<String, Object?> raw) {
  final nestedList = raw["future"]! as List<Object?>;
  final nestedObject = nestedList.single! as Map<String, Object?>;

  expect(() => raw["added"] = true, throwsUnsupportedError);
  expect(() => raw.remove("future"), throwsUnsupportedError);
  expect(() => nestedList.add("added"), throwsUnsupportedError);
  expect(() => nestedList[0] = "replaced", throwsUnsupportedError);
  expect(() => nestedList.remove(nestedObject), throwsUnsupportedError);
  expect(() => nestedObject["value"] = "replaced", throwsUnsupportedError);
  expect(() => nestedObject.remove("value"), throwsUnsupportedError);
}
