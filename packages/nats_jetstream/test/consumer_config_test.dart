import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/consumer_wire.dart";
import "package:test/test.dart";

void main() {
  test("pull configuration excludes invalid flow control states", () {
    expect(
      () => JetStreamPullConsumerConfig(
        name: "WORKER",
        ackPolicy: JetStreamConsumerAckPolicy.flowControl,
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamPullConsumerConfig(name: "bad.name"),
      throwsArgumentError,
    );
    expect(
      () => JetStreamPullConsumerConfig(name: "WORKER", maximumBatch: 0),
      throwsArgumentError,
    );
  });

  test("start positions preserve exact sequence and UTC time", () {
    final sequence = BigInt.parse("18446744073709551615");
    expect(JetStreamConsumerStart.sequence(sequence).sequence, sequence);
    expect(
      () => JetStreamConsumerStart.sequence(sequence + BigInt.one),
      throwsArgumentError,
    );
    expect(
      () => JetStreamConsumerStart.time(DateTime(2026)),
      throwsArgumentError,
    );
    expect(
      JetStreamConsumerStart.time(DateTime.utc(1)).time,
      "0001-01-01T00:00:00.000Z",
    );
    expect(
      JetStreamConsumerStart.time(DateTime.utc(9999)).time,
      "9999-01-01T00:00:00.000Z",
    );
    for (final year in [-1, 10000]) {
      expect(
        () => JetStreamConsumerStart.time(DateTime.utc(year)),
        throwsArgumentError,
      );
      expect(
        () => JetStreamPullConsumerConfig(
          name: "WORKER",
          pauseUntil: DateTime.utc(year),
        ),
        throwsArgumentError,
      );
    }
  });

  test("consumer duration fields enforce signed int64", () {
    final maximum = (BigInt.one << 63) - BigInt.one;
    final overflow = maximum + BigInt.one;
    expect(
      () => JetStreamPullConsumerConfig(
        name: "PULL",
        ackWait: JetStreamNanoseconds(maximum),
        backoff: [JetStreamNanoseconds(maximum)],
        maximumExpires: JetStreamNanoseconds(maximum),
        inactiveThreshold: JetStreamNanoseconds(maximum),
      ),
      returnsNormally,
    );
    for (final create in <Object Function()>[
      () => JetStreamPullConsumerConfig(
        name: "PULL",
        ackWait: JetStreamNanoseconds(overflow),
      ),
      () => JetStreamPullConsumerConfig(
        name: "PULL",
        backoff: [JetStreamNanoseconds(overflow)],
      ),
      () => JetStreamPullConsumerConfig(
        name: "PULL",
        maximumExpires: JetStreamNanoseconds(overflow),
      ),
      () => JetStreamPullConsumerConfig(
        name: "PULL",
        inactiveThreshold: JetStreamNanoseconds(overflow),
      ),
      () => JetStreamPushConsumerConfig(
        name: "PUSH",
        deliverSubject: "deliver.push",
        idleHeartbeat: JetStreamNanoseconds(overflow),
      ),
    ]) {
      expect(create, throwsArgumentError);
    }
  });

  test("multiple filters are disjoint and preserve exact order", () {
    final filters = JetStreamConsumerFilters.multiple(const [
      "events.created",
      "events.updated",
    ]);
    expect(filters.subjects, ["events.created", "events.updated"]);
    for (final subjects in [
      ["events.created", "events.created"],
      ["events.>", "events.*"],
      ["*.created", "events.*"],
    ]) {
      expect(
        () => JetStreamConsumerFilters.multiple(subjects),
        throwsArgumentError,
      );
    }
  });

  test("multiple filters enforce the bounded subject set ceiling", () {
    expect(
      () => JetStreamConsumerFilters.multiple([
        for (var index = 0; index < 4096; index += 1) "events.$index",
      ]),
      returnsNormally,
    );
    expect(
      () => JetStreamConsumerFilters.multiple([
        for (var index = 0; index < 4097; index += 1) "events.$index",
      ]),
      throwsArgumentError,
    );
  });

  test("push flow control requires a heartbeat", () {
    expect(
      () => JetStreamPushConsumerConfig(
        name: "PUSH",
        deliverSubject: "delivery.push",
        flowControl: true,
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamPushConsumerConfig(
        name: "PUSH",
        deliverSubject: "delivery.push",
        idleHeartbeat: JetStreamNanoseconds(BigInt.from(99999999)),
      ),
      throwsArgumentError,
    );
  });

  test("maximum acknowledgement pending accepts unlimited", () {
    expect(
      JetStreamPullConsumerConfig(
        name: "PULL",
        maximumAckPending: -1,
      ).maximumAckPending,
      -1,
    );
    expect(
      JetStreamPushConsumerConfig(
        name: "PUSH",
        deliverSubject: "delivery.push",
        maximumAckPending: -1,
      ).maximumAckPending,
      -1,
    );
    for (final invalid in [0, -2]) {
      expect(
        () => JetStreamPullConsumerConfig(
          name: "PULL",
          maximumAckPending: invalid,
        ),
        throwsArgumentError,
      );
    }
  });

  test("advanced pull fields encode exact latest server values", () {
    final pauseUntil = DateTime.utc(2026, 9, 1, 1, 2, 3);
    final metadata = {"owner": "workers"};
    final groups = ["primary", "spill/1"];
    final encoded = encodePullConsumerConfig(
      JetStreamPullConsumerConfig(
        name: "WORKER",
        samplePercentage: 42,
        metadata: metadata,
        pauseUntil: pauseUntil,
        memoryStorage: true,
        replicas: 3,
        priorityPolicy: JetStreamConsumerPriorityPolicy.pinnedClient,
        priorityGroups: groups,
        priorityTimeout: JetStreamNanoseconds(BigInt.from(2000000000)),
      ),
    );

    metadata["owner"] = "changed";
    groups[0] = "changed";
    expect(encoded["sample_freq"], "42%");
    expect(encoded["metadata"], {"owner": "workers"});
    expect(encoded["pause_until"], pauseUntil.toIso8601String());
    expect(encoded["mem_storage"], isTrue);
    expect(encoded["num_replicas"], 3);
    expect(encoded["priority_policy"], "pinned_client");
    expect(encoded["priority_groups"], ["primary", "spill/1"]);
    expect(encoded["priority_timeout"], BigInt.from(2000000000));
  });

  test("advanced consumer options reject invalid local states", () {
    expect(
      () => JetStreamPullConsumerConfig(
        name: "PULL",
        priorityPolicy: JetStreamConsumerPriorityPolicy.overflow,
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamPullConsumerConfig(
        name: "PULL",
        priorityPolicy: JetStreamConsumerPriorityPolicy.overflow,
        priorityGroups: const ["duplicate", "duplicate"],
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamPullConsumerConfig(
        name: "PULL",
        priorityPolicy: JetStreamConsumerPriorityPolicy.prioritized,
        priorityGroups: const ["too_long_priority_group"],
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamPullConsumerConfig(name: "PULL", samplePercentage: 101),
      throwsArgumentError,
    );
    expect(
      () => JetStreamPullConsumerConfig(
        name: "PULL",
        metadata: {"large": "x" * (128 * 1024)},
      ),
      throwsArgumentError,
    );
    expect(
      () =>
          JetStreamPullConsumerConfig(name: "PULL", pauseUntil: DateTime(2026)),
      throwsArgumentError,
    );
  });

  test("ephemeral push state can use memory and one replica", () {
    final encoded = encodePushConsumerConfig(
      JetStreamPushConsumerConfig(
        name: "WATCH",
        deliverSubject: "delivery.watch",
        durable: false,
        memoryStorage: true,
        replicas: 1,
      ),
    );

    expect(encoded["name"], "WATCH");
    expect(encoded, isNot(contains("durable_name")));
    expect(encoded["mem_storage"], isTrue);
    expect(encoded["num_replicas"], 1);
    expect(
      () => JetStreamPushConsumerConfig(
        name: "WATCH",
        deliverSubject: "delivery.watch",
        replicas: 6,
      ),
      throwsArgumentError,
    );
  });

  test("reliable source shape fixes every AckFlowControl invariant", () {
    final config = JetStreamReliableSourceConsumerConfig(
      name: "SOURCE",
      deliverSubject: "delivery.source",
      maximumAckPending: 64,
    );

    expect(config.name, "SOURCE");
    expect(config.maximumAckPending, 64);
  });

  test("reliable source description uses the common UTF 8 boundary", () {
    final exact = "é" * 2048;
    final config = JetStreamReliableSourceConsumerConfig(
      name: "SOURCE",
      deliverSubject: "source.delivery",
      maximumAckPending: 1,
      description: exact,
    );
    expect(encodeReliableSourceConsumerConfig(config)["description"], exact);

    for (final invalid in ["a" * 4097, "\uD800", "\uDC00"]) {
      expect(
        () => JetStreamReliableSourceConsumerConfig(
          name: "SOURCE",
          deliverSubject: "source.delivery",
          maximumAckPending: 1,
          description: invalid,
        ),
        throwsArgumentError,
      );
    }
  });

  test("consumer metadata reserves _nats keys for every config kind", () {
    final reliableMetadata = {"설명": "값"};
    final reliable = JetStreamReliableSourceConsumerConfig(
      name: "SOURCE",
      deliverSubject: "delivery.source",
      maximumAckPending: 64,
      metadata: reliableMetadata,
    );
    reliableMetadata["설명"] = "changed";
    expect(encodeReliableSourceConsumerConfig(reliable)["metadata"], {
      "설명": "값",
      "_nats.req.level": "4",
    });

    for (final key in ["_nats.owner", "_nats.한"]) {
      for (final create in <Object Function()>[
        () =>
            JetStreamPullConsumerConfig(name: "PULL", metadata: {key: "value"}),
        () => JetStreamPushConsumerConfig(
          name: "PUSH",
          deliverSubject: "delivery.push",
          metadata: {key: "value"},
        ),
        () => JetStreamReliableSourceConsumerConfig(
          name: "SOURCE",
          deliverSubject: "delivery.source",
          maximumAckPending: 64,
          metadata: {key: "value"},
        ),
      ]) {
        expect(create, throwsArgumentError, reason: key);
      }
    }
  });

  test("consumer names and text fields use exact UTF8 byte limits", () {
    expect(
      () => JetStreamPullConsumerConfig(name: "😀" * 63 + "abc"),
      returnsNormally,
    );
    expect(
      () => JetStreamPullConsumerConfig(name: "😀" * 64),
      throwsArgumentError,
    );
    expect(
      () => JetStreamPullConsumerConfig(name: String.fromCharCode(0xd800)),
      throwsArgumentError,
    );
    expect(
      () =>
          JetStreamPullConsumerConfig(name: "WORKER", description: "😀" * 1025),
      throwsArgumentError,
    );
    expect(
      () => JetStreamFetchPriority.pinned(group: "primary", pinId: "😀" * 64),
      throwsArgumentError,
    );
  });

  test("finite pull requires explicit message and heartbeat bounds", () {
    expect(
      () => JetStreamFetchRequest(maximumMessages: 0),
      throwsArgumentError,
    );
    expect(
      () => JetStreamFetchRequest(maximumMessages: 1, maximumBytes: 0),
      throwsArgumentError,
    );
    expect(
      () => JetStreamFetchRequest(maximumMessages: 1, maximumBufferedBytes: 0),
      throwsArgumentError,
    );
    const maximumSafeInteger = 9007199254740991;
    expect(
      () => JetStreamFetchRequest(
        maximumMessages: 1,
        maximumBufferedBytes: maximumSafeInteger - 1024,
      ),
      returnsNormally,
    );
    expect(
      () => JetStreamFetchRequest(
        maximumMessages: 1,
        maximumBufferedBytes: maximumSafeInteger - 1023,
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamFetchRequest(
        maximumMessages: 1,
        expires: const Duration(seconds: 1),
        idleHeartbeat: const Duration(milliseconds: 600),
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamFetchRequest(
        maximumMessages: 1,
        noWait: true,
        idleHeartbeat: const Duration(milliseconds: 100),
      ),
      throwsArgumentError,
    );
    const largestMicroseconds = 9223372036854775;
    expect(
      () => JetStreamFetchRequest(
        maximumMessages: 1,
        expires: const Duration(microseconds: largestMicroseconds),
      ),
      returnsNormally,
    );
    expect(
      () => JetStreamFetchRequest(
        maximumMessages: 1,
        expires: const Duration(microseconds: largestMicroseconds + 1),
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamConsumeOptions(
        expires: const Duration(microseconds: largestMicroseconds + 1),
      ),
      throwsArgumentError,
    );
  });

  test("finite pull priority factories isolate policy specific fields", () {
    final overflow = JetStreamFetchPriority.overflow(
      group: "spill",
      minimumPending: BigInt.from(10),
      minimumAcknowledgementsPending: BigInt.from(3),
    );
    expect(overflow.group, "spill");
    expect(overflow.minimumPending, BigInt.from(10));
    expect(overflow.minimumAcknowledgementsPending, BigInt.from(3));

    final pinned = JetStreamFetchPriority.pinned(
      group: "primary",
      pinId: "existing-client",
    );
    expect(pinned.group, "primary");
    expect(pinned.pinId, "existing-client");

    final prioritized = JetStreamFetchPriority.prioritized(
      group: "urgent",
      priority: 0,
    );
    expect(prioritized.group, "urgent");
    expect(prioritized.priority, 0);
    expect(
      () => JetStreamFetchPriority.overflow(
        group: "spill",
        minimumPending: BigInt.zero,
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamFetchPriority.prioritized(group: "urgent", priority: 10),
      throwsArgumentError,
    );
  });

  test("continuous pull options enforce heartbeat and queue bounds", () {
    final options = JetStreamConsumeOptions(
      batchSize: 8,
      expires: const Duration(seconds: 2),
      idleHeartbeat: const Duration(milliseconds: 250),
      maximumPendingMessages: 8,
      maximumPendingBytes: 4096,
    );
    expect(options.batchSize, 8);
    expect(options.maximumPendingMessages, 8);
    expect(
      () => JetStreamConsumeOptions(
        expires: const Duration(milliseconds: 150),
        idleHeartbeat: const Duration(milliseconds: 100),
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamConsumeOptions(maximumPendingMessages: 0),
      throwsArgumentError,
    );
    expect(
      () => JetStreamConsumeOptions(batchSize: 9, maximumPendingMessages: 8),
      throwsArgumentError,
    );
  });
}
