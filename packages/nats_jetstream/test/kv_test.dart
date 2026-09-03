import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  test("bucket configuration validates identity and history", () {
    expect(
      () => JetStreamKeyValueBucketConfig(name: "bad.bucket"),
      throwsArgumentError,
    );
    expect(
      () => JetStreamKeyValueBucketConfig(name: "GOOD", history: 0),
      throwsArgumentError,
    );
    expect(
      JetStreamKeyValueBucketConfig(name: "GOOD", history: 64).history,
      64,
    );
  });

  test("bucket metadata enforces exact shared server limits", () {
    final metadata = {"k": "v"};
    final config = JetStreamKeyValueBucketConfig(
      name: "GOOD",
      metadata: metadata,
    );
    metadata["k"] = "changed";

    expect(config.metadata, const {"k": "v"});
    expect(
      JetStreamKeyValueBucketConfig(
        name: "GOOD",
        metadata: {"k": "x" * (128 * 1024 - 1)},
      ).metadata["k"]?.length,
      128 * 1024 - 1,
    );
    expect(
      () => JetStreamKeyValueBucketConfig(
        name: "GOOD",
        metadata: {"k": "x" * (128 * 1024)},
      ),
      throwsArgumentError,
    );
    for (final create in <Object Function()>[
      () => JetStreamKeyValueBucketConfig(name: "GOOD", description: "\uD800"),
      () => JetStreamKeyValueBucketConfig(
        name: "GOOD",
        metadata: const {"\uD800": "value"},
      ),
      () => JetStreamKeyValueBucketConfig(
        name: "GOOD",
        metadata: const {"key": "\uD800"},
      ),
    ]) {
      expect(create, throwsArgumentError);
    }
  });

  test("key validation accepts hierarchy and rejects wildcard ambiguity", () {
    validateJetStreamKeyValueKey("users.one.profile");
    for (final key in ["", ".start", "end.", "a..b", "a.*", "a.>"]) {
      expect(() => validateJetStreamKeyValueKey(key), throwsArgumentError);
    }
  });

  test("entry snapshots mutable value bytes", () {
    final value = [0, 255, 13, 10];
    final entry = JetStreamKeyValueEntry(
      bucket: "B",
      key: "k",
      value: value,
      revision: BigInt.one,
      created: "2026-09-01T00:00:00Z",
      operation: JetStreamKeyValueOperation.put,
    );
    value[0] = 9;

    expect(entry.value, [0, 255, 13, 10]);
    expect(() => entry.value[0] = 9, throwsUnsupportedError);
  });

  test("latest TTL configuration rejects subsecond lifetimes", () {
    expect(
      () => JetStreamKeyValueBucketConfig(
        name: "GOOD",
        limitMarkerTtl: JetStreamNanoseconds(BigInt.from(999999999)),
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamKeyValueLifetime.expiresAfter(
        const Duration(milliseconds: 1500),
      ),
      throwsArgumentError,
    );
    expect(
      JetStreamKeyValueLifetime.expiresAfter(
        const Duration(seconds: 2),
      ).wireValue,
      "2s",
    );
    expect(const JetStreamKeyValueLifetime.never().wireValue, "never");
  });

  test("watch options make invalid starts and bounds impossible", () {
    expect(
      () => JetStreamKeyValueWatchStart.revision(BigInt.zero),
      throwsArgumentError,
    );
    for (final filter in [".invalid", "a..b", "a.>.b", "a*"]) {
      expect(
        () => JetStreamKeyValueWatchOptions(filter: filter),
        throwsArgumentError,
      );
    }
    expect(
      () => JetStreamKeyValueWatchOptions(maximumPendingMessages: 0),
      throwsArgumentError,
    );
    final options = JetStreamKeyValueWatchOptions(
      start: const JetStreamKeyValueWatchStart.updates(),
      ignoreDeletes: true,
    );
    expect(options.start.kind, JetStreamKeyValueWatchStartKind.updates);
    expect(options.ignoreDeletes, isTrue);
  });

  test("KV wire integers accept exact maxima and reject overflow", () {
    final int64 = (BigInt.one << 63) - BigInt.one;
    final uint64 = (BigInt.one << 64) - BigInt.one;
    final int32 = (BigInt.one << 31) - BigInt.one;
    expect(
      () => JetStreamKeyValueBucketConfig(
        name: "GOOD",
        maximumAge: JetStreamNanoseconds(int64),
        maximumBytes: int64,
        maximumValueBytes: int32,
      ),
      returnsNormally,
    );
    expect(() => JetStreamKeyValueWatchStart.revision(uint64), returnsNormally);
    for (final create in <Object Function()>[
      () => JetStreamKeyValueBucketConfig(
        name: "GOOD",
        maximumAge: JetStreamNanoseconds(int64 + BigInt.one),
      ),
      () => JetStreamKeyValueBucketConfig(
        name: "GOOD",
        maximumBytes: int64 + BigInt.one,
      ),
      () => JetStreamKeyValueBucketConfig(
        name: "GOOD",
        maximumValueBytes: int32 + BigInt.one,
      ),
      () => JetStreamKeyValueWatchStart.revision(uint64 + BigInt.one),
    ]) {
      expect(create, throwsArgumentError);
    }
  });
}
