import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  group("snapshot metadata", () {
    test("snapshots exact nested caller state", () {
      final subjects = <Object?>["events.>"];
      final unknown = <String, Object?>{
        "counter": BigInt.parse("18446744073709551615"),
      };
      final config = <String, Object?>{
        "name": "EVENTS",
        "subjects": subjects,
        "future": unknown,
      };
      final deleted = <Object?>[BigInt.two];
      final state = <String, Object?>{"deleted": deleted};

      final metadata = JetStreamSnapshotMetadata(config: config, state: state);

      subjects.add("mutated");
      unknown["counter"] = BigInt.zero;
      deleted.add(BigInt.from(3));
      config["name"] = "MUTATED";

      expect(metadata.stream, "EVENTS");
      expect(metadata.config["subjects"], ["events.>"]);
      expect(metadata.config["future"], {
        "counter": BigInt.parse("18446744073709551615"),
      });
      expect(metadata.state["deleted"], [BigInt.two]);
      expect(() => metadata.config["name"] = "MUTATED", throwsUnsupportedError);
      expect(
        () => (metadata.config["subjects"]! as List<Object?>).add("mutated"),
        throwsUnsupportedError,
      );
    });

    test("requires a nonempty stream name", () {
      expect(
        () => JetStreamSnapshotMetadata(config: const {}, state: const {}),
        throwsArgumentError,
      );
      expect(
        () => JetStreamSnapshotMetadata(
          config: const {"name": ""},
          state: const {},
        ),
        throwsArgumentError,
      );
      expect(
        () => JetStreamSnapshotMetadata(
          config: const {"name": 42},
          state: const {},
        ),
        throwsArgumentError,
      );
    });
  });

  group("snapshot options", () {
    test("accepts every official boundary", () {
      final minimum = JetStreamSnapshotOptions(
        chunkSize: 1024,
        windowSize: 1024,
        maximumBufferedBytes: 1024,
      );
      final maximum = JetStreamSnapshotOptions(
        chunkSize: 1024 * 1024,
        windowSize: 32 * 1024 * 1024,
        maximumBufferedBytes: 32 * 1024 * 1024,
      );

      expect(minimum.chunkSize, 1024);
      expect(maximum.chunkSize, 1024 * 1024);
      expect(maximum.windowSize, 32 * 1024 * 1024);
    });

    test("rejects values outside the official bounds", () {
      expect(
        () => JetStreamSnapshotOptions(chunkSize: 1023),
        throwsArgumentError,
      );
      expect(
        () => JetStreamSnapshotOptions(chunkSize: 1024 * 1024 + 1),
        throwsArgumentError,
      );
      expect(
        () => JetStreamSnapshotOptions(windowSize: 1023),
        throwsArgumentError,
      );
      expect(
        () => JetStreamSnapshotOptions(windowSize: 32 * 1024 * 1024 + 1),
        throwsArgumentError,
      );
      expect(
        () => JetStreamSnapshotOptions(chunkSize: 2048, windowSize: 1024),
        throwsArgumentError,
      );
      expect(
        () => JetStreamSnapshotOptions(maximumBufferedBytes: 1024),
        throwsArgumentError,
      );
      expect(
        () => JetStreamSnapshotOptions(timeout: Duration.zero),
        throwsArgumentError,
      );
    });
  });

  group("restore options", () {
    test("accepts chunk and timeout boundaries", () {
      final minimum = JetStreamRestoreOptions(
        uploadChunkSize: 1,
        chunkTimeout: const Duration(microseconds: 1),
      );
      final maximum = JetStreamRestoreOptions(
        uploadChunkSize: 1024 * 1024,
        chunkTimeout: const Duration(milliseconds: 4999),
      );

      expect(minimum.uploadChunkSize, 1);
      expect(maximum.uploadChunkSize, 1024 * 1024);
    });

    test("rejects unsafe upload settings", () {
      expect(
        () => JetStreamRestoreOptions(uploadChunkSize: 0),
        throwsArgumentError,
      );
      expect(
        () => JetStreamRestoreOptions(uploadChunkSize: 1024 * 1024 + 1),
        throwsArgumentError,
      );
      expect(
        () => JetStreamRestoreOptions(chunkTimeout: Duration.zero),
        throwsArgumentError,
      );
      expect(
        () => JetStreamRestoreOptions(chunkTimeout: const Duration(seconds: 5)),
        throwsArgumentError,
      );
      expect(
        () => JetStreamRestoreOptions(completionTimeout: Duration.zero),
        throwsArgumentError,
      );
    });
  });

  test("transfer failures retain exact progress and status", () {
    final snapshotCause = StateError("highly-secret-snapshot-value");
    final restoreCause = StateError("highly-secret-restore-value");
    final snapshot = JetStreamSnapshotException(
      partialBytes: BigInt.parse("18446744073709551615"),
      cause: snapshotCause,
      statusCode: 408,
      statusDescription: "No Flow Response",
    );
    final restore = JetStreamRestoreException(
      partialBytes: BigInt.from(9007199254740993),
      cause: restoreCause,
    );

    expect(snapshot.partialBytes, BigInt.parse("18446744073709551615"));
    expect(snapshot.cause, same(snapshotCause));
    expect(snapshot.statusCode, 408);
    expect(snapshot.statusDescription, "No Flow Response");
    expect(snapshot.toString(), contains("18446744073709551615"));
    expect(snapshot.toString(), contains("status 408"));
    expect(snapshot.toString(), isNot(contains("highly-secret")));
    expect(restore.partialBytes, BigInt.from(9007199254740993));
    expect(restore.cause, same(restoreCause));
    expect(restore.toString(), contains("9007199254740993"));
    expect(restore.toString(), isNot(contains("highly-secret")));
  });
}
