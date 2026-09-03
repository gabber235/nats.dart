import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  test("stream configuration snapshots nested caller state", () {
    final subjects = ["events.>"];
    final tags = ["region:eu"];
    final metadata = {"owner": "payments"};
    final config = JetStreamStreamConfig(
      name: "EVENTS",
      subjects: subjects,
      placement: JetStreamPlacement(tags: tags),
      metadata: metadata,
    );

    subjects.add("mutated");
    tags.add("mutated");
    metadata["owner"] = "mutated";

    expect(config.subjects, ["events.>"]);
    expect(config.placement?.tags, ["region:eu"]);
    expect(config.metadata, {"owner": "payments"});
  });

  test("stream configuration rejects destructive ambiguous states", () {
    expect(
      () => JetStreamStreamConfig(
        name: "MIRROR",
        subjects: const ["events.>"],
        mirror: JetStreamStreamSource(name: "ORIGIN"),
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamStreamConfig(
        name: "LIMITS",
        subjects: const ["events.>"],
        discardNewPerSubject: true,
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamStreamConfig(
        name: "LIMITS",
        subjects: const ["events.>"],
        discard: JetStreamDiscardPolicy.newMessages,
        discardNewPerSubject: true,
      ),
      throwsArgumentError,
    );
  });

  test("stream source validates exclusive starts and consumer identity", () {
    expect(
      () => JetStreamStreamSource(
        name: "ORIGIN",
        startSequence: BigInt.one,
        startTime: DateTime.utc(2026),
      ),
      throwsArgumentError,
    );
    expect(JetStreamStreamConsumerSource.new, throwsArgumentError);
    expect(
      JetStreamStreamSource(
        name: "ORIGIN",
        startTime: DateTime.utc(1),
      ).toJson()["opt_start_time"],
      "0001-01-01T00:00:00.000Z",
    );
    expect(
      JetStreamStreamSource(
        name: "ORIGIN",
        startTime: DateTime.utc(9999),
      ).toJson()["opt_start_time"],
      "9999-01-01T00:00:00.000Z",
    );
    for (final year in [-1, 10000]) {
      expect(
        () => JetStreamStreamSource(
          name: "ORIGIN",
          startTime: DateTime.utc(year),
        ),
        throwsArgumentError,
      );
    }
  });

  test("stream source transforms require disjoint source patterns", () {
    final transforms = [
      JetStreamSubjectTransform(
        source: "events.created",
        destination: "archive.created",
      ),
      JetStreamSubjectTransform(
        source: "events.updated",
        destination: "archive.updated",
      ),
    ];
    final source = JetStreamStreamSource(
      name: "ORIGIN",
      subjectTransforms: transforms,
    );
    expect(source.subjectTransforms, transforms);
    for (final patterns in [
      ["events.created", "events.created"],
      ["events.>", "events.*"],
      ["*.created", "events.*"],
    ]) {
      expect(
        () => JetStreamStreamSource(
          name: "ORIGIN",
          subjectTransforms: [
            JetStreamSubjectTransform(
              source: patterns.first,
              destination: "archive.one",
            ),
            JetStreamSubjectTransform(
              source: patterns.last,
              destination: "archive.two",
            ),
          ],
        ),
        throwsArgumentError,
      );
    }
    expect(
      () => JetStreamStreamSource(
        name: "ORIGIN",
        subjectTransforms: [
          JetStreamSubjectTransform(source: "", destination: ""),
          JetStreamSubjectTransform(
            source: "events.created",
            destination: "archive.created",
          ),
        ],
      ),
      throwsArgumentError,
    );
  });

  test("stream subject sets enforce the explicit complexity ceiling", () {
    expect(
      () => JetStreamStreamConfig(
        name: "EVENTS",
        subjects: [
          for (var index = 0; index < 4097; index += 1) "events.$index",
        ],
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamStreamSource(
        name: "ORIGIN",
        subjectTransforms: [
          for (var index = 0; index < 4097; index += 1)
            JetStreamSubjectTransform(
              source: "events.$index",
              destination: "archive.$index",
            ),
        ],
      ),
      throwsArgumentError,
    );
  });

  test("subject transforms accept every NATS 2.14 function", () {
    final destinations = [
      r"bar.{{Wildcard(1)}}",
      r"bar.{{Partition(10,1,2)}}",
      r"bar.{{SplitFromLeft(1,2)}}",
      r"bar.{{SplitFromRight(1,2)}}",
      r"bar.{{SliceFromLeft(1,2)}}",
      r"bar.{{SliceFromRight(1,2)}}",
      r"bar.{{Split(1,-)}}",
      r"bar.{{Split(1,)}}",
      r"bar.{{Left(1,2)}}",
      r"bar.{{Right(1,2)}}",
      r"bar.{{Random(10)}}",
      r"bar.$2.$1",
    ];
    for (final destination in destinations) {
      expect(
        () => JetStreamSubjectTransform(
          source: "foo.*.*",
          destination: destination,
        ),
        returnsNormally,
        reason: destination,
      );
    }
    expect(
      () => JetStreamSubjectTransform(
        source: "foo.*",
        destination: r"bar.{{wildcard(0)}}",
      ),
      returnsNormally,
    );
    for (final mapping in <(String, String)>[
      ("", "prefix.>"),
      ("foo", ""),
      ("foo.*.*", r"bar.$2"),
      ("foo.*.>", r"bar.$1.>"),
      ("foo.*", r"bar.{{partition(-1,0)}}"),
      ("foo", r"bar.{{partition(10,999)}}"),
      ("foo.*", r"bar.{{random(-1)}}"),
      ("foo.*", r"bar.{{left(-1,-2147483648)}}"),
      ("foo.*", r"bar.$literal"),
      ("foo.*", "bar.foo*bar"),
    ]) {
      expect(
        () => JetStreamSubjectTransform(
          source: mapping.$1,
          destination: mapping.$2,
        ),
        returnsNormally,
        reason: "${mapping.$1} to ${mapping.$2}",
      );
    }
    expect(
      () => JetStreamSubjectTransform(
        source: "foo.bar",
        destination: r"bar.{{partition(2147483647)}}",
      ),
      returnsNormally,
    );
    expect(
      () => JetStreamSubjectTransform(source: "foo.>", destination: "bar.>"),
      returnsNormally,
    );
  });

  test("subject transforms reject exact NATS 2.14 grammar failures", () {
    final cases = <(String, String)>[
      ("foo.*", "bar.*"),
      ("foo.*", "bar.>"),
      ("foo.>", "bar"),
      ("foo.*", r"bar.$2"),
      ("foo.>", r"bar.$1.>"),
      ("foo.*", r"bar.{{partition(10,2)}}"),
      ("foo.*", r"bar.{{unknown(1)}}"),
      ("foo.*", r"bar.{{partition()}}"),
      ("foo.*", r"bar.{{random()}}"),
      ("foo.*", r"bar.{{wildcard(foo)}}"),
      ("foo.*", r"bar.{{wildcard()}}"),
      ("foo.*", r"bar.{{wildcard(1,2)}}"),
      ("foo", r"bar.{{wildcard(1)}}"),
      ("foo.*", r"bar.{{partition(2147483648)}}"),
      ("foo.*", r"bar.{{random(2147483648)}}"),
      ("foo.*", r"bar.{{split(1,a b)}}"),
      ("foo.*", r"bar.{{left(1,2147483648)}}"),
      ("foo.*", r"bar.{{WILDCARD(1)}}"),
      ("foo.*", "bar.{{wildcard(\t1\t)}}"),
      ("foo.*", r"bar.$ 1"),
    ];
    for (final (source, destination) in cases) {
      expect(
        () =>
            JetStreamSubjectTransform(source: source, destination: destination),
        throwsArgumentError,
        reason: "$source to $destination",
      );
    }
  });

  test("republish uses the complete subject transform grammar", () {
    expect(
      () => JetStreamRepublish(source: "foo.*", destination: r"archive.$1"),
      returnsNormally,
    );
    expect(() => JetStreamRepublish(destination: "archive.>"), returnsNormally);
    expect(
      () => JetStreamRepublish(destination: "archive.fixed"),
      throwsArgumentError,
    );
    expect(
      () => JetStreamRepublish(source: "foo", destination: ""),
      returnsNormally,
    );
    expect(
      () => JetStreamRepublish(source: "foo.*", destination: "archive.*"),
      throwsArgumentError,
    );
  });

  test(
    "user stream metadata reserves _nats keys but responses preserve them",
    () {
      expect(
        JetStreamStreamConfig(
          name: "EVENTS",
          metadata: const {"설명": "값"},
        ).metadata,
        {"설명": "값"},
      );
      for (final key in ["_nats.owner", "_nats.한"]) {
        expect(
          () => JetStreamStreamConfig(name: "EVENTS", metadata: {key: "value"}),
          throwsArgumentError,
        );
      }
      final response = JetStreamStreamConfig.fromJson({
        "name": "EVENTS",
        "metadata": {"_nats.level": "4"},
        "subject_transform": {"src": "server.future", "dest": "{{future()}}"},
      });
      expect(response.metadata, {"_nats.level": "4"});
      expect(response.raw["subject_transform"], {
        "src": "server.future",
        "dest": "{{future()}}",
      });
    },
  );

  test("copyWith preserves unknown fields from newer servers", () {
    final config = JetStreamStreamConfig.fromJson({
      "name": "EVENTS",
      "subjects": ["events.>"],
      "storage": "file",
      "retention": "limits",
      "discard": "old",
      "compression": "none",
      "future_feature": {
        "enabled": true,
        "limit": BigInt.from(9007199254740993),
      },
    });

    final updated = config.copyWith(description: "updated");

    expect(updated.description, "updated");

    final cleared = updated.copyWith(clearDescription: true);
    expect(cleared.description, isNull);
    expect(cleared.raw.containsKey("description"), isFalse);
    expect(cleared.raw["future_feature"], config.raw["future_feature"]);
    expect(
      () => config.copyWith(description: "set", clearDescription: true),
      throwsArgumentError,
    );
    expect(updated.toJson()["future_feature"], {
      "enabled": true,
      "limit": BigInt.from(9007199254740993),
    });
  });

  test("copyWith promotes a mirror without erasing unknown fields", () {
    final config = JetStreamStreamConfig.fromJson({
      "name": "MIRROR",
      "mirror": {"name": "ORIGIN"},
      "mirror_direct": true,
      "future_feature": {
        "enabled": true,
        "limit": BigInt.from(9007199254740993),
      },
    });

    expect(
      () => config.copyWith(subjects: ["promoted.>"]),
      throwsArgumentError,
    );

    final promoted = config.copyWith(
      subjects: ["promoted.>"],
      clearMirror: true,
    );

    expect(promoted.raw.containsKey("mirror"), isFalse);
    expect(promoted.raw["mirror_direct"], isFalse);
    expect(promoted.subjects, ["promoted.>"]);
    expect(promoted.raw["future_feature"], config.raw["future_feature"]);
  });

  test("unknown stream enum values remain available through exact JSON", () {
    final config = JetStreamStreamConfig.fromJson({
      "name": "EVENTS",
      "storage": "future_storage",
      "retention": "future_retention",
      "discard": "future_discard",
      "compression": "future_compression",
      "persist_mode": "future_persist",
    });

    expect(config.storageWireValue, "future_storage");
    expect(config.knownStorage, isNull);
    expect(config.raw["retention"], "future_retention");
    expect(() => config.storage, throwsArgumentError);
    expect(
      () => JetStreamStreamConfig.fromJson({
        "name": "EVENTS",
        "subjects": "events.>",
      }),
      throwsArgumentError,
    );
    expect(
      () => JetStreamStreamConfig.fromJson({
        "name": "EVENTS",
        "sources": "ORIGIN",
      }),
      throwsArgumentError,
    );
  });

  test("stream wire integers accept exact maxima and reject overflow", () {
    final int64 = (BigInt.one << 63) - BigInt.one;
    final uint64 = (BigInt.one << 64) - BigInt.one;
    final int32 = (BigInt.one << 31) - BigInt.one;
    expect(
      () => JetStreamStreamConfig.fromJson({
        "name": "EVENTS",
        "max_consumers": int64,
        "max_msgs": int64,
        "max_bytes": int64,
        "max_msgs_per_subject": int64,
        "max_msg_size": int32,
        "max_age": int64,
        "duplicate_window": int64,
        "first_seq": uint64,
      }),
      returnsNormally,
    );

    final cases = <(String, BigInt)>[
      ("max_consumers", int64 + BigInt.one),
      ("max_msgs", int64 + BigInt.one),
      ("max_bytes", int64 + BigInt.one),
      ("max_msgs_per_subject", int64 + BigInt.one),
      ("max_msg_size", int32 + BigInt.one),
      ("max_age", int64 + BigInt.one),
      ("duplicate_window", int64 + BigInt.one),
      ("first_seq", uint64 + BigInt.one),
    ];
    for (final (field, value) in cases) {
      expect(
        () => JetStreamStreamConfig.fromJson({"name": "EVENTS", field: value}),
        throwsArgumentError,
        reason: field,
      );
    }
    expect(
      () => JetStreamStreamConfig.fromJson({
        "name": "EVENTS",
        "mirror": {"name": "ORIGIN", "opt_start_seq": uint64 + BigInt.one},
      }),
      throwsArgumentError,
    );
  });
}
