import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  test("bucket configuration validates identity and snapshots metadata", () {
    final metadata = {"owner": "infra"};
    final tags = ["ssd"];
    final config = JetStreamObjectStoreConfig(
      name: "OBJECTS_1",
      metadata: metadata,
      compression: true,
      placement: JetStreamPlacement(cluster: "C", tags: tags),
    );
    metadata["owner"] = "changed";
    tags.add("changed");

    expect(config.metadata, {"owner": "infra"});
    expect(() => config.metadata["new"] = "value", throwsUnsupportedError);
    expect(config.compression, isTrue);
    expect(config.placement?.cluster, "C");
    expect(config.placement?.tags, ["ssd"]);
    expect(
      () => JetStreamObjectStoreConfig(name: "bad.bucket"),
      throwsArgumentError,
    );
    expect(
      () => JetStreamObjectStoreConfig(
        name: "VALID",
        metadata: const {"_nats.level": "4"},
      ),
      throwsArgumentError,
    );
  });

  test("object metadata accepts encoded names and validates local bounds", () {
    final metadata = {"content-type": "binary"};
    final value = JetStreamObjectMetadata(
      name: "paths/한글/🙂.bin",
      headers: NatsHeaders(
        entries: const [MapEntry("Content-Type", "application/octet-stream")],
      ),
      metadata: metadata,
      chunkSize: 1,
    );
    metadata["content-type"] = "changed";

    expect(value.name, "paths/한글/🙂.bin");
    expect(value.metadata, {"content-type": "binary"});
    expect(value.chunkSize, 1);
    expect(
      () => JetStreamObjectMetadata(name: "", chunkSize: 1),
      throwsArgumentError,
    );
    expect(
      () => JetStreamObjectMetadata(name: "valid", chunkSize: 0),
      throwsArgumentError,
    );
    expect(
      () => JetStreamObjectMetadata(name: "\uD800", chunkSize: 1),
      throwsArgumentError,
    );
    expect(
      () => JetStreamObjectMetadata(name: "\uDC00", chunkSize: 1),
      throwsArgumentError,
    );
    expect(
      JetStreamObjectMetadata(name: "한" * 768, chunkSize: 1).name,
      "한" * 768,
    );
    expect(
      () => JetStreamObjectMetadata(name: "한" * 769, chunkSize: 1),
      throwsArgumentError,
    );
  });

  test("object textual metadata requires exact UTF 8", () {
    for (final create in <Object Function()>[
      () => JetStreamObjectStoreConfig(name: "B", description: "\uD800"),
      () => JetStreamObjectMetadata(name: "o", description: "\uD800"),
      () => JetStreamObjectMetadata(name: "o", metadata: {"\uD800": "v"}),
      () => JetStreamObjectMetadata(name: "o", metadata: {"k": "\uDC00"}),
      () => JetStreamObjectMetadataUpdate(name: "o", description: "\uDC00"),
    ]) {
      expect(create, throwsArgumentError);
    }
  });

  test("object information snapshots application metadata", () {
    final metadata = {"owner": "infra"};
    final info = JetStreamObjectInfo(
      name: "object",
      bucket: "B",
      identifier: "identifier",
      revision: BigInt.one,
      size: BigInt.one,
      chunks: BigInt.one,
      digest: "SHA-256=value=",
      modified: "2026-09-01T00:00:00Z",
      deleted: false,
      chunkSize: 1,
      metadata: metadata,
    );
    metadata["owner"] = "changed";

    expect(info.metadata, {"owner": "infra"});
    expect(info.metadata.clear, throwsUnsupportedError);
  });

  test("metadata updates validate names and snapshot replacement values", () {
    final metadata = {"owner": "infra"};
    final update = JetStreamObjectMetadataUpdate(
      name: "paths/renamed.bin",
      description: "replacement",
      metadata: metadata,
    );
    metadata["owner"] = "changed";

    expect(update.name, "paths/renamed.bin");
    expect(update.description, "replacement");
    expect(update.metadata, {"owner": "infra"});
    expect(update.metadata.clear, throwsUnsupportedError);
    expect(() => JetStreamObjectMetadataUpdate(name: ""), throwsArgumentError);
    expect(
      () =>
          JetStreamObjectMetadataUpdate(name: "valid", description: "x" * 4097),
      throwsArgumentError,
    );
  });

  test("object links validate bucket and optional target identity", () {
    final objectLink = JetStreamObjectLink(bucket: "TARGET", name: "path/한글");
    final bucketLink = JetStreamObjectLink(bucket: "TARGET");

    expect(objectLink.bucket, "TARGET");
    expect(objectLink.name, "path/한글");
    expect(bucketLink.name, isNull);
    expect(
      () => JetStreamObjectLink(bucket: "bad.bucket"),
      throwsArgumentError,
    );
    expect(
      () => JetStreamObjectLink(bucket: "TARGET", name: ""),
      throwsArgumentError,
    );
  });

  test("Object Store status snapshots bucket metadata", () {
    final metadata = {"owner": "infra"};
    final status = JetStreamObjectStoreStatus(
      name: "BUCKET",
      description: "objects",
      maximumAge: JetStreamNanoseconds(BigInt.zero),
      maximumBytes: BigInt.from(-1),
      storage: JetStreamStorage.file,
      replicas: 1,
      sealed: false,
      storageBytes: BigInt.zero,
      streamMessages: BigInt.zero,
      metadata: metadata,
      compression: true,
    );
    metadata["owner"] = "changed";

    expect(status.metadata, {"owner": "infra"});
    expect(status.metadata.clear, throwsUnsupportedError);
    expect(status.maximumBytes, BigInt.from(-1));
  });

  test(
    "Object Store wire integers accept exact maxima and reject overflow",
    () {
      final int64 = (BigInt.one << 63) - BigInt.one;
      final uint64 = (BigInt.one << 64) - BigInt.one;
      expect(
        () => JetStreamObjectStoreConfig(
          name: "OBJECTS",
          maximumAge: JetStreamNanoseconds(int64),
          maximumBytes: int64,
        ),
        returnsNormally,
      );
      expect(() => JetStreamObjectWatchStart.revision(uint64), returnsNormally);
      for (final create in <Object Function()>[
        () => JetStreamObjectStoreConfig(
          name: "OBJECTS",
          maximumAge: JetStreamNanoseconds(int64 + BigInt.one),
        ),
        () => JetStreamObjectStoreConfig(
          name: "OBJECTS",
          maximumBytes: int64 + BigInt.one,
        ),
        () => JetStreamObjectWatchStart.revision(uint64 + BigInt.one),
      ]) {
        expect(create, throwsArgumentError);
      }
    },
  );
}
