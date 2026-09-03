@TestOn("vm")
library;

import "dart:convert";
import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  final serverUrl = Platform.environment["NATS_URL"];
  final skipReason = serverUrl == null ? "NATS_URL is not configured" : false;

  test(
    "Object Store metadata and links preserve identity and targets",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final bucketName = "L$suffix";
      final targetBucketName = "T$suffix";
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(serverUrl!)]),
      );
      final jetStream = JetStreamContext(connection);
      final otherConnection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(serverUrl)]),
      );
      final otherJetStream = JetStreamContext(otherConnection);
      final createdBuckets = <String>[];
      addTearDown(() async {
        for (final name in createdBuckets.reversed) {
          try {
            await jetStream.objectStore.deleteStore(name);
          } on JetStreamApiException {
            continue;
          }
        }
        await otherConnection.close();
        await connection.close();
      });
      final bucket = await jetStream.objectStore.createStore(
        JetStreamObjectStoreConfig(
          name: bucketName,
          storage: JetStreamStorage.memory,
        ),
      );
      createdBuckets.add(bucketName);
      final streamedBytes = <int>[0, 1, 2, 3, 4, 5, 255];
      final streamed = await bucket.putBytes("streamed", streamedBytes);
      const paddedName = "a";
      await bucket.putBytes(paddedName, const []);

      final metadataUpdated = await bucket.updateMetadata(
        "streamed",
        JetStreamObjectMetadataUpdate(
          name: "streamed",
          description: "updated description",
          headers: NatsHeaders(
            entries: const [
              MapEntry("Content-Type", "application/custom"),
              MapEntry("X-Update", "one"),
              MapEntry("X-Update", "two"),
            ],
          ),
          metadata: const {"stage": "updated"},
        ),
      );
      expect(metadataUpdated.name, "streamed");
      expect(metadataUpdated.description, "updated description");
      expect(
        metadataUpdated.headers?.first("Content-Type"),
        "application/custom",
      );
      expect(metadataUpdated.headers?.values("X-Update"), ["one", "two"]);
      expect(metadataUpdated.metadata, {"stage": "updated"});
      expect(metadataUpdated.identifier, streamed.identifier);
      expect(metadataUpdated.size, streamed.size);
      expect(metadataUpdated.chunks, streamed.chunks);
      expect(metadataUpdated.digest, streamed.digest);
      expect(metadataUpdated.chunkSize, streamed.chunkSize);
      expect(await bucket.getBytes("streamed"), streamedBytes);

      final renamed = await bucket.updateMetadata(
        "streamed",
        JetStreamObjectMetadataUpdate(
          name: "renamed-streamed",
          metadata: const {"stage": "renamed"},
        ),
      );
      expect(renamed.name, "renamed-streamed");
      expect(renamed.description, isNull);
      expect(renamed.headers, isNull);
      expect(renamed.identifier, streamed.identifier);
      expect(renamed.metadata, {"stage": "renamed"});
      expect(await bucket.getInfo("streamed"), isNull);
      expect(await bucket.getBytes("renamed-streamed"), streamedBytes);

      await bucket.putBytes("rename-collision", const [1]);
      await expectLater(
        bucket.updateMetadata(
          "renamed-streamed",
          JetStreamObjectMetadataUpdate(name: "rename-collision"),
        ),
        throwsA(isA<JetStreamObjectAlreadyExistsException>()),
      );
      expect(await bucket.getBytes("renamed-streamed"), streamedBytes);
      await bucket.putBytes("deleted-destination", const [2]);
      await bucket.delete("deleted-destination");
      final reusedDestination = await bucket.updateMetadata(
        "renamed-streamed",
        JetStreamObjectMetadataUpdate(name: "deleted-destination"),
      );
      expect(reusedDestination.identifier, streamed.identifier);
      expect(await bucket.getInfo("renamed-streamed"), isNull);
      expect(await bucket.getBytes("deleted-destination"), streamedBytes);
      await expectLater(
        bucket.updateMetadata(
          "missing-metadata",
          JetStreamObjectMetadataUpdate(name: "still-missing"),
        ),
        throwsA(isA<JetStreamObjectNotFoundException>()),
      );
      await bucket.putBytes("deleted-metadata-source", const [4]);
      await bucket.delete("deleted-metadata-source");
      await expectLater(
        bucket.updateMetadata(
          "deleted-metadata-source",
          JetStreamObjectMetadataUpdate(name: "revived-metadata"),
        ),
        throwsA(isA<JetStreamObjectNotFoundException>()),
      );

      final objectLink = await bucket.addObjectLink(
        "object-link",
        targetStore: bucket,
        targetName: "deleted-destination",
      );
      expect(objectLink.size, BigInt.zero);
      expect(objectLink.chunks, BigInt.zero);
      expect(objectLink.digest, isEmpty);
      expect(objectLink.link?.bucket, bucketName);
      expect(objectLink.link?.name, "deleted-destination");
      expect(await bucket.getBytes("object-link"), streamedBytes);
      final linkedDownload = await bucket.get("object-link");
      expect(
        await linkedDownload?.chunks.expand((chunk) => chunk).toList(),
        streamedBytes,
      );
      await linkedDownload?.verified;
      await expectLater(
        bucket.addObjectLink(
          "nested-link",
          targetStore: bucket,
          targetName: "object-link",
        ),
        throwsA(
          isA<JetStreamObjectLinkException>().having(
            (error) => error.failure,
            "failure",
            JetStreamObjectLinkFailure.targetIsLink,
          ),
        ),
      );
      await expectLater(
        bucket.addObjectLink(
          "rename-collision",
          targetStore: bucket,
          targetName: "deleted-destination",
        ),
        throwsA(isA<JetStreamObjectAlreadyExistsException>()),
      );
      await bucket.putBytes("deleted-target", const [3]);
      await bucket.delete("deleted-target");
      await expectLater(
        bucket.addObjectLink(
          "deleted-target-link",
          targetStore: bucket,
          targetName: "deleted-target",
        ),
        throwsA(isA<JetStreamObjectNotFoundException>()),
      );
      await expectLater(
        bucket.addObjectLink(
          "deleted-target",
          targetStore: bucket,
          targetName: paddedName,
        ),
        throwsA(isA<JetStreamObjectAlreadyExistsException>()),
      );

      final replacedLink = await bucket.addObjectLink(
        "object-link",
        targetStore: bucket,
        targetName: paddedName,
      );
      expect(replacedLink.identifier, isNot(objectLink.identifier));
      expect(replacedLink.link?.name, paddedName);
      final updatedLink = await bucket.updateMetadata(
        "object-link",
        JetStreamObjectMetadataUpdate(
          name: "object-link",
          description: "preserved link",
        ),
      );
      expect(updatedLink.identifier, replacedLink.identifier);
      expect(updatedLink.link?.bucket, replacedLink.link?.bucket);
      expect(updatedLink.link?.name, replacedLink.link?.name);
      expect(updatedLink.description, "preserved link");
      await bucket.delete("object-link");
      final revivedLink = await bucket.addObjectLink(
        "object-link",
        targetStore: bucket,
        targetName: paddedName,
      );
      expect(revivedLink.identifier, isNot(updatedLink.identifier));
      expect(await bucket.getBytes("object-link"), isEmpty);

      final targetBucket = await jetStream.objectStore.createStore(
        JetStreamObjectStoreConfig(
          name: targetBucketName,
          storage: JetStreamStorage.memory,
        ),
      );
      createdBuckets.add(targetBucketName);
      final otherTargetBucket = await otherJetStream.objectStore.store(
        targetBucketName,
      );
      expect(
        () => bucket.addObjectLink(
          "cross-context-object",
          targetStore: otherTargetBucket,
          targetName: paddedName,
        ),
        throwsA(
          isA<JetStreamObjectLinkException>().having(
            (error) => error.failure,
            "failure",
            JetStreamObjectLinkFailure.contextMismatch,
          ),
        ),
      );
      expect(
        () => bucket.addBucketLink("cross-context-bucket", otherTargetBucket),
        throwsA(
          isA<JetStreamObjectLinkException>().having(
            (error) => error.failure,
            "failure",
            JetStreamObjectLinkFailure.contextMismatch,
          ),
        ),
      );
      final otherPrefixTarget = JetStreamObjectStore(
        connection,
        "${targetBucket.apiPrefix}.OTHER",
        targetBucket.timeout,
        targetBucket.name,
      );
      expect(
        () => bucket.addObjectLink(
          "cross-prefix-object",
          targetStore: otherPrefixTarget,
          targetName: paddedName,
        ),
        throwsA(
          isA<JetStreamObjectLinkException>().having(
            (error) => error.failure,
            "failure",
            JetStreamObjectLinkFailure.contextMismatch,
          ),
        ),
      );
      expect(await bucket.getInfo("cross-context-object"), isNull);
      expect(await bucket.getInfo("cross-context-bucket"), isNull);
      expect(await bucket.getInfo("cross-prefix-object"), isNull);
      final bucketLink = await bucket.addBucketLink(
        "bucket-link",
        targetBucket,
      );
      expect(bucketLink.link?.bucket, targetBucketName);
      expect(bucketLink.link?.name, isNull);
      expect(bucketLink.size, BigInt.zero);
      expect(bucketLink.chunks, BigInt.zero);
      expect(bucketLink.digest, isEmpty);
      await expectLater(
        bucket.getBytes("bucket-link"),
        throwsA(
          isA<JetStreamObjectLinkException>().having(
            (error) => error.failure,
            "failure",
            JetStreamObjectLinkFailure.bucketLink,
          ),
        ),
      );
      final crossBucketBytes = utf8.encode("cross bucket");
      await targetBucket.putBytes("target", crossBucketBytes);
      final crossBucketLink = await bucket.addObjectLink(
        "object-link",
        targetStore: targetBucket,
        targetName: "target",
      );
      expect(crossBucketLink.link?.bucket, targetBucketName);
      expect(await bucket.getBytes("object-link"), crossBucketBytes);

      Future<void> publishRawLink(String linkName, String targetName) async {
        await jetStream.publish(
          "\$O.$bucketName.M.${base64Url.encode(utf8.encode(linkName))}",
          utf8.encode(
            jsonEncode({
              "name": linkName,
              "bucket": bucketName,
              "nuid": "RAWLINKIDENTIFIER00001",
              "size": 0,
              "chunks": 0,
              "options": {
                "link": {"bucket": bucketName, "name": targetName},
                "max_chunk_size": jetStreamObjectDefaultChunkSize,
              },
            }),
          ),
          headers: NatsHeaders(entries: const [MapEntry("Nats-Rollup", "sub")]),
        );
      }

      await publishRawLink("cycle-a", "cycle-b");
      await publishRawLink("cycle-b", "cycle-a");
      await expectLater(
        bucket.getBytes("cycle-a"),
        throwsA(
          isA<JetStreamObjectLinkException>().having(
            (error) => error.failure,
            "failure",
            JetStreamObjectLinkFailure.cycle,
          ),
        ),
      );
      await publishRawLink("depth-a", "depth-b");
      await publishRawLink("depth-b", paddedName);
      await expectLater(
        bucket.getBytes("depth-a", maximumLinkDepth: 1),
        throwsA(
          isA<JetStreamObjectLinkException>().having(
            (error) => error.failure,
            "failure",
            JetStreamObjectLinkFailure.maximumDepthExceeded,
          ),
        ),
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}
