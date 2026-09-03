@TestOn("vm")
library;

import "dart:async";
import "dart:convert";
import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  final serverUrl = Platform.environment["NATS_URL"];
  final skipReason = serverUrl == null ? "NATS_URL is not configured" : false;

  test(
    "Object Store CRUD and streaming upload preserve exact bytes",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final bucketName = "O$suffix";
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(serverUrl!)]),
      );
      final jetStream = JetStreamContext(connection);
      var bucketExists = false;
      addTearDown(() async {
        if (bucketExists) {
          try {
            await jetStream.objectStore.deleteStore(bucketName);
          } on JetStreamApiException {
            // Cleanup follows the test owned bucket state.
          }
        }
        await connection.close();
      });
      final bucket = await jetStream.objectStore.createStore(
        JetStreamObjectStoreConfig(
          name: bucketName,
          description: "Object Store integration",
          storage: JetStreamStorage.memory,
          metadata: const {"owner": "nats.dart"},
          compression: true,
          experimentalFastIngest: true,
        ),
      );
      bucketExists = true;
      expect(await bucket.getInfo("missing"), isNull);
      expect(await bucket.getBytes("missing"), isNull);
      expect(
        () => bucket.put(
          JetStreamObjectMetadata(name: "invalid-negative"),
          const [-1],
        ),
        throwsFormatException,
      );
      await expectLater(
        bucket.putStream(
          JetStreamObjectMetadata(name: "invalid-large"),
          Stream.value(const [256]),
        ),
        throwsA(
          isA<JetStreamObjectPutException>().having(
            (error) => error.cause,
            "cause",
            isA<FormatException>(),
          ),
        ),
      );
      expect(
        (await jetStream.streamInfo("OBJ_$bucketName")).messages,
        BigInt.zero,
      );

      final objectName = "paths/한글/🙂.bin";
      final original = <int>[0, 255, 13, 10, 1, 2, 3, 4, 5, 6];
      final first = await bucket.put(
        JetStreamObjectMetadata(
          name: objectName,
          description: "binary object",
          headers: NatsHeaders(
            entries: const [
              MapEntry("Content-Type", "application/octet-stream"),
              MapEntry("X-Value", "one"),
              MapEntry("X-Value", "two"),
            ],
          ),
          metadata: const {"kind": "fixture"},
          chunkSize: 3,
        ),
        original,
      );
      expect(first.name, objectName);
      expect(first.size, BigInt.from(original.length));
      expect(first.chunks, BigInt.from(4));
      expect(first.digest, startsWith("SHA-256="));
      expect(first.headers?.values("X-Value"), ["one", "two"]);
      expect(first.metadata, {"kind": "fixture"});
      expect(await bucket.getBytes(objectName), original);
      await expectLater(
        bucket.getBytes(objectName, maximumBytes: original.length - 1),
        throwsA(isA<JetStreamObjectIntegrityException>()),
      );

      final fastBytes = List<int>.generate(4097, (index) => index & 255);
      final fast = await bucket.put(
        JetStreamObjectMetadata(name: "fast", chunkSize: 1024),
        fastBytes,
        mode: JetStreamObjectPutMode.experimentalFastIngest,
      );
      expect(fast.size, BigInt.from(fastBytes.length));
      expect(fast.chunks, BigInt.from(5));
      expect(fast.digest, startsWith("SHA-256="));
      expect(await bucket.getBytes("fast"), fastBytes);
      final fastReplacement = await bucket.putBytes("fast", const [9, 8, 7]);
      expect(fastReplacement.identifier, isNot(fast.identifier));
      expect(await bucket.getBytes("fast"), [9, 8, 7]);

      final replacement = utf8.encode("replacement");
      final second = await bucket.putBytes(objectName, replacement);
      expect(second.identifier, isNot(first.identifier));
      expect(await bucket.getBytes(objectName), replacement);
      expect(
        (await jetStream.streamInfo("OBJ_$bucketName")).messages,
        BigInt.from(4),
      );

      final empty = await bucket.putBytes("a", const []);
      expect(empty.size, BigInt.zero);
      expect(empty.chunks, BigInt.zero);
      expect(
        empty.digest,
        "SHA-256=47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU=",
      );
      expect(await bucket.getBytes("a"), isEmpty);

      final streamedBytes = <int>[0, 1, 2, 3, 4, 5, 255];
      final streamed = await bucket.putStream(
        JetStreamObjectMetadata(name: "streamed", chunkSize: 4),
        Stream.fromIterable([
          const [0, 1],
          const [],
          const [2, 3, 4, 5, 255],
        ]),
      );
      expect(streamed.size, BigInt.from(streamedBytes.length));
      expect(streamed.chunks, BigInt.two);
      expect(await bucket.getBytes("streamed"), streamedBytes);
      expect(
        await bucket.getBytes("streamed", maximumChunks: 2),
        streamedBytes,
      );
      await expectLater(
        bucket.getBytes("streamed", maximumChunks: 1),
        throwsA(isA<JetStreamObjectIntegrityException>()),
      );
      final download = await bucket.get("streamed");
      expect(download?.info.identifier, streamed.identifier);
      final downloadedChunks = await download!.chunks.toList();
      expect(downloadedChunks.map((chunk) => chunk.length), [4, 3]);
      expect(downloadedChunks.expand((chunk) => chunk), streamedBytes);
      expect(() => downloadedChunks.first[0] = 9, throwsUnsupportedError);
      await download.verified;
      await expectLater(
        bucket.get("streamed", maximumChunks: 1),
        throwsA(isA<JetStreamObjectIntegrityException>()),
      );

      await bucket.put(
        JetStreamObjectMetadata(name: "streamed-text", chunkSize: 2),
        utf8.encode("text"),
      );
      expect(await bucket.getString("streamed-text", maximumChunks: 2), "text");
      await expectLater(
        bucket.getString("streamed-text", maximumChunks: 1),
        throwsA(isA<JetStreamObjectIntegrityException>()),
      );

      final cancelledDownload = await bucket.get("streamed");
      final cancelled = expectLater(
        cancelledDownload!.verified,
        throwsA(isA<JetStreamObjectDownloadCancelledException>()),
      );
      final cancelledChunks = StreamIterator(cancelledDownload.chunks);
      expect(await cancelledChunks.moveNext(), isTrue);
      await cancelledChunks.cancel();
      await cancelled;

      expect((await jetStream.objectStore.store(bucketName)).name, bucketName);
      await bucket.delete(objectName);
      expect(await bucket.getInfo(objectName), isNull);
      final deleted = await bucket.getInfo(objectName, showDeleted: true);
      expect(deleted?.deleted, isTrue);
      expect(deleted?.size, BigInt.zero);
      expect(await bucket.getBytes(objectName), isNull);
      await bucket.delete(objectName);
      final deletedAgain = await bucket.getInfo(objectName, showDeleted: true);
      expect(deletedAgain?.deleted, isTrue);
      expect(deletedAgain!.revision, greaterThan(deleted!.revision));
      await expectLater(
        bucket.delete("missing-delete"),
        throwsA(isA<JetStreamObjectNotFoundException>()),
      );

      await jetStream.objectStore.deleteStore(bucketName);
      bucketExists = false;
      await expectLater(
        jetStream.objectStore.store(bucketName),
        throwsA(isA<JetStreamApiException>()),
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}
