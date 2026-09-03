@TestOn("vm")
library;

import "dart:convert";
import "dart:io";

import "package:crypto/crypto.dart";
import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  final serverUrl = Platform.environment["NATS_URL"];
  final skipReason = serverUrl == null ? "NATS_URL is not configured" : false;

  test(
    "Object Store rejects forged metadata and ambiguous concurrent writes",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final bucketName = "I$suffix";
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
          storage: JetStreamStorage.memory,
        ),
      );
      bucketExists = true;

      await bucket.putBytes("wildcard-victim", const [7, 8, 9]);
      for (final wildcard in const ["*", ">"]) {
        final forgedName = wildcard == "*" ? "forged-star" : "forged-tail";
        await jetStream.publish(
          "\$O.$bucketName.M.${base64Url.encode(utf8.encode(forgedName))}",
          utf8.encode(
            jsonEncode({
              "name": forgedName,
              "bucket": bucketName,
              "nuid": wildcard,
              "size": 0,
              "chunks": 0,
              "digest": "SHA-256=47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU=",
            }),
          ),
          headers: NatsHeaders(entries: const [MapEntry("Nats-Rollup", "sub")]),
        );
        await expectLater(
          bucket.delete(forgedName),
          throwsA(isA<JetStreamProtocolException>()),
        );
        expect(await bucket.getBytes("wildcard-victim"), [7, 8, 9]);
      }

      final beforeFailedStream = (await jetStream.streamInfo(
        "OBJ_$bucketName",
      )).messages;
      await expectLater(
        bucket.putStream(
          JetStreamObjectMetadata(name: "failed-stream", chunkSize: 3),
          Stream.fromIterable([
            const [1, 2, 3],
            const [4, -1],
          ]),
        ),
        throwsA(
          isA<JetStreamObjectPutException>().having(
            (error) => error.metadataCommitAmbiguous,
            "metadataCommitAmbiguous",
            isFalse,
          ),
        ),
      );
      expect(await bucket.getInfo("failed-stream"), isNull);
      expect(
        (await jetStream.streamInfo("OBJ_$bucketName")).messages,
        beforeFailedStream,
      );

      final malformedName = "malformed-digest";
      await jetStream.publish(
        "\$O.$bucketName.M.${base64Url.encode(utf8.encode(malformedName))}",
        utf8.encode(
          jsonEncode({
            "name": malformedName,
            "bucket": bucketName,
            "nuid": "MALFORMEDDIGESTVALUE12",
            "size": 0,
            "chunks": 0,
            "digest": "MD5=invalid",
          }),
        ),
        headers: NatsHeaders(entries: const [MapEntry("Nats-Rollup", "sub")]),
      );
      await expectLater(
        bucket.getInfo(malformedName),
        throwsA(isA<JetStreamProtocolException>()),
      );

      final wrongEmptyName = "wrong-empty-digest";
      await jetStream.publish(
        "\$O.$bucketName.M.${base64Url.encode(utf8.encode(wrongEmptyName))}",
        utf8.encode(
          jsonEncode({
            "name": wrongEmptyName,
            "bucket": bucketName,
            "nuid": "WRONGEMPTYDIGESTVAL123",
            "size": 0,
            "chunks": 0,
            "digest": "SHA-256=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
          }),
        ),
        headers: NatsHeaders(entries: const [MapEntry("Nats-Rollup", "sub")]),
      );
      await expectLater(
        bucket.getBytes(wrongEmptyName),
        throwsA(isA<JetStreamObjectIntegrityException>()),
      );

      final messagesBeforeRace = (await jetStream.streamInfo(
        "OBJ_$bucketName",
      )).messages;
      Future<(List<int>, Object)> race(List<int> payload) async {
        try {
          return (
            payload,
            await bucket.put(
              JetStreamObjectMetadata(name: "concurrent", chunkSize: 1),
              payload,
            ),
          );
        } on Object catch (error) {
          return (payload, error);
        }
      }

      final raceResults = await Future.wait([
        race(List<int>.filled(16, 1)),
        race(List<int>.filled(16, 2)),
      ]);
      final winners = raceResults
          .where((result) => result.$2 is JetStreamObjectInfo)
          .toList();
      final losers = raceResults
          .where((result) => result.$2 is JetStreamObjectPutException)
          .toList();
      expect(winners, hasLength(1));
      expect(losers, hasLength(1));
      expect(
        (losers.single.$2 as JetStreamObjectPutException)
            .metadataCommitAmbiguous,
        isFalse,
      );
      expect(await bucket.getBytes("concurrent"), winners.single.$1);
      expect(
        (await jetStream.streamInfo("OBJ_$bucketName")).messages,
        messagesBeforeRace + BigInt.from(17),
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "stale Object Store handles cannot mutate replacement streams",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final bucketName = "S$suffix";
      final captureName = "CAPTURE_$suffix";
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(serverUrl!)]),
      );
      final jetStream = JetStreamContext(connection);
      var captureExists = false;
      addTearDown(() async {
        if (captureExists) {
          try {
            await jetStream.deleteStream(captureName);
          } on JetStreamApiException {
            // Cleanup follows the test owned stream state.
          }
        }
        await connection.close();
      });
      final stale = await jetStream.objectStore.createStore(
        JetStreamObjectStoreConfig(
          name: bucketName,
          storage: JetStreamStorage.memory,
        ),
      );
      await jetStream.objectStore.deleteStore(bucketName);
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: captureName,
          subjects: ["\$O.$bucketName.C.>", "\$O.$bucketName.M.>"],
          storage: JetStreamStorage.memory,
        ),
      );
      captureExists = true;
      await expectLater(
        stale.putBytes("must-not-commit", const [1, 2, 3]),
        throwsA(
          anyOf(
            isA<JetStreamApiException>(),
            isA<JetStreamObjectPutException>().having(
              (error) => error.cause,
              "cause",
              isA<JetStreamApiException>(),
            ),
          ),
        ),
      );
      expect((await jetStream.streamInfo(captureName)).messages, BigInt.zero);
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 15)),
  );

  test(
    "cross bucket links reject an ordinary stream before reading bytes",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final bucketName = "L$suffix";
      final fakeBucket = "F$suffix";
      final fakeStream = "OBJ_$fakeBucket";
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(serverUrl!)]),
      );
      final jetStream = JetStreamContext(connection);
      var bucketExists = false;
      var fakeExists = false;
      addTearDown(() async {
        if (bucketExists) {
          try {
            await jetStream.objectStore.deleteStore(bucketName);
          } on JetStreamApiException {
            // Cleanup follows the test owned bucket state.
          }
        }
        if (fakeExists) {
          try {
            await jetStream.deleteStream(fakeStream);
          } on JetStreamApiException {
            // Cleanup follows the test owned stream state.
          }
        }
        await connection.close();
      });
      final bucket = await jetStream.objectStore.createStore(
        JetStreamObjectStoreConfig(
          name: bucketName,
          storage: JetStreamStorage.memory,
        ),
      );
      bucketExists = true;
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: fakeStream,
          subjects: ["\$O.$fakeBucket.>"],
          storage: JetStreamStorage.memory,
        ),
      );
      fakeExists = true;
      const identifier = "BBBBBBBBBBBBBBBBBBBBBB";
      const targetName = "target";
      final targetMetadataSubject =
          "\$O.$fakeBucket.M.${base64Url.encode(utf8.encode(targetName))}";
      await jetStream.publish("\$O.$fakeBucket.C.$identifier", const [7]);
      await jetStream.publish(
        targetMetadataSubject,
        utf8.encode(
          jsonEncode({
            "name": targetName,
            "bucket": fakeBucket,
            "nuid": identifier,
            "size": 1,
            "chunks": 1,
            "digest":
                "SHA-256=${base64Url.encode(sha256.convert(const [7]).bytes)}",
          }),
        ),
      );
      const linkName = "forged-link";
      await jetStream.publish(
        "\$O.$bucketName.M.${base64Url.encode(utf8.encode(linkName))}",
        utf8.encode(
          jsonEncode({
            "name": linkName,
            "bucket": bucketName,
            "nuid": "CCCCCCCCCCCCCCCCCCCCCC",
            "size": 0,
            "chunks": 0,
            "options": {
              "link": {"bucket": fakeBucket, "name": targetName},
            },
          }),
        ),
        headers: NatsHeaders(entries: const [MapEntry("Nats-Rollup", "sub")]),
      );

      await expectLater(
        bucket.getBytes(linkName),
        throwsA(
          isA<JetStreamProtocolException>()
              .having(
                (error) => error.subject,
                "subject",
                "\$JS.API.STREAM.INFO.$fakeStream",
              )
              .having((error) => error.fieldPath, "field", "config"),
        ),
      );
      expect((await jetStream.streamInfo(fakeStream)).messages, BigInt.two);
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 15)),
  );

  test(
    "Object Store status is exact and seal is permanent",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final bucketName = "Z$suffix";
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
      final maximumAge = JetStreamNanoseconds(BigInt.from(120000000000));
      final maximumBytes = BigInt.from(1024 * 1024);
      final bucket = await jetStream.objectStore.createStore(
        JetStreamObjectStoreConfig(
          name: bucketName,
          description: "sealed objects",
          storage: JetStreamStorage.memory,
          maximumAge: maximumAge,
          maximumBytes: maximumBytes,
          metadata: const {"owner": "nats.dart"},
          compression: true,
        ),
      );
      bucketExists = true;
      await bucket.putBytes("readable", const [1, 2, 3]);

      final before = await bucket.status();
      expect(before.name, bucketName);
      expect(before.description, "sealed objects");
      expect(before.maximumAge.nanoseconds, maximumAge.nanoseconds);
      expect(before.maximumBytes, maximumBytes);
      expect(before.storage, JetStreamStorage.memory);
      expect(before.replicas, 1);
      expect(before.sealed, isFalse);
      expect(before.storageBytes, greaterThan(BigInt.zero));
      expect(before.streamMessages, BigInt.two);
      expect(before.metadata, {"owner": "nats.dart"});
      expect(before.systemMetadata["_nats.ver"], "2.14.6");
      expect(before.systemMetadata["_nats.level"], "4");
      expect(before.compression, isTrue);

      final sealed = await bucket.seal();
      expect(sealed.sealed, isTrue);
      expect(sealed.maximumAge.nanoseconds, BigInt.zero);
      expect(sealed.maximumBytes, maximumBytes);
      expect(sealed.metadata, before.metadata);
      expect((await bucket.seal()).sealed, isTrue);
      expect((await bucket.status()).sealed, isTrue);
      expect(await bucket.getBytes("readable"), [1, 2, 3]);
      await expectLater(
        bucket.putBytes("blocked", const [4]),
        throwsA(isA<JetStreamObjectPutException>()),
      );
      await expectLater(
        bucket.delete("readable"),
        throwsA(isA<JetStreamApiException>()),
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 15)),
  );
}
