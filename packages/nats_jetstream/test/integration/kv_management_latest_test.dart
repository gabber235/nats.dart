@TestOn("vm")
library;

import "dart:async";
import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  final serverUrl = Platform.environment["NATS_URL"];
  final skipReason = serverUrl == null ? "NATS_URL is not configured" : false;

  test(
    "KV management status filters wildcard mutations and cleanup round trip",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final bucketName = "M$suffix";
      final cleanupName = "C$suffix";
      final malformedName = "BAD$suffix";
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(serverUrl!)]),
      );
      final jetStream = JetStreamContext(connection);
      final streams = <String>[];
      final subscriptions = <NatsSubscription>[];
      addTearDown(() async {
        for (final subscription in subscriptions) {
          await subscription.unsubscribe();
        }
        for (final stream in streams.reversed) {
          try {
            await jetStream.deleteStream(stream);
          } on JetStreamApiException {
            continue;
          }
        }
        await connection.close();
      });

      final republishSubject = "republished.$suffix.>";
      final republished = await connection.subscribe(republishSubject);
      subscriptions.add(republished);
      final republishedMessage = republished.messages.first;
      final bucket = await jetStream.keyValue.createBucket(
        JetStreamKeyValueBucketConfig(
          name: bucketName,
          history: 3,
          storage: JetStreamStorage.memory,
          maximumBytes: BigInt.from(1048576),
          maximumValueBytes: BigInt.from(1024),
          description: "management proof",
          metadata: const {"owner": "nats.dart"},
          compression: true,
          republish: JetStreamRepublish(
            source: ">",
            destination: republishSubject,
          ),
        ),
      );
      streams.add("KV_$bucketName");

      await bucket.put("status.key", const [1]);
      final mirrored = await republishedMessage.timeout(
        const Duration(seconds: 2),
      );
      expect(
        mirrored.subject,
        "republished.$suffix.\$KV.$bucketName.status.key",
      );
      expect(mirrored.payload, const [1]);

      final status = await bucket.status();
      expect(status.bucket, bucketName);
      expect(status.values, BigInt.one);
      expect(status.history, 3);
      expect(status.knownStorage, JetStreamStorage.memory);
      expect(status.replicas, 1);
      expect(status.description, "management proof");
      expect(status.maximumBytes, BigInt.from(1048576));
      expect(status.maximumValueBytes, BigInt.from(1024));
      expect(status.metadata, containsPair("owner", "nats.dart"));
      expect(status.compression, isTrue);
      expect(status.compressionWireValue, "s2");
      expect(status.republish?.source, ">");
      expect(status.republish?.destination, republishSubject);

      final listed = await jetStream.keyValue.list().toList();
      expect(listed.where((entry) => entry.bucket == bucketName), hasLength(1));

      final historyOne = await bucket.put("history.one", const [1]);
      await bucket.update("history.one", const [2], historyOne);
      await bucket.put("history.two", const [3]);
      await bucket.put("history.other", const [4]);
      final selectedHistory = await bucket.historyMatching(const [
        "history.one",
        "history.two",
      ]);
      expect(selectedHistory.map((entry) => entry.key), [
        "history.one",
        "history.one",
        "history.two",
      ]);

      final watch = bucket.watch(
        options: JetStreamKeyValueWatchOptions(
          filters: const ["watch.one", "watch.two"],
          start: const JetStreamKeyValueWatchStart.updates(),
        ),
      );
      final events = StreamIterator(watch.events);
      expect(await events.moveNext(), isTrue);
      expect(events.current, isA<JetStreamKeyValueInitialStateComplete>());
      await bucket.put("watch.other", const [0]);
      await bucket.put("watch.one", const [1]);
      await bucket.put("watch.two", const [2]);
      expect(await events.moveNext(), isTrue);
      expect(
        (events.current as JetStreamKeyValueChanged).entry.key,
        "watch.one",
      );
      expect(await events.moveNext(), isTrue);
      expect(
        (events.current as JetStreamKeyValueChanged).entry.key,
        "watch.two",
      );
      await watch.close();
      await events.cancel();
      await watch.done;

      await bucket.put("delete.one", const [1]);
      await bucket.put("delete.two", const [2]);
      await bucket.put("delete.other", const [3]);
      final deleted = await bucket.deleteMatching("delete.*");
      expect(deleted.revisions.keys, [
        "delete.one",
        "delete.other",
        "delete.two",
      ]);
      expect(
        (await bucket.get("delete.one"))?.operation,
        JetStreamKeyValueOperation.delete,
      );
      expect(
        (await bucket.get("delete.two"))?.operation,
        JetStreamKeyValueOperation.delete,
      );

      await bucket.put("purge.one", const [1]);
      await bucket.put("purge.one", const [2]);
      await bucket.put("purge.two", const [3]);
      final purged = await bucket.purgeMatching("purge.*");
      expect(purged.matched, 2);
      expect(await bucket.history("purge.one"), hasLength(1));
      expect(
        (await bucket.get("purge.one"))?.operation,
        JetStreamKeyValueOperation.purge,
      );

      final cleanup = await jetStream.keyValue.createBucket(
        JetStreamKeyValueBucketConfig(
          name: cleanupName,
          history: 3,
          storage: JetStreamStorage.memory,
        ),
      );
      streams.add("KV_$cleanupName");
      final cleanupFirst = await cleanup.put("young", const [1]);
      final cleanupSecond = await cleanup.update("young", const [
        2,
      ], cleanupFirst);
      await cleanup.delete("young", expectedRevision: cleanupSecond);
      final recreatedFirst = await cleanup.put("recreated", const [1]);
      final recreatedDelete = await cleanup.delete(
        "recreated",
        expectedRevision: recreatedFirst,
      );
      final recreated = await cleanup.update("recreated", const [
        3,
      ], recreatedDelete);

      final recentCleanup = await cleanup.purgeDeletes();
      expect(recentCleanup.processedMarkers, 1);
      expect(recentCleanup.retainedMarkers, 1);
      expect(recentCleanup.removedMarkers, 0);
      expect(recentCleanup.purged, BigInt.two);
      expect(await cleanup.history("young"), hasLength(1));
      expect((await cleanup.get("recreated"))?.revision, recreated);

      final completeCleanup = await cleanup.purgeDeletes(
        olderThan: Duration.zero,
      );
      expect(completeCleanup.processedMarkers, 1);
      expect(completeCleanup.retainedMarkers, 0);
      expect(completeCleanup.removedMarkers, 1);
      expect(completeCleanup.purged, BigInt.one);
      expect(await cleanup.get("young"), isNull);
      expect((await cleanup.get("recreated"))?.revision, recreated);

      await expectLater(
        jetStream.keyValue.bucket("MISSING$suffix"),
        throwsA(
          isA<JetStreamApiException>().having(
            (error) => error.errorCode,
            "errorCode",
            10059,
          ),
        ),
      );

      final malformedStream = "KV_$malformedName";
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: malformedStream,
          subjects: ["\$KV.WRONG$suffix.>"],
          storage: JetStreamStorage.memory,
          maximumMessagesPerSubject: BigInt.one,
          discard: JetStreamDiscardPolicy.newMessages,
          denyDelete: true,
          allowRollup: true,
          allowDirect: true,
        ),
      );
      streams.add(malformedStream);
      await expectLater(
        jetStream.keyValue.list().toList(),
        throwsA(isA<JetStreamProtocolException>()),
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 20)),
  );
}
