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
    "KV values, watches, history, conflicts, and lifetimes round trip",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final bucketName = "K$suffix";
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(serverUrl!)]),
      );
      final jetStream = JetStreamContext(connection);
      var bucketExists = false;
      addTearDown(() async {
        if (bucketExists) {
          try {
            await jetStream.keyValue.deleteBucket(bucketName);
          } on JetStreamApiException {
            // Cleanup follows the test owned bucket state.
          }
        }
        await connection.close();
      });
      final bucket = await jetStream.keyValue.createBucket(
        JetStreamKeyValueBucketConfig(
          name: bucketName,
          history: 2,
          description: "KV integration",
          metadata: const {"owner": "nats.dart"},
          compression: true,
          limitMarkerTtl: JetStreamNanoseconds.fromDuration(
            const Duration(seconds: 2),
          ),
        ),
      );
      bucketExists = true;
      expect(await bucket.get("missing"), isNull);

      await bucket.put("watch.initial", const [7]);
      final currentWatch = bucket.watch(
        options: JetStreamKeyValueWatchOptions(filter: "watch.>"),
      );
      final currentEvents = StreamIterator(currentWatch.events);
      expect(await currentEvents.moveNext(), isTrue);
      final initialChange = currentEvents.current as JetStreamKeyValueChanged;
      expect(initialChange.entry.key, "watch.initial");
      expect(await currentEvents.moveNext(), isTrue);
      expect(
        currentEvents.current,
        isA<JetStreamKeyValueInitialStateComplete>(),
      );
      await bucket.put("watch.live", const [8]);
      expect(await currentEvents.moveNext(), isTrue);
      expect(
        (currentEvents.current as JetStreamKeyValueChanged).entry.key,
        "watch.live",
      );
      await currentWatch.close();
      await currentEvents.cancel();
      await currentWatch.done;

      final updatesWatch = bucket.watch(
        options: JetStreamKeyValueWatchOptions(
          filter: "updates.>",
          start: const JetStreamKeyValueWatchStart.updates(),
          ignoreDeletes: true,
        ),
      );
      final updatesEvents = StreamIterator(updatesWatch.events);
      expect(await updatesEvents.moveNext(), isTrue);
      expect(
        updatesEvents.current,
        isA<JetStreamKeyValueInitialStateComplete>(),
      );
      final suppressedRevision = await bucket.put("updates.one", const [1]);
      expect(await updatesEvents.moveNext(), isTrue);
      expect(
        (updatesEvents.current as JetStreamKeyValueChanged).entry.revision,
        suppressedRevision,
      );
      await bucket.delete("updates.one", expectedRevision: suppressedRevision);
      await bucket.put("updates.two", const [2]);
      expect(await updatesEvents.moveNext(), isTrue);
      expect(
        (updatesEvents.current as JetStreamKeyValueChanged).entry.key,
        "updates.two",
      );
      await updatesWatch.close();
      await updatesEvents.cancel();
      await updatesWatch.done;

      final overflowWatch = bucket.watch(
        options: JetStreamKeyValueWatchOptions(
          filter: "overflow.>",
          start: const JetStreamKeyValueWatchStart.updates(),
          maximumPendingMessages: 1,
        ),
      );
      final overflowEvents = StreamIterator(overflowWatch.events);
      expect(await overflowEvents.moveNext(), isTrue);
      expect(
        overflowEvents.current,
        isA<JetStreamKeyValueInitialStateComplete>(),
      );
      final overflowDone = expectLater(
        overflowWatch.done,
        throwsA(isA<NatsSlowConsumerException>()),
      );
      await bucket.put("overflow.one", const [1]);
      await bucket.put("overflow.two", const [2]);
      await overflowDone;
      await expectLater(
        overflowEvents.moveNext(),
        throwsA(isA<NatsSlowConsumerException>()),
      );
      await overflowEvents.cancel();
      await bucket.delete("overflow.one");
      await bucket.delete("overflow.two");

      final firstRevision = await bucket.put("users.one", const [
        0,
        255,
        13,
        10,
      ]);
      expect(firstRevision, greaterThan(BigInt.zero));
      final firstValue = await bucket.get("users.one");
      expect(firstValue?.operation, JetStreamKeyValueOperation.put);
      expect(firstValue?.value, const [0, 255, 13, 10]);
      expect(firstValue?.revision, firstRevision);
      await expectLater(
        bucket.create("users.one", const [1]),
        throwsA(isA<JetStreamKeyValueConflictException>()),
      );
      await expectLater(
        bucket.update("users.one", const [2], BigInt.from(999)),
        throwsA(isA<JetStreamKeyValueConflictException>()),
      );
      final secondRevision = await bucket.update("users.one", const [
        2,
      ], firstRevision);
      expect(secondRevision, firstRevision + BigInt.one);

      final history = await bucket.history("users.one");
      expect(history.map((entry) => entry.revision), [
        firstRevision,
        secondRevision,
      ]);
      expect(history.map((entry) => entry.value), [
        const [0, 255, 13, 10],
        const [2],
      ]);
      expect(history.last.delta, BigInt.zero);
      expect(
        (await bucket.getRevision("users.one", firstRevision))?.value,
        const [0, 255, 13, 10],
      );
      expect(await bucket.getRevision("users.two", firstRevision), isNull);

      await bucket.put("users.two", const [3]);
      expect(await bucket.keys(), [
        "updates.two",
        "users.one",
        "users.two",
        "watch.initial",
        "watch.live",
      ]);
      final deleteRevision = await bucket.delete(
        "users.one",
        expectedRevision: secondRevision,
      );
      final deleted = await bucket.get("users.one");
      expect(deleted?.operation, JetStreamKeyValueOperation.delete);
      expect(deleted?.revision, deleteRevision);
      expect(await bucket.keys(), [
        "updates.two",
        "users.two",
        "watch.initial",
        "watch.live",
      ]);

      final recreatedAfterDelete = await bucket.create("users.one", const [4]);
      expect(recreatedAfterDelete, greaterThan(deleteRevision));
      expect(await bucket.keys(), [
        "updates.two",
        "users.one",
        "users.two",
        "watch.initial",
        "watch.live",
      ]);
      final purgeRevision = await bucket.purge(
        "users.one",
        expectedRevision: recreatedAfterDelete,
      );
      final purged = await bucket.get("users.one");
      expect(purged?.operation, JetStreamKeyValueOperation.purge);
      expect(purged?.revision, purgeRevision);
      final recreatedAfterPurge = await bucket.create("users.one", const [5]);
      expect(recreatedAfterPurge, greaterThan(purgeRevision));
      expect(await bucket.history("users.one"), hasLength(2));

      await bucket.create(
        "ttl.short",
        const [9],
        lifetime: JetStreamKeyValueLifetime.expiresAfter(
          const Duration(seconds: 1),
        ),
      );
      final expired =
          await Stream<int>.periodic(
                const Duration(milliseconds: 25),
                (index) => index,
              )
              .asyncMap((_) => bucket.get("ttl.short"))
              .firstWhere(
                (entry) => entry?.operation == JetStreamKeyValueOperation.purge,
              )
              .timeout(const Duration(seconds: 5));
      expect(expired?.value, isEmpty);
      expect(await bucket.keys(), isNot(contains("ttl.short")));

      final neverRevision = await bucket.create("ttl.never", const [
        10,
      ], lifetime: const JetStreamKeyValueLifetime.never());
      expect((await bucket.get("ttl.never"))?.revision, neverRevision);
      await bucket.purge(
        "ttl.never",
        expectedRevision: neverRevision,
        markerLifetime: JetStreamKeyValueLifetime.expiresAfter(
          const Duration(seconds: 2),
        ),
      );
      expect((await jetStream.keyValue.bucket(bucketName)).name, bucketName);
      await jetStream.keyValue.deleteBucket(bucketName);
      bucketExists = false;
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}
