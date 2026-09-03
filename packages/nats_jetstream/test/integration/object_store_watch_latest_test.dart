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
    "Object Store watch and list preserve bounded metadata semantics",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final bucketName = "W$suffix";
      final connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(serverUrl!)],
          name: "nats-dart-object-watch-$suffix",
        ),
      );
      final jetStream = JetStreamContext(connection);
      var bucketExists = false;
      addTearDown(() async {
        if (bucketExists) {
          try {
            await jetStream.objectStore.deleteStore(bucketName);
          } on JetStreamApiException {
            // The test already proved or attempted deletion.
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

      final emptyWatch = bucket.watch();
      final emptyEvents = StreamIterator(emptyWatch.events);
      expect(await emptyEvents.moveNext(), isTrue);
      expect(emptyEvents.current, isA<JetStreamObjectInitialStateComplete>());
      await emptyWatch.close();
      await emptyEvents.cancel();
      expect(
        (await jetStream.consumers.listPage("OBJ_$bucketName")).total,
        BigInt.zero,
      );
      final emptyList = await bucket.list();
      expect(emptyList, isEmpty);
      expect(emptyList.clear, throwsUnsupportedError);

      final alpha = await bucket.put(
        JetStreamObjectMetadata(name: "alpha", chunkSize: 2),
        const [1, 2, 3, 4, 5],
      );
      final unicode = await bucket.putBytes("한글", const [6]);
      await bucket.putBytes("deleted", const [7]);
      await bucket.delete("deleted");
      final link = await bucket.addObjectLink(
        "link",
        targetStore: bucket,
        targetName: "alpha",
      );

      final currentWatch = bucket.watch();
      final currentEvents = StreamIterator(currentWatch.events);
      final current = <JetStreamObjectInfo>[];
      while (await currentEvents.moveNext()) {
        switch (currentEvents.current) {
          case JetStreamObjectChanged(:final info, :final encodedBytes):
            expect(encodedBytes, greaterThan(0));
            current.add(info);
          case JetStreamObjectInitialStateComplete():
            break;
        }
        if (currentEvents.current is JetStreamObjectInitialStateComplete) {
          break;
        }
      }
      expect(current.map((info) => info.name).toSet(), {
        "alpha",
        "한글",
        "deleted",
        "link",
      });
      expect(
        current.map((info) => info.revision),
        orderedEquals(current.map((info) => info.revision).toList()..sort()),
      );
      expect(
        current.singleWhere((info) => info.name == "deleted").deleted,
        isTrue,
      );
      expect(
        current.singleWhere((info) => info.name == "link").link?.name,
        "alpha",
      );

      final live = await bucket.putBytes("live", const [8]);
      expect(await currentEvents.moveNext(), isTrue);
      final liveEvent = currentEvents.current as JetStreamObjectChanged;
      expect(liveEvent.info.name, "live");
      expect(liveEvent.info.revision, live.revision);
      final renamed = await bucket.updateMetadata(
        "live",
        JetStreamObjectMetadataUpdate(name: "renamed"),
      );
      expect(await currentEvents.moveNext(), isTrue);
      final renamedEvent = currentEvents.current as JetStreamObjectChanged;
      expect(renamedEvent.info.name, "renamed");
      expect(renamedEvent.info.revision, renamed.revision);
      expect(renamedEvent.info.deleted, isFalse);
      expect(await currentEvents.moveNext(), isTrue);
      final sourceTombstone = currentEvents.current as JetStreamObjectChanged;
      expect(sourceTombstone.info.name, "live");
      expect(sourceTombstone.info.deleted, isTrue);
      expect(sourceTombstone.info.identifier, isNot(renamed.identifier));
      expect(sourceTombstone.info.revision, greaterThan(renamed.revision));
      expect(await bucket.getBytes("renamed"), const [8]);
      await currentWatch.close();
      await currentEvents.cancel();
      await bucket.delete("live");
      expect(await bucket.getBytes("renamed"), const [8]);

      final listed = await bucket.list();
      expect(listed.map((info) => info.name), [
        "alpha",
        "link",
        "renamed",
        "한글",
      ]);
      expect(listed.first.revision, alpha.revision);
      expect(listed[1].revision, link.revision);
      expect(listed.clear, throwsUnsupportedError);
      final withDeleted = await bucket.list(includeDeleted: true);
      expect(withDeleted.map((info) => info.name), [
        "alpha",
        "deleted",
        "link",
        "live",
        "renamed",
        "한글",
      ]);
      expect(
        withDeleted.singleWhere((info) => info.name == "deleted").deleted,
        isTrue,
      );
      await expectLater(
        bucket.list(maximumObjects: 1),
        throwsA(isA<JetStreamObjectListLimitException>()),
      );
      await expectLater(
        bucket.list(maximumMetadataBytes: 1),
        throwsA(isA<JetStreamObjectListLimitException>()),
      );

      final updatesWatch = bucket.watch(
        options: JetStreamObjectWatchOptions(
          names: {"selected"},
          start: const JetStreamObjectWatchStart.updates(),
        ),
      );
      await bucket.putBytes("selected", const [9]);
      final updateEvents = StreamIterator(updatesWatch.events);
      expect(await updateEvents.moveNext(), isTrue);
      expect(updateEvents.current, isA<JetStreamObjectInitialStateComplete>());
      await bucket.putBytes("ignored", const [10]);
      final selected = await bucket.putBytes("selected", const [11]);
      expect(await updateEvents.moveNext(), isTrue);
      final selectedEvent = updateEvents.current as JetStreamObjectChanged;
      expect(selectedEvent.info.revision, selected.revision);
      expect(selectedEvent.info.name, "selected");
      await updatesWatch.close();
      await updateEvents.cancel();
      expect(unicode.name, "한글");
      expect(
        (await jetStream.consumers.listPage("OBJ_$bucketName")).total,
        BigInt.zero,
      );

      await jetStream.objectStore.deleteStore(bucketName);
      bucketExists = false;
      await connection.close();
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "malformed Object Store metadata terminates watch and list",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final bucketName = "M$suffix";
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(serverUrl!)]),
      );
      final jetStream = JetStreamContext(connection);
      final bucket = await jetStream.objectStore.createStore(
        JetStreamObjectStoreConfig(
          name: bucketName,
          storage: JetStreamStorage.memory,
        ),
      );
      addTearDown(() async {
        try {
          await jetStream.objectStore.deleteStore(bucketName);
        } on JetStreamApiException {
          // The test already attempted deletion.
        }
        await connection.close();
      });
      final badName = base64Url.encode(utf8.encode("bad"));
      await jetStream.publish("\$O.$bucketName.M.$badName", utf8.encode("{"));

      final watch = bucket.watch();
      final events = StreamIterator(watch.events);
      final doneFailure = expectLater(
        watch.done,
        throwsA(isA<JetStreamProtocolException>()),
      );
      await expectLater(
        events.moveNext(),
        throwsA(isA<JetStreamProtocolException>()),
      );
      await doneFailure;
      await events.cancel();
      await expectLater(
        bucket.list(),
        throwsA(isA<JetStreamProtocolException>()),
      );
      expect(
        (await jetStream.consumers.listPage("OBJ_$bucketName")).total,
        BigInt.zero,
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}
