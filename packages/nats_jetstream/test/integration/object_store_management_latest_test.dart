@TestOn("vm")
library;

import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/object_store_stream.dart";
import "package:test/test.dart";

void main() {
  final serverUrl = Platform.environment["NATS_URL"];
  final skipReason = serverUrl == null ? "NATS_URL is not configured" : false;

  test(
    "Object Store management and strict string helpers round trip",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final managedName = "M$suffix";
      final createdName = "C$suffix";
      final preservedName = "P$suffix";
      final malformedName = "B$suffix";
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(serverUrl!)]),
      );
      final jetStream = JetStreamContext(connection);
      final streams = <String>[];
      addTearDown(() async {
        for (final stream in streams.reversed) {
          try {
            await jetStream.deleteStream(stream);
          } on JetStreamApiException {
            continue;
          }
        }
        await connection.close();
      });

      final managed = await jetStream.objectStore.createStore(
        JetStreamObjectStoreConfig(
          name: managedName,
          description: "before update",
          storage: JetStreamStorage.memory,
          maximumBytes: BigInt.from(1048576),
          metadata: const {"owner": "before"},
          compression: true,
        ),
      );
      streams.add(objectStreamName(managedName));
      const text = "NATS 객체 저장소 🙂";
      await managed.putString("text", text);
      expect(await managed.getString("text"), text);
      expect(await managed.getString("missing"), isNull);
      final immutable = await managed.getBytes("text");
      expect(immutable, isNotNull);
      expect(() => immutable![0] = 0, throwsUnsupportedError);
      await managed.putBytes("invalid", const [0xc3, 0x28]);
      await expectLater(managed.getString("invalid"), throwsFormatException);

      final updated = await jetStream.objectStore.update(
        JetStreamObjectStoreConfig(
          name: managedName,
          description: "after update",
          storage: JetStreamStorage.memory,
          maximumBytes: BigInt.from(2097152),
          metadata: const {"owner": "after"},
          compression: true,
        ),
      );
      final status = await updated.status();
      expect(status.name, managedName);
      expect(status.description, "after update");
      expect(status.maximumBytes, BigInt.from(2097152));
      expect(status.storage, JetStreamStorage.memory);
      expect(status.metadata, containsPair("owner", "after"));
      expect(status.systemMetadata, containsPair("_nats.ver", "2.14.6"));
      expect(status.compression, isTrue);
      expect(await updated.getString("text"), text);

      await jetStream.objectStore.createOrUpdate(
        JetStreamObjectStoreConfig(
          name: managedName,
          description: "upserted",
          storage: JetStreamStorage.memory,
          maximumBytes: BigInt.from(2097152),
          metadata: const {"owner": "upsert"},
          compression: true,
        ),
      );
      expect((await managed.status()).description, "upserted");

      final created = await jetStream.objectStore.createOrUpdate(
        JetStreamObjectStoreConfig(
          name: createdName,
          storage: JetStreamStorage.memory,
        ),
      );
      streams.add(objectStreamName(createdName));
      expect(created.name, createdName);

      await jetStream.createStream(
        JetStreamStreamConfig(
          name: objectStreamName(preservedName),
          subjects: objectStreamSubjects(preservedName),
          storage: JetStreamStorage.memory,
          discard: JetStreamDiscardPolicy.newMessages,
          maximumMessageSize: 4096,
          allowRollup: true,
          allowDirect: true,
        ),
      );
      streams.add(objectStreamName(preservedName));
      await jetStream.objectStore.update(
        JetStreamObjectStoreConfig(
          name: preservedName,
          description: "preserved",
          storage: JetStreamStorage.memory,
        ),
      );
      final preserved = await jetStream.streamInfo(
        objectStreamName(preservedName),
      );
      expect(preserved.config.raw["max_msg_size"], BigInt.from(4096));

      final statuses = await jetStream.objectStore.list().toList();
      expect(
        statuses.where((value) => value.name == managedName),
        hasLength(1),
      );
      expect(
        statuses.where((value) => value.name == createdName),
        hasLength(1),
      );
      expect(
        statuses.where((value) => value.name == preservedName),
        hasLength(1),
      );
      final names = await jetStream.objectStore.names().toList();
      expect(names, containsAll([managedName, createdName, preservedName]));

      await jetStream.createStream(
        JetStreamStreamConfig(
          name: objectStreamName(malformedName),
          subjects: ["\$O.WRONG$suffix.C.>", "\$O.$malformedName.M.>"],
          storage: JetStreamStorage.memory,
          discard: JetStreamDiscardPolicy.newMessages,
          allowRollup: true,
          allowDirect: true,
        ),
      );
      streams.add(objectStreamName(malformedName));
      await expectLater(
        jetStream.objectStore.store(malformedName),
        throwsA(isA<JetStreamProtocolException>()),
      );
      await expectLater(
        jetStream.objectStore.list().toList(),
        throwsA(isA<JetStreamProtocolException>()),
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}
