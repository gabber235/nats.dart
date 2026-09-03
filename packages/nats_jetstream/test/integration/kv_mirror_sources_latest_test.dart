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
    "KV mirror sources updates and strict strings round trip",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final sourceName = "S$suffix";
      final secondName = "T$suffix";
      final mirrorName = "M$suffix";
      final aggregateName = "A$suffix";
      final upsertName = "U$suffix";
      final preserveName = "P$suffix";
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

      final source = await jetStream.keyValue.createBucket(
        JetStreamKeyValueBucketConfig(
          name: sourceName,
          storage: JetStreamStorage.memory,
          limitMarkerTtl: JetStreamNanoseconds.fromDuration(
            const Duration(seconds: 5),
          ),
          metadata: const {"owner": "source"},
        ),
      );
      streams.add("KV_$sourceName");
      final second = await jetStream.keyValue.createBucket(
        JetStreamKeyValueBucketConfig(
          name: secondName,
          storage: JetStreamStorage.memory,
        ),
      );
      streams.add("KV_$secondName");
      await source.putString("source.key", "한글 🙂");
      await second.putString("second.key", "second");
      await source.put("invalid", const [0xc3, 0x28]);
      await expectLater(source.getString("invalid"), throwsFormatException);

      final mirror = await jetStream.keyValue.createBucket(
        JetStreamKeyValueBucketConfig(
          name: mirrorName,
          storage: JetStreamStorage.memory,
          mirror: JetStreamStreamSource(name: sourceName),
        ),
      );
      streams.add("KV_$mirrorName");
      expect(mirror.backingStreamName, "KV_$mirrorName");
      expect(mirror.readSubjectPrefix, "\$KV.$sourceName.");
      expect(mirror.editStreamName, "KV_$sourceName");
      expect(mirror.editSubjectPrefix, "\$KV.$sourceName.");
      expect(mirror.allowPerKeyTtl, isTrue);
      expect(await _eventually(() => mirror.getString("source.key")), "한글 🙂");
      final mirroredEntry = await _eventually(() => mirror.get("source.key"));
      expect(
        await mirror.getRevision("source.key", mirroredEntry.revision),
        isNotNull,
      );
      expect(await mirror.history("source.key"), hasLength(1));
      expect(await mirror.keys(), contains("source.key"));
      await mirror.putString("mirror.edit", "through source");
      expect(await source.getString("mirror.edit"), "through source");
      expect(
        await _eventually(() => mirror.getString("mirror.edit")),
        "through source",
      );
      await mirror.delete("mirror.edit");
      expect(
        (await source.get("mirror.edit"))?.operation,
        JetStreamKeyValueOperation.delete,
      );
      await mirror.create("mirror.ttl", const [
        1,
      ], lifetime: const JetStreamKeyValueLifetime.never());
      expect((await source.get("mirror.ttl"))?.value, [1]);
      final mirrorStatus = await mirror.status();
      expect(mirrorStatus.mirror?.name, "KV_$sourceName");
      expect(mirrorStatus.sources, isEmpty);

      final aggregate = await jetStream.keyValue.createBucket(
        JetStreamKeyValueBucketConfig(
          name: aggregateName,
          storage: JetStreamStorage.memory,
          sources: [
            JetStreamStreamSource(name: sourceName),
            JetStreamStreamSource(name: "KV_$secondName"),
          ],
        ),
      );
      streams.add("KV_$aggregateName");
      expect(
        await _eventually(() => aggregate.getString("source.key")),
        "한글 🙂",
      );
      expect(
        await _eventually(() => aggregate.getString("second.key")),
        "second",
      );
      final aggregateStatus = await aggregate.status();
      expect(aggregateStatus.mirror, isNull);
      expect(aggregateStatus.sources.map((value) => value.name), [
        "KV_$sourceName",
        "KV_$secondName",
      ]);
      expect(
        aggregateStatus.sources.first.subjectTransforms.single.source,
        "\$KV.$sourceName.>",
      );
      expect(
        aggregateStatus.sources.first.subjectTransforms.single.destination,
        "\$KV.$aggregateName.>",
      );

      await expectLater(
        jetStream.keyValue.update(
          JetStreamKeyValueBucketConfig(
            name: upsertName,
            storage: JetStreamStorage.memory,
          ),
        ),
        throwsA(
          isA<JetStreamApiException>().having(
            (error) => error.errorCode,
            "errorCode",
            10059,
          ),
        ),
      );
      final updated = await jetStream.keyValue.createOrUpdate(
        JetStreamKeyValueBucketConfig(
          name: upsertName,
          storage: JetStreamStorage.memory,
          description: "created after absence",
        ),
      );
      streams.add("KV_$upsertName");
      expect((await updated.status()).description, "created after absence");
      final upserted = await jetStream.keyValue.createOrUpdate(
        JetStreamKeyValueBucketConfig(
          name: upsertName,
          storage: JetStreamStorage.memory,
          description: "updated existing",
        ),
      );
      expect((await upserted.status()).description, "updated existing");

      await jetStream.createStream(
        JetStreamStreamConfig(
          name: "KV_$preserveName",
          subjects: ["\$KV.$preserveName.>"],
          storage: JetStreamStorage.memory,
          maximumMessages: BigInt.from(99),
          maximumMessagesPerSubject: BigInt.one,
          discard: JetStreamDiscardPolicy.newMessages,
          denyDelete: true,
          allowRollup: true,
          allowDirect: true,
        ),
      );
      streams.add("KV_$preserveName");
      await jetStream.keyValue.update(
        JetStreamKeyValueBucketConfig(
          name: preserveName,
          storage: JetStreamStorage.memory,
          description: "preserved",
        ),
      );
      final preserved = await jetStream.streamInfo("KV_$preserveName");
      expect(preserved.config.raw["max_msgs"], BigInt.from(99));
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 40)),
  );
}

Future<T> _eventually<T>(Future<T?> Function() read) async {
  final deadline = DateTime.now().add(const Duration(seconds: 10));
  Object? lastError;
  while (DateTime.now().isBefore(deadline)) {
    try {
      final value = await read();
      if (value != null) return value;
    } on Object catch (error) {
      lastError = error;
    }
    await Future<void>.delayed(const Duration(milliseconds: 25));
  }
  throw StateError("Value did not replicate before deadline: $lastError");
}
