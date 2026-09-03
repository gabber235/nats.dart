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
    "KV codecs and custom source transforms preserve logical data",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final codecBucketName = "C$suffix";
      final eventStreamName = "EVENTS$suffix";
      final transformedBucketName = "T$suffix";
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

      final raw = await jetStream.keyValue.createBucket(
        JetStreamKeyValueBucketConfig(
          name: codecBucketName,
          storage: JetStreamStorage.memory,
          history: 3,
        ),
      );
      streams.add("KV_$codecBucketName");
      const codecs = JetStreamKeyValueCodecs(
        key: JetStreamKeyValueBase64KeyCodec(),
        value: JetStreamKeyValueBase64ValueCodec(),
      );
      final coded = raw.withCodecs(codecs);
      const logicalKey = "users.Jane Doe.한글";
      final logicalValue = [0, 1, 127, 128, 255];

      final firstRevision = await coded.put(logicalKey, logicalValue);
      final encodedKey = codecs.encodeKey(logicalKey);
      final rawEntry = await raw.get(encodedKey);
      expect(rawEntry?.key, encodedKey);
      expect(utf8.decode(rawEntry!.value), "AAF/gP8=");

      final decoded = await coded.get(logicalKey);
      expect(decoded?.key, logicalKey);
      expect(decoded?.value, logicalValue);
      expect(decoded?.revision, firstRevision);
      expect(
        (await coded.getRevision(logicalKey, firstRevision))?.value,
        logicalValue,
      );
      expect((await coded.history(logicalKey)).single.key, logicalKey);
      expect(await coded.keys(filters: const ["users.Jane Doe.>"]), [
        logicalKey,
      ]);

      final watch = coded.watch(
        options: JetStreamKeyValueWatchOptions(
          filter: "users.Jane Doe.>",
          start: const JetStreamKeyValueWatchStart.updates(),
        ),
      );
      final events = StreamIterator(watch.events);
      addTearDown(() async {
        await events.cancel();
        await watch.close();
      });
      expect(
        await events.moveNext().timeout(const Duration(seconds: 10)),
        isTrue,
      );
      expect(events.current, isA<JetStreamKeyValueInitialStateComplete>());
      final secondRevision = await coded.put(logicalKey, const [9, 8, 7]);
      expect(
        await events.moveNext().timeout(const Duration(seconds: 10)),
        isTrue,
      );
      final changed = events.current as JetStreamKeyValueChanged;
      expect(changed.entry.key, logicalKey);
      expect(changed.entry.value, [9, 8, 7]);
      expect(changed.entry.revision, secondRevision);
      await watch.close();

      final deleted = await coded.deleteMatching("users.Jane Doe.>");
      expect(deleted.revisions.keys, [logicalKey]);
      expect(
        (await coded.get(logicalKey))?.operation,
        JetStreamKeyValueOperation.delete,
      );
      final strict = raw.withCodecs(
        const JetStreamKeyValueCodecs(
          key: JetStreamKeyValueBase64KeyCodec(),
          value: _RejectEmptyValueCodec(),
        ),
      );
      await strict.put("strict.value", const [1]);
      expect(await strict.keys(filters: const ["strict.>"]), ["strict.value"]);
      await strict.delete("strict.value");
      expect(
        (await strict.get("strict.value"))?.operation,
        JetStreamKeyValueOperation.delete,
      );

      await jetStream.createStream(
        JetStreamStreamConfig(
          name: eventStreamName,
          subjects: ["events.$suffix.processed.>"],
          storage: JetStreamStorage.memory,
        ),
      );
      streams.add(eventStreamName);
      final transformed = await jetStream.keyValue.createBucket(
        JetStreamKeyValueBucketConfig(
          name: transformedBucketName,
          storage: JetStreamStorage.memory,
          sources: [
            JetStreamStreamSource(
              name: eventStreamName,
              subjectTransforms: [
                JetStreamSubjectTransform(
                  source: "events.$suffix.processed.>",
                  destination: "\$KV.$transformedBucketName.>",
                ),
              ],
            ),
          ],
        ),
      );
      streams.add("KV_$transformedBucketName");
      final transformedWatch = transformed.watch(
        options: JetStreamKeyValueWatchOptions(
          filter: "item.42",
          start: const JetStreamKeyValueWatchStart.updates(),
        ),
      );
      final transformedEvents = StreamIterator(transformedWatch.events);
      addTearDown(() async {
        await transformedEvents.cancel();
        await transformedWatch.close();
      });
      expect(
        await transformedEvents.moveNext().timeout(const Duration(seconds: 10)),
        isTrue,
      );
      expect(
        transformedEvents.current,
        isA<JetStreamKeyValueInitialStateComplete>(),
      );
      await jetStream.publish(
        "events.$suffix.processed.item.42",
        utf8.encode("from events"),
      );
      expect(
        await transformedEvents.moveNext().timeout(const Duration(seconds: 10)),
        isTrue,
      );
      final transformedChange =
          transformedEvents.current as JetStreamKeyValueChanged;
      expect(transformedChange.entry.key, "item.42");
      expect(utf8.decode(transformedChange.entry.value), "from events");
      await transformedWatch.close();
      expect(await transformed.getString("item.42"), "from events");

      final status = await transformed.status();
      expect(status.sources.single.name, eventStreamName);
      expect(
        status.sources.single.subjectTransforms.single.source,
        "events.$suffix.processed.>",
      );
      expect(
        status.sources.single.subjectTransforms.single.destination,
        "\$KV.$transformedBucketName.>",
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 45)),
  );
}

final class _RejectEmptyValueCodec implements JetStreamKeyValueValueCodec {
  const _RejectEmptyValueCodec();

  @override
  List<int> encode(List<int> value) => value;

  @override
  List<int> decode(List<int> value) {
    if (value.isEmpty) throw const FormatException("Empty stored value");
    return value;
  }
}
