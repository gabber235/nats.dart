import "dart:convert";
import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  final server = Platform.environment["NATS_URL"];
  final skip = server == null ? "NATS_URL is not configured" : false;

  test(
    "stream read, copy, update preserves the complete server configuration",
    () async {
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(server!)]),
      );
      final jetStream = JetStreamContext(connection);
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final name = "STREAM$suffix";
      addTearDown(() async {
        try {
          await jetStream.deleteStream(name);
        } on JetStreamApiException {
          // The test may fail before stream creation.
        }
        await connection.close();
      });

      final created = await jetStream.createStream(
        JetStreamStreamConfig(
          name: name,
          description: "created",
          subjects: ["stream.$suffix.>"],
          maximumConsumers: 8,
          maximumMessages: BigInt.from(1000),
          maximumBytes: BigInt.from(1024 * 1024),
          maximumAge: JetStreamNanoseconds.fromDuration(
            const Duration(minutes: 10),
          ),
          maximumMessagesPerSubject: BigInt.from(100),
          maximumMessageSize: 4096,
          storage: JetStreamStorage.memory,
          duplicateWindow: JetStreamNanoseconds.fromDuration(
            const Duration(seconds: 30),
          ),
          allowRollup: true,
          allowDirect: true,
          consumerLimits: JetStreamStreamConsumerLimits(
            inactiveThresholdNanoseconds: BigInt.from(30000000000),
            maximumAckPending: 32,
          ),
          metadata: const {"owner": "stream test", "schema": "2.14"},
          allowMessageTtl: true,
          subjectDeleteMarkerTtl: JetStreamNanoseconds.fromDuration(
            const Duration(minutes: 1),
          ),
        ),
      );
      final before = Map<String, Object?>.from(created.config.toJson())
        ..remove("description");

      final strictResponse = await connection.request(
        "\$JS.API.STREAM.INFO.$name",
        utf8.encode('{"unknown_latest_field":true}'),
      );
      final strictBody = jsonDecode(utf8.decode(strictResponse.payload));
      expect(strictBody, isA<Map<String, Object?>>());
      final strictError = (strictBody as Map<String, Object?>)["error"];
      expect(strictError, isA<Map<String, Object?>>());
      expect((strictError as Map<String, Object?>)["code"], 400);
      expect(
        strictError["description"],
        allOf(isA<String>(), contains("unknown field")),
      );

      final updated = await jetStream.updateStream(
        created.config.copyWith(description: "updated"),
      );
      final after = Map<String, Object?>.from(updated.config.toJson())
        ..remove("description");

      expect(updated.config.description, "updated");
      expect(after, before);
      expect(updated.created, created.created);
      expect(updated.timestamp, isNotEmpty);
      expect(updated.state.messages, BigInt.zero);

      final upserted = await jetStream.createOrUpdateStream(
        updated.config.copyWith(description: "upserted"),
      );
      expect(upserted.config.description, "upserted");
      expect(
        Map<String, Object?>.from(upserted.config.toJson())
          ..remove("description"),
        before,
      );

      final first = await jetStream.publish("stream.$suffix.a", const [1]);
      final second = await jetStream.publish("stream.$suffix.b", const [2]);
      final third = await jetStream.publish("stream.$suffix.a", const [3]);
      final fourth = await jetStream.publish("stream.$suffix.b", const [4]);
      expect(first.sequence, BigInt.one);
      expect(second.sequence, BigInt.from(2));
      expect(third.sequence, BigInt.from(3));
      expect(fourth.sequence, BigInt.from(4));

      final namePage = await jetStream.streamNamesPage(
        subject: "stream.$suffix.a",
      );
      expect(namePage.names, [name]);
      final infoPage = await jetStream.streamListPage(
        subject: "stream.$suffix.a",
      );
      expect(infoPage.streams.single.config.name, name);
      expect(await jetStream.streamNameBySubject("stream.$suffix.b"), name);
      expect(await jetStream.streamNames().contains(name), isTrue);

      final last = await jetStream.getLastMessage(name, "stream.$suffix.a");
      expect(last.sequence, third.sequence);
      expect(last.payload, [3]);
      final next = await jetStream.getNextMessage(
        name,
        "stream.$suffix.a",
        second.sequence,
      );
      expect(next.sequence, third.sequence);

      await jetStream.deleteMessage(name, second.sequence);
      await expectLater(
        jetStream.getMessage(name, second.sequence),
        throwsA(
          isA<JetStreamApiException>().having(
            (error) => error.errorCode,
            "errorCode",
            10037,
          ),
        ),
      );
      await jetStream.deleteMessage(name, fourth.sequence, secure: true);
      await expectLater(
        jetStream.getMessage(name, fourth.sequence),
        throwsA(
          isA<JetStreamApiException>().having(
            (error) => error.errorCode,
            "errorCode",
            10037,
          ),
        ),
      );
      final purged = await jetStream.purgeStream(
        name,
        options: JetStreamPurgeOptions(
          filter: "stream.$suffix.a",
          keep: BigInt.one,
        ),
      );
      expect(purged.purged, BigInt.one);
      expect((await jetStream.streamInfo(name)).messages, BigInt.one);
    },
    skip: skip,
  );

  test(
    "mirror promotion preserves stored data and accepts publications",
    () async {
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(server!)]),
      );
      final jetStream = JetStreamContext(connection);
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final origin = "ORIGIN$suffix";
      final mirror = "MIRROR$suffix";
      final originSubject = "origin.$suffix";
      final promotedSubject = "promoted.$suffix";
      addTearDown(() async {
        for (final stream in [mirror, origin]) {
          try {
            await jetStream.deleteStream(stream);
          } on JetStreamApiException {
            continue;
          }
        }
        await connection.close();
      });

      await jetStream.createStream(
        JetStreamStreamConfig(
          name: origin,
          subjects: [originSubject],
          storage: JetStreamStorage.memory,
        ),
      );
      await jetStream.publish(originSubject, const [1]);
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: mirror,
          mirror: JetStreamStreamSource(name: origin),
          storage: JetStreamStorage.memory,
        ),
      );

      final mirrored = await _awaitMessages(jetStream, mirror, BigInt.one);
      expect(mirrored.config.raw["mirror"], isNotNull);
      expect((await jetStream.getMessage(mirror, BigInt.one)).payload, [1]);

      final promoted = await jetStream.updateStream(
        mirrored.config.copyWith(
          subjects: [promotedSubject],
          clearMirror: true,
        ),
      );
      expect(promoted.config.raw.containsKey("mirror"), isFalse);
      expect(promoted.config.raw["mirror_direct"], isFalse);
      expect(promoted.config.subjects, [promotedSubject]);

      final acknowledgement = await jetStream.publish(promotedSubject, const [
        2,
      ]);
      expect(acknowledgement.stream, mirror);
      expect(acknowledgement.sequence, BigInt.two);
      expect((await jetStream.getMessage(mirror, BigInt.one)).payload, [1]);
      expect((await jetStream.getMessage(mirror, BigInt.two)).payload, [2]);
    },
    skip: skip,
    timeout: const Timeout(Duration(seconds: 20)),
  );
}

Future<JetStreamStreamInfo> _awaitMessages(
  JetStreamContext jetStream,
  String stream,
  BigInt messages,
) async {
  final deadline = DateTime.now().add(const Duration(seconds: 10));
  JetStreamStreamInfo? last;
  while (DateTime.now().isBefore(deadline)) {
    last = await jetStream.streamInfo(stream);
    if (last.messages == messages) return last;
    await Future<void>.delayed(const Duration(milliseconds: 20));
  }
  throw StateError(
    "Stream $stream did not reach $messages messages. "
    "Last messages: ${last?.messages}",
  );
}
