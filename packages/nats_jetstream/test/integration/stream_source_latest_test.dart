import "dart:async";
import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  final server = Platform.environment["NATS_URL"];
  final skip = server == null ? "NATS_URL is not configured" : false;

  test(
    "ordinary and durable sources move messages and report zero lag",
    () async {
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(server!)]),
      );
      final jetStream = JetStreamContext(connection);
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final ordinaryOrigin = "ORIGIN$suffix";
      final ordinaryTarget = "TARGET$suffix";
      final durableOrigin = "WORK$suffix";
      final durableTarget = "DURABLE$suffix";
      final consumer = "SOURCE$suffix";
      final deliverSubject = "delivery.$suffix.source";
      final streams = [
        ordinaryTarget,
        ordinaryOrigin,
        durableTarget,
        durableOrigin,
      ];
      addTearDown(() async {
        for (final stream in streams) {
          try {
            await jetStream.deleteStream(stream);
          } on JetStreamApiException {
            // The test may fail before every stream is created.
          }
        }
        await connection.close();
      });

      await jetStream.createStream(
        JetStreamStreamConfig(
          name: ordinaryOrigin,
          subjects: ["origin.$suffix.>"],
          storage: JetStreamStorage.memory,
        ),
      );
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: ordinaryTarget,
          storage: JetStreamStorage.memory,
          sources: [JetStreamStreamSource(name: ordinaryOrigin)],
        ),
      );
      await jetStream.publish("origin.$suffix.a", const [1]);
      await jetStream.publish("origin.$suffix.b", const [2]);

      final ordinaryInfo = await _awaitStream(
        jetStream,
        ordinaryTarget,
        messages: BigInt.from(2),
      );
      expect(ordinaryInfo.sources.single.name, ordinaryOrigin);
      expect(ordinaryInfo.sources.single.lag, BigInt.zero);
      expect((await jetStream.getMessage(ordinaryTarget, BigInt.one)).payload, [
        1,
      ]);

      await jetStream.createStream(
        JetStreamStreamConfig(
          name: durableOrigin,
          subjects: ["work.$suffix"],
          retention: JetStreamRetentionPolicy.workQueue,
          storage: JetStreamStorage.memory,
        ),
      );
      await jetStream.consumers.createReliableSource(
        durableOrigin,
        JetStreamReliableSourceConsumerConfig(
          name: consumer,
          deliverSubject: deliverSubject,
          maximumAckPending: 64,
          filters: JetStreamConsumerFilters.single("work.$suffix"),
        ),
      );
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: durableTarget,
          storage: JetStreamStorage.memory,
          sources: [
            JetStreamStreamSource(
              name: durableOrigin,
              consumer: JetStreamStreamConsumerSource(
                name: consumer,
                deliverSubject: deliverSubject,
              ),
            ),
          ],
        ),
      );
      await jetStream.publish("work.$suffix", const [9]);

      final durableInfo = await _awaitStream(
        jetStream,
        durableTarget,
        messages: BigInt.one,
      );
      expect(durableInfo.sources.single.name, durableOrigin);
      expect(durableInfo.sources.single.lag, BigInt.zero);
      await _awaitStream(jetStream, durableOrigin, messages: BigInt.zero);
      expect((await jetStream.getMessage(durableTarget, BigInt.one)).payload, [
        9,
      ]);
    },
    skip: skip,
    timeout: const Timeout(Duration(seconds: 20)),
  );

  test(
    "managed Interest source stores before release and cleans its consumer",
    () async {
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(server!)]),
      );
      final jetStream = JetStreamContext(connection);
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final origin = "INTEREST$suffix";
      final target = "AGGREGATE$suffix";
      final subject = "interest.$suffix";
      final streams = [target, origin];
      addTearDown(() async {
        for (final stream in streams) {
          try {
            await jetStream.deleteStream(stream);
          } on JetStreamApiException {
            // The test may fail before every stream is created.
          }
        }
        await connection.close();
      });

      await jetStream.createStream(
        JetStreamStreamConfig(
          name: origin,
          subjects: [subject],
          retention: JetStreamRetentionPolicy.interest,
          storage: JetStreamStorage.memory,
        ),
      );
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: target,
          storage: JetStreamStorage.memory,
          sources: [JetStreamStreamSource(name: origin)],
        ),
      );

      final managedNames = await _awaitConsumerNames(
        jetStream,
        origin,
        (names) => names.length == 1,
      );
      final managedName = managedNames.single;
      expect(managedName, startsWith("JS_SRC_"));
      final managed = await jetStream.consumers.info(origin, managedName);
      expect(managed.config.kind, JetStreamConsumerKind.reliableSource);
      expect(managed.config.ackPolicy, JetStreamConsumerAckPolicy.flowControl);

      await jetStream.publish(subject, const [7, 8, 9]);
      final targetInfo = await _awaitStream(
        jetStream,
        target,
        messages: BigInt.one,
      );
      expect(targetInfo.sources.single.name, origin);
      expect(targetInfo.sources.single.lag, BigInt.zero);
      expect((await jetStream.getMessage(target, BigInt.one)).payload, [
        7,
        8,
        9,
      ]);
      await _awaitStream(jetStream, origin, messages: BigInt.zero);

      await jetStream.deleteStream(target);
      expect(
        await _awaitConsumerNames(jetStream, origin, (names) => names.isEmpty),
        isEmpty,
      );
    },
    skip: skip,
    timeout: const Timeout(Duration(seconds: 20)),
  );
}

Future<JetStreamStreamInfo> _awaitStream(
  JetStreamContext jetStream,
  String name, {
  required BigInt messages,
}) async {
  final deadline = DateTime.now().add(const Duration(seconds: 10));
  JetStreamStreamInfo? last;
  while (DateTime.now().isBefore(deadline)) {
    last = await jetStream.streamInfo(name);
    if (last.messages == messages &&
        (last.sources.isEmpty ||
            last.sources.every((source) => source.lag == BigInt.zero))) {
      return last;
    }
    await Future<void>.delayed(const Duration(milliseconds: 20));
  }
  throw StateError(
    "Stream $name did not reach $messages messages with zero source lag. "
    "Last messages: ${last?.messages}. Last sources: ${last?.sources.length}",
  );
}

Future<List<String>> _awaitConsumerNames(
  JetStreamContext jetStream,
  String stream,
  bool Function(List<String> names) matches,
) async {
  final deadline = DateTime.now().add(const Duration(seconds: 10));
  var last = const <String>[];
  while (DateTime.now().isBefore(deadline)) {
    last = await jetStream.consumers.names(stream).toList();
    if (matches(last)) return last;
    await Future<void>.delayed(const Duration(milliseconds: 25));
  }
  throw StateError(
    "Stream $stream consumers did not reach the expected state. "
    "Last consumers: $last",
  );
}
