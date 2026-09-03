@TestOn("vm")
library;

import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  final serverUrl = Platform.environment["NATS_URL"];
  final skipReason = serverUrl == null ? "NATS_URL is not configured" : false;

  test(
    "ordinary publish deduplication and counters round trip",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final ordinaryName = "P$suffix";
      final ordinarySubject = "publish.$suffix.ordinary";
      final counterName = "C$suffix";
      final counterSubject = "publish.$suffix.counter";
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(serverUrl!)]),
      );
      final jetStream = JetStreamContext(connection);
      final createdStreams = <String>[];
      addTearDown(() async {
        for (final name in createdStreams.reversed) {
          try {
            await jetStream.deleteStream(name);
          } on JetStreamApiException {
            continue;
          }
        }
        await connection.close();
      });
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: ordinaryName,
          subjects: [ordinarySubject],
          storage: JetStreamStorage.memory,
        ),
      );
      createdStreams.add(ordinaryName);
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: counterName,
          subjects: [counterSubject],
          storage: JetStreamStorage.memory,
          allowMessageCounter: true,
        ),
      );
      createdStreams.add(counterName);

      final acknowledgement = await jetStream.publish(ordinarySubject, const [
        0,
        255,
        13,
        10,
      ]);
      expect(acknowledgement.stream, ordinaryName);
      expect(acknowledgement.sequence, BigInt.one);
      expect(acknowledgement.duplicate, isFalse);
      final identified = await jetStream.publish(
        ordinarySubject,
        const [1],
        options: JetStreamPublishOptions(
          messageId: "message-$suffix",
          expectedStream: ordinaryName,
          expectedLastSequence: BigInt.one,
        ),
      );
      expect(identified.sequence, BigInt.two);
      expect(identified.duplicate, isFalse);
      final duplicate = await jetStream.publish(ordinarySubject, const [
        1,
      ], options: JetStreamPublishOptions(messageId: "message-$suffix"));
      expect(duplicate.sequence, identified.sequence);
      expect(duplicate.duplicate, isTrue);
      expect((await jetStream.streamInfo(ordinaryName)).messages, BigInt.two);

      await expectLater(
        jetStream.publish(
          ordinarySubject,
          const [2],
          options: JetStreamPublishOptions(
            expectedLastSequence: BigInt.from(999),
          ),
        ),
        throwsA(
          isA<JetStreamApiException>()
              .having((error) => error.code, "code", 400)
              .having((error) => error.errorCode, "errorCode", greaterThan(0)),
        ),
      );

      final firstIncrement = BigInt.parse("9223372036854775808");
      final firstCounter = await jetStream.incrementCounter(
        counterSubject,
        firstIncrement,
      );
      expect(firstCounter.stream, counterName);
      expect(firstCounter.sequence, BigInt.one);
      expect(firstCounter.value, firstIncrement);
      final secondCounter = await jetStream.incrementCounter(
        counterSubject,
        BigInt.parse("-9223372036854775809"),
      );
      expect(secondCounter.sequence, BigInt.two);
      expect(secondCounter.value, BigInt.from(-1));
      final zeroCounter = await jetStream.incrementCounter(
        counterSubject,
        BigInt.zero,
      );
      expect(zeroCounter.sequence, BigInt.from(3));
      expect(zeroCounter.value, BigInt.from(-1));
      final large = BigInt.parse("100000000000000000000");
      final largeCounter = await jetStream.incrementCounter(
        counterSubject,
        large,
      );
      expect(largeCounter.sequence, BigInt.from(4));
      expect(largeCounter.value, large - BigInt.one);
      final restoredCounter = await jetStream.incrementCounter(
        counterSubject,
        -large,
      );
      expect(restoredCounter.sequence, BigInt.from(5));
      expect(restoredCounter.value, BigInt.from(-1));
      await expectLater(
        jetStream.publish(counterSubject, const []),
        throwsA(isA<JetStreamApiException>()),
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 15)),
  );
}
