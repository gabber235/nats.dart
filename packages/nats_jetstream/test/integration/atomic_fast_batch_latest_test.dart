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
    "atomic and fast batch publication round trip",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final atomicName = "A$suffix";
      final atomicSubject = "batch.$suffix.atomic";
      final fastName = "F$suffix";
      final fastSubject = "batch.$suffix.fast";
      final disabledName = "D$suffix";
      final disabledSubject = "batch.$suffix.disabled";
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
      for (final config in [
        JetStreamStreamConfig(
          name: atomicName,
          subjects: [atomicSubject],
          storage: JetStreamStorage.memory,
          allowAtomicPublish: true,
        ),
        JetStreamStreamConfig(
          name: fastName,
          subjects: [fastSubject],
          storage: JetStreamStorage.memory,
          allowFastBatchPublish: true,
        ),
        JetStreamStreamConfig(
          name: disabledName,
          subjects: [disabledSubject],
          storage: JetStreamStorage.memory,
        ),
      ]) {
        await jetStream.createStream(config);
        createdStreams.add(config.name);
      }

      final beforeAtomic = await jetStream.streamInfo(atomicName);
      final atomicAck = await jetStream.publishAtomicBatch([
        JetStreamAtomicMessage(atomicSubject, const [10]),
        JetStreamAtomicMessage(atomicSubject, const [11]),
        JetStreamAtomicMessage(atomicSubject, const [12]),
      ], batchId: "valid-$suffix");
      expect(atomicAck.stream, atomicName);
      expect(atomicAck.batchId, "valid-$suffix");
      expect(atomicAck.messageCount, BigInt.from(3));
      expect(atomicAck.sequence, beforeAtomic.lastSequence + BigInt.from(3));
      final afterAtomic = await jetStream.streamInfo(atomicName);
      expect(afterAtomic.messages, beforeAtomic.messages + BigInt.from(3));

      expect(
        () => jetStream.publishAtomicBatch([
          JetStreamAtomicMessage(
            atomicSubject,
            const [13],
            headers: NatsHeaders(
              entries: const [MapEntry("Nats-Batch-Commit", "1")],
            ),
          ),
        ], batchId: "caller-commit-$suffix"),
        throwsArgumentError,
      );
      expect(
        (await jetStream.streamInfo(atomicName)).messages,
        afterAtomic.messages,
      );

      final beforeDuplicateBatch = afterAtomic.messages;
      await expectLater(
        jetStream.publishAtomicBatch([
          JetStreamAtomicMessage(atomicSubject, const [
            20,
          ], options: JetStreamPublishOptions(messageId: "duplicate-$suffix")),
          JetStreamAtomicMessage(atomicSubject, const [
            21,
          ], options: JetStreamPublishOptions(messageId: "duplicate-$suffix")),
        ], batchId: "duplicate-$suffix"),
        throwsA(
          isA<JetStreamApiException>().having(
            (error) => error.errorCode,
            "errorCode",
            10201,
          ),
        ),
      );
      expect(
        (await jetStream.streamInfo(atomicName)).messages,
        beforeDuplicateBatch,
      );

      final beforeFast = await jetStream.streamInfo(fastName);
      final fastMessages = List.generate(
        40,
        (index) =>
            JetStreamFastBatchMessage(fastSubject, [index, 0, 255, 13, 10]),
      );
      final fastAck = await jetStream.publishFastBatchMessages(
        fastMessages,
        options: JetStreamFastBatchOptions(
          batchId: "fast-$suffix",
          initialFlow: 2,
          maximumOutstandingBytes: 256,
        ),
      );
      expect(fastAck.stream, fastName);
      expect(fastAck.batchId, "fast-$suffix");
      expect(fastAck.messageCount, BigInt.from(fastMessages.length));
      expect(fastAck.gaps, isEmpty);
      expect(fastAck.errors, isEmpty);
      final afterFast = await jetStream.streamInfo(fastName);
      expect(
        afterFast.messages,
        beforeFast.messages + BigInt.from(fastMessages.length),
      );
      final firstFast = await jetStream.getMessage(
        fastName,
        beforeFast.lastSequence + BigInt.one,
      );
      final lastFast = await jetStream.getMessage(
        fastName,
        afterFast.lastSequence,
      );
      expect(firstFast.payload, [0, 0, 255, 13, 10]);
      expect(lastFast.payload, [39, 0, 255, 13, 10]);

      final singleFast = await jetStream.publishFastBatchMessages([
        JetStreamFastBatchMessage(fastSubject, const [77]),
      ], options: JetStreamFastBatchOptions(batchId: "single-$suffix"));
      expect(singleFast.messageCount, BigInt.one);
      final endOfBatch = await jetStream.publishFastBatchMessages(
        [
          JetStreamFastBatchMessage(fastSubject, const [81]),
          JetStreamFastBatchMessage(fastSubject, const [82]),
          JetStreamFastBatchMessage(fastSubject, const [83]),
        ],
        options: JetStreamFastBatchOptions(
          batchId: "eob-$suffix",
          initialFlow: 1,
          commitStrategy: JetStreamFastBatchCommitStrategy.endOfBatch,
        ),
      );
      expect(endOfBatch.messageCount, BigInt.from(3));
      expect(
        (await jetStream.streamInfo(fastName)).messages,
        afterFast.messages + BigInt.from(4),
      );

      final beforeUnlimited = await jetStream.streamInfo(fastName);
      final unlimitedFast = await jetStream.publishFastBatchMessages(
        List.generate(
          1001,
          (index) => JetStreamFastBatchMessage(fastSubject, [index & 255]),
        ),
        options: JetStreamFastBatchOptions(
          batchId: "unlimited-$suffix",
          initialFlow: 500,
        ),
      );
      expect(unlimitedFast.messageCount, BigInt.from(1001));
      expect(
        (await jetStream.streamInfo(fastName)).messages,
        beforeUnlimited.messages + BigInt.from(1001),
      );
      final beforeDisabled = await jetStream.streamInfo(disabledName);
      await expectLater(
        jetStream.publishFastBatchMessages([
          JetStreamFastBatchMessage(disabledSubject, const [1]),
          JetStreamFastBatchMessage(disabledSubject, const [2]),
        ], options: JetStreamFastBatchOptions(batchId: "disabled-$suffix")),
        throwsA(
          isA<JetStreamFastBatchException>()
              .having((error) => error.ambiguous, "ambiguous", isFalse)
              .having((error) => error.sentSequence, "sentSequence", BigInt.one)
              .having(
                (error) => error.cause,
                "cause",
                isA<JetStreamApiException>().having(
                  (error) => error.errorCode,
                  "errorCode",
                  10205,
                ),
              ),
        ),
      );
      expect(
        (await jetStream.streamInfo(disabledName)).messages,
        beforeDisabled.messages,
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "atomic inactivity timeout emits typed noncommit proof",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final streamName = "T$suffix";
      final subject = "batch.$suffix.timeout";
      final batchId = "timeout-$suffix";
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(serverUrl!)]),
      );
      final jetStream = JetStreamContext(connection);
      final watcher = await jetStream.watchAtomicBatchAbandonments(
        stream: streamName,
      );
      final advisories = StreamIterator(watcher.advisories);
      addTearDown(() async {
        await advisories.cancel();
        await watcher.close();
        try {
          await jetStream.deleteStream(streamName);
        } on JetStreamApiException {
          // The test may fail before stream creation completes.
        }
        await connection.close();
      });
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: streamName,
          subjects: [subject],
          storage: JetStreamStorage.memory,
          allowAtomicPublish: true,
        ),
      );

      final pendingAdvisory = advisories.moveNext();
      final stagingAck = await connection.request(
        subject,
        const [1],
        headers: NatsHeaders(
          entries: [
            MapEntry("Nats-Batch-Id", batchId),
            const MapEntry("Nats-Batch-Sequence", "1"),
            const MapEntry("Nats-Required-Api-Level", "4"),
          ],
        ),
      );

      expect(stagingAck.payload, isEmpty);
      expect(
        await pendingAdvisory.timeout(const Duration(seconds: 15)),
        isTrue,
      );
      final advisory = advisories.current;
      expect(advisory.stream, streamName);
      expect(advisory.batchId, batchId);
      expect(
        advisory.reason.knownKind,
        JetStreamAtomicBatchAbandonKind.timeout,
      );
      expect((await jetStream.streamInfo(streamName)).messages, BigInt.zero);
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 25)),
  );
}
