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
    "pull fetch acknowledgements and continuous delivery round trip",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final streamName = "D$suffix";
      final subject = "delivery.$suffix.messages";
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(serverUrl!)]),
      );
      final jetStream = JetStreamContext(connection);
      var streamExists = false;
      addTearDown(() async {
        if (streamExists) {
          try {
            await jetStream.deleteStream(streamName);
          } on JetStreamApiException {
            // Cleanup follows the test owned stream state.
          }
        }
        await connection.close();
      });
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: streamName,
          subjects: [subject],
          storage: JetStreamStorage.memory,
        ),
      );
      streamExists = true;
      await jetStream.publish(subject, const [0, 255, 13, 10]);
      await jetStream.publish(subject, const [1]);

      final pullName = "P$suffix";
      final createdPull = await jetStream.consumers.createPull(
        streamName,
        JetStreamPullConsumerConfig(
          name: pullName,
          filters: JetStreamConsumerFilters.single(subject),
          maximumAckPending: -1,
          maximumBatch: 32,
          samplePercentage: 42,
          metadata: const {"owner": "nats.dart", "kind": "pull"},
          memoryStorage: true,
          replicas: 1,
        ),
      );
      expect(createdPull.name, pullName);
      expect(createdPull.config.kind, JetStreamConsumerKind.pull);
      expect(createdPull.config.filterSubjects, [subject]);
      expect(createdPull.config.maximumAckPending, -1);
      expect(createdPull.config.sampleFrequency, "42%");
      expect(createdPull.config.metadata, containsPair("owner", "nats.dart"));
      expect(createdPull.config.metadata, containsPair("kind", "pull"));
      expect(createdPull.config.metadata, contains("_nats.ver"));
      expect(createdPull.config.memoryStorage, isTrue);
      expect(createdPull.config.replicas, 1);
      expect(createdPull.timestamp, isNotEmpty);
      expect(createdPull.raw["config"], isA<Map<String, Object?>>());

      final updatedPull = await jetStream.consumers.updatePull(
        streamName,
        JetStreamPullConsumerConfig(
          name: pullName,
          description: "updated",
          filters: JetStreamConsumerFilters.single(subject),
          maximumAckPending: 20,
          maximumBatch: 32,
          samplePercentage: 42,
          metadata: const {"owner": "nats.dart", "kind": "pull"},
          memoryStorage: true,
          replicas: 1,
        ),
      );
      expect(updatedPull.config.maximumAckPending, 20);
      expect(
        (await jetStream.consumers.info(streamName, pullName)).name,
        pullName,
      );

      final pull = await jetStream.consumers.pull(streamName, pullName);
      final fetched = await pull.fetch(
        JetStreamFetchRequest(
          maximumMessages: 2,
          expires: const Duration(seconds: 2),
          idleHeartbeat: const Duration(milliseconds: 200),
        ),
      );
      expect(fetched.completion, JetStreamFetchCompletion.batchComplete);
      expect(fetched.messages, hasLength(2));
      expect(fetched.messages.first.message.payload, const [0, 255, 13, 10]);
      expect(fetched.messages.last.message.payload, const [1]);
      expect(fetched.messages.first.metadata?.version, 1);
      await fetched.messages.first.acknowledger!.ack();
      await fetched.messages.last.acknowledger!.ackConfirmed();

      final emptyFetch = await pull.fetch(
        JetStreamFetchRequest(maximumMessages: 1, noWait: true),
      );
      expect(emptyFetch.messages, isEmpty);
      expect(emptyFetch.completion, JetStreamFetchCompletion.noMessages);
      final reset = await jetStream.consumers.resetToSequence(
        streamName,
        pullName,
        BigInt.one,
      );
      expect(reset.resetSequence, BigInt.one);
      expect(reset.info.delivered.consumer, BigInt.zero);
      final resetFirst = await pull.fetch(
        JetStreamFetchRequest(
          maximumMessages: 1,
          expires: const Duration(seconds: 2),
        ),
      );
      expect(resetFirst.messages.single.metadata?.streamSequence, BigInt.one);
      await resetFirst.messages.single.acknowledger!.inProgress();
      await resetFirst.messages.single.acknowledger!.nak();
      final redelivered = await pull.fetch(
        JetStreamFetchRequest(
          maximumMessages: 1,
          expires: const Duration(seconds: 2),
        ),
      );
      expect(redelivered.messages.single.metadata?.streamSequence, BigInt.one);
      expect(redelivered.messages.single.metadata?.delivered, BigInt.two);
      await redelivered.messages.single.acknowledger!.termWithReason(
        "invalid payload",
      );
      final afterTerm = await pull.fetch(
        JetStreamFetchRequest(
          maximumMessages: 1,
          expires: const Duration(seconds: 2),
        ),
      );
      expect(afterTerm.messages.single.metadata?.streamSequence, BigInt.two);
      await afterTerm.messages.single.acknowledger!.ackConfirmed();

      final continuousName = "N$suffix";
      await jetStream.consumers.createPull(
        streamName,
        JetStreamPullConsumerConfig(
          name: continuousName,
          filters: JetStreamConsumerFilters.single(subject),
        ),
      );
      final continuousPull = await jetStream.consumers.pull(
        streamName,
        continuousName,
      );
      final continuous = continuousPull.consume(
        options: JetStreamConsumeOptions(
          batchSize: 1,
          expires: const Duration(seconds: 2),
          idleHeartbeat: const Duration(milliseconds: 200),
        ),
      );
      final continuousMessages = StreamIterator(continuous.messages);
      expect(await continuousMessages.moveNext(), isTrue);
      expect(continuousMessages.current.message.payload, [0, 255, 13, 10]);
      await continuousMessages.current.acknowledger!.ackConfirmed();
      expect(await continuousMessages.moveNext(), isTrue);
      expect(continuousMessages.current.message.payload, [1]);
      await continuousMessages.current.acknowledger!.ackConfirmed();
      await continuous.drain(timeout: const Duration(seconds: 2));
      expect(await continuousMessages.moveNext(), isFalse);
      await continuousMessages.cancel();
      await continuous.done;

      final ackNoneName = "Z$suffix";
      await jetStream.consumers.createPull(
        streamName,
        JetStreamPullConsumerConfig(
          name: ackNoneName,
          filters: JetStreamConsumerFilters.single(subject),
          ackPolicy: JetStreamConsumerAckPolicy.none,
          maximumBatch: 32,
        ),
      );
      final ackNonePull = await jetStream.consumers.pull(
        streamName,
        ackNoneName,
      );
      final ackNoneFetched = await ackNonePull.fetch(
        JetStreamFetchRequest(
          maximumMessages: 1,
          expires: const Duration(seconds: 2),
        ),
      );
      expect(ackNoneFetched.messages.single.metadata, isNotNull);
      expect(ackNoneFetched.messages.single.acknowledger, isNull);

      final ackNoneContinuous = ackNonePull.consume(
        options: JetStreamConsumeOptions(
          batchSize: 1,
          expires: const Duration(seconds: 2),
          idleHeartbeat: const Duration(milliseconds: 200),
        ),
      );
      final ackNoneMessages = StreamIterator(ackNoneContinuous.messages);
      expect(await ackNoneMessages.moveNext(), isTrue);
      expect(ackNoneMessages.current.metadata, isNotNull);
      expect(ackNoneMessages.current.acknowledger, isNull);
      await ackNoneContinuous.stop();
      await ackNoneMessages.cancel();
      await ackNoneContinuous.done;

      await jetStream.consumers.delete(streamName, ackNoneName);
      await jetStream.consumers.createPull(
        streamName,
        JetStreamPullConsumerConfig(
          name: ackNoneName,
          filters: JetStreamConsumerFilters.single(subject),
          maximumBatch: 32,
        ),
      );
      final reboundExplicit = await ackNonePull.fetch(
        JetStreamFetchRequest(
          maximumMessages: 1,
          expires: const Duration(seconds: 2),
        ),
      );
      expect(reboundExplicit.messages.single.acknowledger, isNotNull);
      await reboundExplicit.messages.single.acknowledger!.ack();

      await jetStream.consumers.delete(streamName, ackNoneName);
      await jetStream.consumers.createPull(
        streamName,
        JetStreamPullConsumerConfig(
          name: ackNoneName,
          filters: JetStreamConsumerFilters.single(subject),
          ackPolicy: JetStreamConsumerAckPolicy.none,
          maximumBatch: 32,
        ),
      );
      final reboundNone = await ackNonePull.fetch(
        JetStreamFetchRequest(
          maximumMessages: 1,
          expires: const Duration(seconds: 2),
        ),
      );
      expect(reboundNone.messages.single.metadata, isNotNull);
      expect(reboundNone.messages.single.acknowledger, isNull);

      final overflowName = "L$suffix";
      await jetStream.consumers.createPull(
        streamName,
        JetStreamPullConsumerConfig(
          name: overflowName,
          filters: JetStreamConsumerFilters.single(subject),
        ),
      );
      final overflowPull = await jetStream.consumers.pull(
        streamName,
        overflowName,
      );
      final overflow = overflowPull.consume(
        options: JetStreamConsumeOptions(
          batchSize: 1,
          expires: const Duration(seconds: 2),
          idleHeartbeat: const Duration(milliseconds: 200),
          maximumPendingMessages: 1,
          maximumPendingBytes: 1,
        ),
      );
      final overflowStreamError = Completer<Object>();
      final overflowListener = overflow.messages.listen(
        (_) {},
        onError: (Object error, StackTrace stackTrace) {
          if (!overflowStreamError.isCompleted) {
            overflowStreamError.complete(error);
          }
        },
      );
      await expectLater(
        overflow.done,
        throwsA(
          isA<JetStreamConsumerStatusException>().having(
            (error) => error.code,
            "code",
            409,
          ),
        ),
      );
      expect(
        await overflowStreamError.future.timeout(const Duration(seconds: 2)),
        isA<JetStreamConsumerStatusException>().having(
          (error) => error.code,
          "code",
          409,
        ),
      );
      await overflowListener.cancel();
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}
