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
    "consumer variants, priorities, listing, pause, and deletion round trip",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final streamName = "M$suffix";
      final subject = "consumer.$suffix.messages";
      final scheduleName = "S$suffix";
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
          name: streamName,
          subjects: [subject],
          storage: JetStreamStorage.memory,
        ),
      );
      createdStreams.add(streamName);
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: scheduleName,
          subjects: ["consumer.$suffix.schedule.>"],
          storage: JetStreamStorage.memory,
          allowMessageSchedules: true,
        ),
      );
      createdStreams.add(scheduleName);
      await jetStream.publish(subject, const [0, 255, 13, 10]);
      await jetStream.publish(subject, const [1]);

      final pullName = "P$suffix";
      await jetStream.consumers.createPull(
        streamName,
        JetStreamPullConsumerConfig(
          name: pullName,
          filters: JetStreamConsumerFilters.single(subject),
        ),
      );
      final pushName = "Q$suffix";
      final push = await jetStream.consumers.createPush(
        streamName,
        JetStreamPushConsumerConfig(
          name: pushName,
          deliverSubject: "delivery.$suffix.push",
          filters: JetStreamConsumerFilters.single(subject),
          ackPolicy: JetStreamConsumerAckPolicy.none,
          rateLimitBitsPerSecond: BigInt.from(8000000),
          metadata: const {"kind": "push"},
          memoryStorage: true,
          replicas: 1,
        ),
      );
      expect(push.config.kind, JetStreamConsumerKind.push);
      expect(push.config.rateLimitBitsPerSecond, BigInt.from(8000000));
      expect(push.config.metadata, containsPair("kind", "push"));
      final updatedPush = await jetStream.consumers.updatePush(
        streamName,
        JetStreamPushConsumerConfig(
          name: pushName,
          deliverSubject: "delivery.$suffix.push",
          description: "updated push",
          filters: JetStreamConsumerFilters.single(subject),
          ackPolicy: JetStreamConsumerAckPolicy.none,
          rateLimitBitsPerSecond: BigInt.from(16000000),
          metadata: const {"kind": "push"},
          memoryStorage: true,
          replicas: 1,
        ),
      );
      expect(updatedPush.config.description, "updated push");
      expect(updatedPush.config.rateLimitBitsPerSecond, BigInt.from(16000000));
      final upsertedPush = await jetStream.consumers.createOrUpdatePush(
        streamName,
        JetStreamPushConsumerConfig(
          name: pushName,
          deliverSubject: "delivery.$suffix.push",
          description: "upserted push",
          filters: JetStreamConsumerFilters.single(subject),
          ackPolicy: JetStreamConsumerAckPolicy.none,
          rateLimitBitsPerSecond: BigInt.from(16000000),
          metadata: const {"kind": "push"},
          memoryStorage: true,
          replicas: 1,
        ),
      );
      expect(upsertedPush.config.description, "upserted push");

      final reliableName = "R$suffix";
      final reliable = await jetStream.consumers.createReliableSource(
        streamName,
        JetStreamReliableSourceConsumerConfig(
          name: reliableName,
          deliverSubject: "delivery.$suffix.source",
          maximumAckPending: 64,
          filters: JetStreamConsumerFilters.single(subject),
        ),
      );
      expect(reliable.config.kind, JetStreamConsumerKind.reliableSource);
      expect(reliable.config.ackPolicy, JetStreamConsumerAckPolicy.flowControl);
      final updatedReliable = await jetStream.consumers.updateReliableSource(
        streamName,
        JetStreamReliableSourceConsumerConfig(
          name: reliableName,
          deliverSubject: "delivery.$suffix.source",
          maximumAckPending: 64,
          filters: JetStreamConsumerFilters.single(subject),
          description: "updated source",
        ),
      );
      expect(updatedReliable.config.description, "updated source");
      final upsertedReliable = await jetStream.consumers
          .createOrUpdateReliableSource(
            streamName,
            JetStreamReliableSourceConsumerConfig(
              name: reliableName,
              deliverSubject: "delivery.$suffix.source",
              maximumAckPending: 64,
              filters: JetStreamConsumerFilters.single(subject),
              description: "upserted source",
            ),
          );
      expect(upsertedReliable.config.description, "upserted source");

      final multiName = "U$suffix";
      final multiFilters = [
        "consumer.$suffix.schedule.one",
        "consumer.$suffix.schedule.two",
      ];
      final multi = await jetStream.consumers.createPull(
        scheduleName,
        JetStreamPullConsumerConfig(
          name: multiName,
          filters: JetStreamConsumerFilters.multiple(multiFilters),
        ),
      );
      expect(multi.config.filterSubjects, multiFilters);

      final priorityName = "Y$suffix";
      final priority = await jetStream.consumers.createPull(
        streamName,
        JetStreamPullConsumerConfig(
          name: priorityName,
          priorityPolicy: JetStreamConsumerPriorityPolicy.pinnedClient,
          priorityGroups: const ["primary", "spill"],
          priorityTimeout: JetStreamNanoseconds.fromDuration(
            const Duration(seconds: 30),
          ),
        ),
      );
      expect(
        priority.config.priorityPolicy,
        JetStreamConsumerPriorityPolicy.pinnedClient,
      );
      expect(priority.config.priorityGroups, ["primary", "spill"]);
      expect(
        priority.config.priorityTimeout?.nanoseconds,
        BigInt.from(30000000000),
      );
      final pinnedPull = await jetStream.consumers.pull(
        streamName,
        priorityName,
      );
      final firstPinned = await pinnedPull.fetch(
        JetStreamFetchRequest(
          maximumMessages: 1,
          noWait: true,
          priority: JetStreamFetchPriority.pinned(group: "primary"),
        ),
      );
      expect(firstPinned.messages, hasLength(1));
      expect(firstPinned.pinId, isNotEmpty);
      expect(
        firstPinned.messages.single.message.headers?.first("Nats-Pin-Id"),
        firstPinned.pinId,
      );
      await firstPinned.messages.single.acknowledger!.ackConfirmed();
      final renewedPinned = await pinnedPull.fetch(
        JetStreamFetchRequest(
          maximumMessages: 1,
          noWait: true,
          priority: JetStreamFetchPriority.pinned(
            group: "primary",
            pinId: firstPinned.pinId,
          ),
        ),
      );
      expect(renewedPinned.pinId, firstPinned.pinId);
      await renewedPinned.messages.single.acknowledger!.ackConfirmed();
      await expectLater(
        pinnedPull.fetch(
          JetStreamFetchRequest(
            maximumMessages: 1,
            noWait: true,
            priority: JetStreamFetchPriority.pinned(
              group: "primary",
              pinId: "wrong-client",
            ),
          ),
        ),
        throwsA(
          isA<JetStreamFetchException>().having(
            (error) => error.cause,
            "cause",
            isA<JetStreamConsumerStatusException>().having(
              (error) => error.code,
              "code",
              423,
            ),
          ),
        ),
      );
      await jetStream.consumers.unpin(streamName, priorityName, "primary");
      final unpinned = await jetStream.consumers.info(streamName, priorityName);
      expect(unpinned.priorityGroups.single.group, "primary");
      expect(unpinned.priorityGroups.single.pinnedClientId, isNull);

      final prioritizedName = "Z$suffix";
      await jetStream.consumers.createPull(
        streamName,
        JetStreamPullConsumerConfig(
          name: prioritizedName,
          priorityPolicy: JetStreamConsumerPriorityPolicy.prioritized,
          priorityGroups: const ["urgent"],
        ),
      );
      final prioritizedPull = await jetStream.consumers.pull(
        streamName,
        prioritizedName,
      );
      final prioritized = await prioritizedPull.fetch(
        JetStreamFetchRequest(
          maximumMessages: 1,
          noWait: true,
          priority: JetStreamFetchPriority.prioritized(
            group: "urgent",
            priority: 0,
          ),
        ),
      );
      expect(prioritized.messages, hasLength(1));
      await prioritized.messages.single.acknowledger!.ackConfirmed();

      final overflowName = "O$suffix";
      await jetStream.consumers.createPull(
        streamName,
        JetStreamPullConsumerConfig(
          name: overflowName,
          priorityPolicy: JetStreamConsumerPriorityPolicy.overflow,
          priorityGroups: const ["spill"],
        ),
      );
      final overflowPull = await jetStream.consumers.pull(
        streamName,
        overflowName,
      );
      final overflow = await overflowPull.fetch(
        JetStreamFetchRequest(
          maximumMessages: 1,
          noWait: true,
          priority: JetStreamFetchPriority.overflow(
            group: "spill",
            minimumPending: BigInt.one,
          ),
        ),
      );
      expect(overflow.messages, hasLength(1));
      await overflow.messages.single.acknowledger!.ackConfirmed();

      final scheduleConsumers = await jetStream.consumers.listPage(
        scheduleName,
      );
      expect(scheduleConsumers.total, BigInt.one);
      expect(
        scheduleConsumers.consumers.map((consumer) => consumer.name),
        contains(multiName),
      );
      final consumerPage = await jetStream.consumers.listPage(streamName);
      expect(consumerPage.total, BigInt.from(6));
      expect(
        consumerPage.consumers.map((consumer) => consumer.name),
        containsAll([
          pullName,
          pushName,
          reliableName,
          priorityName,
          prioritizedName,
          overflowName,
        ]),
      );
      final consumerNamePage = await jetStream.consumers.namesPage(streamName);
      expect(consumerNamePage.total, BigInt.from(6));
      expect(
        consumerNamePage.names,
        containsAll([
          pullName,
          pushName,
          reliableName,
          priorityName,
          prioritizedName,
          overflowName,
        ]),
      );
      expect(
        await jetStream.consumers.names(streamName).toList(),
        containsAll(consumerNamePage.names),
      );

      final pause = await jetStream.consumers.pause(
        streamName,
        pullName,
        DateTime.now().toUtc().add(const Duration(minutes: 1)),
      );
      expect(pause.paused, isTrue);
      expect(pause.pauseRemaining?.nanoseconds, greaterThan(BigInt.zero));
      expect(
        (await jetStream.consumers.info(streamName, pullName)).paused,
        isTrue,
      );
      final resume = await jetStream.consumers.resume(streamName, pullName);
      expect(resume.paused, isFalse);
      expect(
        (await jetStream.consumers.info(streamName, pullName)).paused,
        isFalse,
      );

      final continuousName = "N$suffix";
      final overflowContinuousName = "L$suffix";
      for (final name in [continuousName, overflowContinuousName]) {
        await jetStream.consumers.createPull(
          streamName,
          JetStreamPullConsumerConfig(
            name: name,
            filters: JetStreamConsumerFilters.single(subject),
          ),
        );
      }
      for (final name in [
        reliableName,
        pushName,
        pullName,
        priorityName,
        prioritizedName,
        overflowName,
        continuousName,
        overflowContinuousName,
      ]) {
        await jetStream.consumers.delete(streamName, name);
      }
      await jetStream.consumers.delete(scheduleName, multiName);
      expect(
        (await jetStream.consumers.listPage(streamName)).total,
        BigInt.zero,
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}
