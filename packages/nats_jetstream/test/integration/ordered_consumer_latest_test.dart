import "dart:async";
import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  final server = Platform.environment["NATS_URL"];
  final skip = server == null ? "NATS_URL is not configured" : false;

  test(
    "ordered delivery recreates a deleted consumer without duplicates",
    () async {
      final connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(server!)],
          name: "nats-dart-ordered-consumer",
        ),
      );
      final jetStream = JetStreamContext(connection);
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final streamName = "ORDERED$suffix";
      final subject = "ordered.$suffix.events";
      JetStreamOrderedConsumerStream? ordered;
      StreamIterator<JetStreamDelivery>? messages;
      addTearDown(() async {
        await ordered?.stop();
        await messages?.cancel();
        try {
          await jetStream.deleteStream(streamName);
        } on JetStreamApiException {
          // The test may fail before stream creation.
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
      await jetStream.publish(subject, const [1]);

      ordered = jetStream.consumers
          .ordered(streamName)
          .consume(
            options: JetStreamConsumeOptions(
              batchSize: 1,
              expires: const Duration(seconds: 2),
              idleHeartbeat: const Duration(milliseconds: 250),
              maximumPendingMessages: 2,
              maximumPendingBytes: 1024,
            ),
          );
      final created = Completer<JetStreamOrderedConsumerCreated>();
      final recreated = Completer<JetStreamOrderedConsumerRecreated>();
      final diagnostics = <JetStreamOrderedConsumerEvent>[];
      final eventSubscription = ordered.events.listen((event) {
        diagnostics.add(event);
        if (event is JetStreamOrderedConsumerCreated && !created.isCompleted) {
          created.complete(event);
        }
        if (event is JetStreamOrderedConsumerRecreated &&
            !recreated.isCompleted) {
          recreated.complete(event);
        }
      });
      addTearDown(eventSubscription.cancel);
      messages = StreamIterator(ordered.messages);

      try {
        expect(await messages.moveNext(), isTrue);
      } on JetStreamConsumerStatusException catch (error) {
        fail("Raw $error escaped before the first ordered delivery");
      } on NatsSlowConsumerException catch (error) {
        fail("Ordered delivery overflowed before its first message: $error");
      }
      expect(messages.current.message.payload, [1]);
      expect(messages.current.acknowledger, isNull);
      final initial = await created.future.timeout(const Duration(seconds: 5));
      final hidden = await jetStream.consumers.info(
        streamName,
        initial.consumer,
      );
      expect(hidden.config.durableName, isNull);
      expect(hidden.config.ackPolicy, JetStreamConsumerAckPolicy.none);
      expect(hidden.config.memoryStorage, isTrue);
      expect(hidden.config.replicas, 1);
      expect(
        hidden.config.metadata,
        containsPair("nats.dart.owner", "ordered"),
      );

      await jetStream.publish(subject, const [2]);
      final paused = await jetStream.consumers.info(
        streamName,
        initial.consumer,
      );
      expect(paused.waiting, BigInt.zero);
      expect(paused.pending, BigInt.one);
      for (var value = 3; value <= 6; value += 1) {
        await jetStream.publish(subject, [value]);
      }
      await jetStream.deleteMessage(streamName, BigInt.from(4));

      await jetStream.consumers.delete(streamName, initial.consumer);

      final received = <int>[1];
      try {
        while (received.length < 5) {
          final available = await messages.moveNext().timeout(
            const Duration(seconds: 5),
            onTimeout: () => fail(
              "Ordered delivery stalled after $received with $diagnostics",
            ),
          );
          expect(available, isTrue);
          final delivery = messages.current;
          expect(delivery.acknowledger, isNull);
          received.add(delivery.message.payload.single);
        }
      } on JetStreamConsumerStatusException catch (error) {
        fail("Raw $error escaped ordered messages after consumer deletion");
      } on NatsSlowConsumerException catch (error) {
        fail("Ordered delivery overflowed after $received: $error");
      }
      expect(received, [1, 2, 3, 5, 6]);
      final replacement = await recreated.future.timeout(
        const Duration(seconds: 5),
      );
      expect(replacement.previousConsumer, initial.consumer);
      expect(replacement.consumer, isNot(initial.consumer));
      expect(
        replacement.reason,
        anyOf(
          JetStreamOrderedConsumerRecreationReason.consumerDeleted,
          JetStreamOrderedConsumerRecreationReason.noResponders,
        ),
      );
      expect(replacement.nextStreamSequence, greaterThan(BigInt.one));
      expect(
        diagnostics.whereType<JetStreamOrderedConsumerGap>(),
        contains(
          isA<JetStreamOrderedConsumerGap>()
              .having(
                (gap) => gap.kind,
                "kind",
                JetStreamOrderedConsumerGapKind.streamForward,
              )
              .having((gap) => gap.causesRecreation, "recreation", isFalse),
        ),
      );

      try {
        await ordered.stop().timeout(const Duration(seconds: 5));
      } on JetStreamConsumerStatusException catch (error) {
        fail("Raw $error escaped ordered stop after consumer recreation");
      }
      await messages.cancel();
      await ordered.done;
      ordered = null;
      messages = null;
      expect(
        (await jetStream.consumers.listPage(streamName)).total,
        BigInt.zero,
      );
    },
    skip: skip,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}
