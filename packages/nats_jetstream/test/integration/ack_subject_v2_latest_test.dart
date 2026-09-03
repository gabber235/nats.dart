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
    "ACK subject v2 carries domain and account identity through delivery",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final streamName = "V$suffix";
      final consumerName = "C$suffix";
      final subject = "ack.v2.$suffix";
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
      await jetStream.publish(subject, const [1]);
      await jetStream.publish(subject, const [2]);
      await jetStream.consumers.createPull(
        streamName,
        JetStreamPullConsumerConfig(
          name: consumerName,
          filters: JetStreamConsumerFilters.single(subject),
        ),
      );

      final consumer = await jetStream.consumers.pull(streamName, consumerName);
      final fetched = await consumer.fetch(
        JetStreamFetchRequest(
          maximumMessages: 2,
          expires: const Duration(seconds: 2),
        ),
      );
      expect(fetched.messages, hasLength(2));

      final first = fetched.messages.first.metadata!;
      final second = fetched.messages.last.metadata!;
      expect(first.version, 2);
      expect(first.domain, "ACKV2");
      expect(first.accountHash, isNotNull);
      expect(first.accountHash, isNotEmpty);
      expect(first.accountHash, matches(RegExp(r"^[A-Za-z0-9_-]+$")));
      expect(second.accountHash, first.accountHash);
      expect(first.stream, streamName);
      expect(first.consumer, consumerName);
      expect(first.streamSequence, BigInt.one);
      expect(second.streamSequence, BigInt.two);
      expect(first.pending, BigInt.one);
      expect(second.pending, BigInt.zero);

      await fetched.messages.first.acknowledger!.ackConfirmed();
      await fetched.messages.last.acknowledger!.ackConfirmed();
      final acknowledged = await jetStream.consumers.info(
        streamName,
        consumerName,
      );
      expect(acknowledged.acknowledgementsPending, BigInt.zero);
      expect(acknowledged.acknowledgementFloor.stream, BigInt.two);
      expect(acknowledged.acknowledgementFloor.consumer, BigInt.two);
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 15)),
  );
}
