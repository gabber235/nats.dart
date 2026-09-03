@TestOn("vm")
library;

import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  final serverUrl = Platform.environment["NATS_URL"];
  final expectedApiLevel = int.tryParse(
    Platform.environment["NATS_EXPECTED_API_LEVEL"] ?? "",
  );
  final skipReason = serverUrl == null ? "NATS_URL is not configured" : false;

  test(
    "baseline stream, publish, pull, acknowledge, and cleanup round trip",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final stream = "COMPAT$suffix";
      final subject = "compat.$suffix.events";
      final consumer = "WORKER";
      final connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(serverUrl!)],
          name: "nats-dart-compatibility-$suffix",
        ),
      );
      final jetStream = JetStreamContext(connection);
      addTearDown(() async {
        try {
          await jetStream.deleteStream(stream);
        } on JetStreamApiException {
          // Cleanup is idempotent after a partial test failure.
        }
        await connection.close();
      });

      final account = await jetStream.accountInfo();
      if (expectedApiLevel case final value?) {
        expect(account.apiLevel, value);
      } else {
        expect(account.apiLevel, greaterThanOrEqualTo(0));
      }
      final created = await jetStream.createStream(
        JetStreamStreamConfig(
          name: stream,
          subjects: [subject],
          storage: JetStreamStorage.memory,
        ),
      );
      expect(created.config.name, stream);
      expect(created.config.subjects, [subject]);

      final acknowledgement = await jetStream.publish(subject, const [
        0,
        255,
        13,
        10,
      ], options: JetStreamPublishOptions(expectedStream: stream));
      expect(acknowledgement.stream, stream);
      expect(acknowledgement.sequence, BigInt.one);

      await jetStream.consumers.createPull(
        stream,
        JetStreamPullConsumerConfig(name: consumer),
      );
      final pull = await jetStream.consumers.pull(stream, consumer);
      final fetched = await pull.fetch(
        JetStreamFetchRequest(
          maximumMessages: 1,
          expires: const Duration(seconds: 2),
        ),
      );
      expect(fetched.messages, hasLength(1));
      expect(fetched.messages.single.message.payload, [0, 255, 13, 10]);
      await fetched.messages.single.acknowledger!.ackConfirmed();
      expect((await jetStream.streamInfo(stream)).state.messages, BigInt.one);
      await jetStream.consumers.delete(stream, consumer);
      await jetStream.deleteStream(stream);
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}
