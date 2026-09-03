@TestOn("vm")
library;

import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  final serverUrl = Platform.environment["NATS_URL"];
  final userJwtPath = Platform.environment["NATS_TIER_USER_JWT_FILE"];
  final skipReason = serverUrl == null || userJwtPath == null
      ? "Tiered NATS fixture is not configured"
      : false;

  test(
    "named R1 account tier reports limits before usage and tracks storage",
    () async {
      final userJwt = await File(userJwtPath!).readAsString();
      final connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(serverUrl!)],
          authentication: NatsAuthentication.provider(
            (_) async => NatsConnectCredentials(jwt: userJwt),
          ),
        ),
      );
      final jetStream = JetStreamContext(connection);
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final streamName = "R$suffix";
      final subject = "tier.$suffix";
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

      final before = await jetStream.accountInfo();
      expect(before.tiers, contains("R1"));
      final beforeTier = before.tiers["R1"]!;
      expect(beforeTier.streams, BigInt.zero);
      expect(beforeTier.consumers, BigInt.zero);
      expect(beforeTier.memoryBytes, BigInt.zero);
      expect(beforeTier.storageBytes, BigInt.zero);
      expect(beforeTier.limits.maximumMemoryBytes, BigInt.from(8000000));
      expect(beforeTier.limits.maximumStorageBytes, BigInt.from(16000000));
      expect(beforeTier.limits.maximumStreams, BigInt.from(3));
      expect(beforeTier.limits.maximumConsumers, BigInt.from(4));
      expect(beforeTier.limits.maximumAcknowledgementsPending, BigInt.from(32));
      expect(beforeTier.limits.maximumMemoryStreamBytes, BigInt.from(4000000));
      expect(beforeTier.limits.maximumStorageStreamBytes, BigInt.from(6000000));
      expect(beforeTier.limits.maximumBytesRequired, isFalse);

      await jetStream.createStream(
        JetStreamStreamConfig(
          name: streamName,
          subjects: [subject],
          maximumBytes: BigInt.from(1000000),
          storage: JetStreamStorage.memory,
        ),
      );
      streamExists = true;
      await jetStream.publish(subject, const [1, 2, 3, 4]);

      final after = await jetStream.accountInfo();
      expect(after.tiers, contains("R1"));
      final afterTier = after.tiers["R1"]!;
      expect(afterTier.streams, BigInt.one);
      expect(afterTier.memoryBytes, greaterThan(BigInt.zero));
      expect(afterTier.storageBytes, BigInt.zero);
      expect(afterTier.reservedMemoryBytes, BigInt.from(1000000));
      expect(afterTier.reservedStorageBytes, BigInt.zero);
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 15)),
  );
}
