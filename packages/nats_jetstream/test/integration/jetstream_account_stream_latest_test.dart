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
    "account information and latest stream flags round trip",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
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

      await jetStream.accountInfo();
      final account = await jetStream.accountInfo();
      expect(account.apiLevel, greaterThanOrEqualTo(4));
      expect(account.api.total, greaterThan(BigInt.zero));
      expect(account.api.errors, greaterThanOrEqualTo(BigInt.zero));
      expect(account.api.inflight, greaterThanOrEqualTo(BigInt.zero));
      expect(account.reservedMemoryBytes, account.raw["reserved_memory"]);
      expect(account.reservedStorageBytes, account.raw["reserved_storage"]);
      expect(
        account.limits.maximumBytesRequired,
        account.limits.raw["max_bytes_required"],
      );
      expect(account.api.total, account.api.raw["total"]);
      expect(account.api.errors, account.api.raw["errors"]);
      expect(account.api.inflight, account.api.raw["inflight"] ?? BigInt.zero);
      for (final entry in account.tiers.entries) {
        expect(entry.value.memoryBytes, entry.value.raw["memory"]);
        expect(entry.value.storageBytes, entry.value.raw["storage"]);
        expect(
          entry.value.limits.maximumBytesRequired,
          entry.value.limits.raw["max_bytes_required"],
        );
      }

      final configurations = [
        JetStreamStreamConfig(
          name: "A$suffix",
          subjects: ["flags.$suffix.atomic"],
          storage: JetStreamStorage.memory,
          allowAtomicPublish: true,
        ),
        JetStreamStreamConfig(
          name: "B$suffix",
          subjects: ["flags.$suffix.batch"],
          storage: JetStreamStorage.memory,
          allowFastBatchPublish: true,
        ),
        JetStreamStreamConfig(
          name: "S$suffix",
          subjects: ["flags.$suffix.schedule.>"],
          allowMessageSchedules: true,
        ),
        JetStreamStreamConfig(
          name: "C$suffix",
          subjects: ["flags.$suffix.counter"],
          storage: JetStreamStorage.memory,
          allowMessageCounter: true,
        ),
        JetStreamStreamConfig(
          name: "T$suffix",
          subjects: ["flags.$suffix.ttl"],
          storage: JetStreamStorage.memory,
          allowMessageTtl: true,
          subjectDeleteMarkerTtl: JetStreamNanoseconds.fromDuration(
            const Duration(minutes: 1),
          ),
        ),
        JetStreamStreamConfig(
          name: "P$suffix",
          subjects: ["flags.$suffix.persist"],
          persistMode: JetStreamPersistMode.asynchronous,
        ),
      ];

      for (final configuration in configurations) {
        final created = await jetStream.createStream(configuration);
        createdStreams.add(configuration.name);
        expect(
          created.config.allowAtomicPublish,
          configuration.allowAtomicPublish,
        );
        expect(
          created.config.allowFastBatchPublish,
          configuration.allowFastBatchPublish,
        );
        expect(
          created.config.allowMessageSchedules,
          configuration.allowMessageSchedules,
        );
        expect(
          created.config.allowMessageCounter,
          configuration.allowMessageCounter,
        );
        expect(created.config.allowMessageTtl, configuration.allowMessageTtl);
        expect(
          created.config.raw["persist_mode"],
          configuration.raw["persist_mode"],
        );
        if (configuration.raw["persist_mode"] == "async") {
          final acknowledgement = await jetStream.publish(
            configuration.subjects.single,
            const [1, 2, 3],
          );
          expect(acknowledgement.stream, configuration.name);
          expect(acknowledgement.sequence, BigInt.one);
        }
      }

      await expectLater(
        jetStream.streamInfo("MISSING$suffix"),
        throwsA(
          isA<JetStreamApiException>().having(
            (error) => error.errorCode,
            "errorCode",
            10059,
          ),
        ),
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 15)),
  );
}
