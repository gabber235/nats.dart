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
    "message TTL and subject sequence constraints round trip",
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

      final ttlName = "TTL$suffix";
      final ttlPrefix = "ttl.$suffix";
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: ttlName,
          subjects: ["$ttlPrefix.>"],
          storage: JetStreamStorage.memory,
          maximumAge: JetStreamNanoseconds.fromDuration(
            const Duration(seconds: 2),
          ),
          allowMessageTtl: true,
        ),
      );
      createdStreams.add(ttlName);

      final expires = await jetStream.publish(
        "$ttlPrefix.expires",
        const [1],
        options: JetStreamPublishOptions(
          lifetime: JetStreamMessageLifetime.expiresAfter(
            const Duration(seconds: 1),
          ),
        ),
      );
      final never = await jetStream.publish(
        "$ttlPrefix.never",
        const [2],
        options: JetStreamPublishOptions(
          lifetime: const JetStreamMessageLifetime.never(),
        ),
      );
      expect(expires.sequence, BigInt.one);
      expect(never.sequence, BigInt.two);

      await Stream<int>.periodic(
            const Duration(milliseconds: 50),
            (index) => index,
          )
          .asyncMap((_) => jetStream.streamInfo(ttlName))
          .firstWhere((state) => state.messages == BigInt.one)
          .timeout(const Duration(seconds: 5));
      final retained = await jetStream.getMessage(ttlName, never.sequence);
      expect(retained.subject, "$ttlPrefix.never");
      expect(retained.payload, const [2]);

      final oldestAllowedCheck = DateTime.timestamp().add(
        const Duration(milliseconds: 1200),
      );
      final afterMaximumAge =
          await Stream<int>.periodic(
                const Duration(milliseconds: 50),
                (index) => index,
              )
              .asyncMap((_) => jetStream.streamInfo(ttlName))
              .firstWhere(
                (state) =>
                    DateTime.timestamp().isAfter(oldestAllowedCheck) &&
                    state.messages == BigInt.one,
              )
              .timeout(const Duration(seconds: 5));
      expect(afterMaximumAge.messages, BigInt.one);
      expect(
        (await jetStream.getMessage(ttlName, never.sequence)).payload,
        const [2],
      );

      await expectLater(
        jetStream.publish(
          "$ttlPrefix.invalid",
          const [],
          headers: NatsHeaders(entries: const [MapEntry("Nats-TTL", "500ms")]),
        ),
        throwsA(
          isA<JetStreamApiException>().having(
            (error) => error.errorCode,
            "errorCode",
            10165,
          ),
        ),
      );

      final constraintName = "EXPECT$suffix";
      final constraintPrefix = "expect.$suffix";
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: constraintName,
          subjects: ["$constraintPrefix.>"],
          storage: JetStreamStorage.memory,
        ),
      );
      createdStreams.add(constraintName);

      await expectLater(
        jetStream.publish(
          "$constraintPrefix.disabled",
          const [],
          options: JetStreamPublishOptions(
            lifetime: JetStreamMessageLifetime.expiresAfter(
              const Duration(seconds: 1),
            ),
          ),
        ),
        throwsA(
          isA<JetStreamApiException>().having(
            (error) => error.errorCode,
            "errorCode",
            10166,
          ),
        ),
      );

      final initial = await jetStream.publish("$constraintPrefix.one", const [
        1,
      ]);
      final exact = await jetStream.publish(
        "$constraintPrefix.one",
        const [2],
        options: JetStreamPublishOptions(
          expectedLastSubjectSequence: initial.sequence,
          expectedLastSubjectSequenceSubject: "$constraintPrefix.one",
        ),
      );
      final wildcard = await jetStream.publish(
        "$constraintPrefix.two",
        const [3],
        options: JetStreamPublishOptions(
          expectedLastSubjectSequence: exact.sequence,
          expectedLastSubjectSequenceSubject: "$constraintPrefix.*",
        ),
      );
      expect(exact.sequence, BigInt.two);
      expect(wildcard.sequence, BigInt.from(3));

      await expectLater(
        jetStream.publish(
          "$constraintPrefix.three",
          const [4],
          options: JetStreamPublishOptions(
            expectedLastSubjectSequence: exact.sequence,
            expectedLastSubjectSequenceSubject: "$constraintPrefix.*",
          ),
        ),
        throwsA(
          isA<JetStreamApiException>().having(
            (error) => error.errorCode,
            "errorCode",
            10071,
          ),
        ),
      );
      expect(
        (await jetStream.streamInfo(constraintName)).messages,
        BigInt.from(3),
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 15)),
  );
}
