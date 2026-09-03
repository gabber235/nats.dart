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
    "message schedules deliver, purge, and cancel exactly",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final streamName = "S$suffix";
      final schedulePrefix = "schedule.$suffix";
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
          subjects: ["$schedulePrefix.>"],
          storage: JetStreamStorage.memory,
          allowMessageSchedules: true,
        ),
      );
      streamExists = true;

      final targetSubject = "$schedulePrefix.output";
      final scheduleAck = await jetStream.scheduleMessage(
        "$schedulePrefix.immediate",
        const [0, 255, 13, 10],
        options: JetStreamScheduleOptions(
          schedule: JetStreamSchedule.at(
            DateTime.now().toUtc().add(const Duration(seconds: 1)),
          ),
          target: targetSubject,
        ),
      );
      final generatedSequence = scheduleAck.sequence + BigInt.one;
      await Stream<int>.periodic(
            const Duration(milliseconds: 25),
            (index) => index,
          )
          .asyncMap((_) => jetStream.streamInfo(streamName))
          .firstWhere((state) => state.lastSequence >= generatedSequence)
          .timeout(const Duration(seconds: 5));
      final delivered = await jetStream.getMessage(
        streamName,
        generatedSequence,
      );
      expect(delivered.subject, targetSubject);
      expect(delivered.payload, const [0, 255, 13, 10]);
      expect(delivered.headers?.first("Nats-Scheduler"), isNotNull);
      expect(delivered.headers?.first("Nats-Schedule-Next"), "purge");
      expect((await jetStream.streamInfo(streamName)).messages, BigInt.one);

      final beforeCancellation = (await jetStream.streamInfo(
        streamName,
      )).messages;
      final scheduled = await jetStream.scheduleMessage(
        "$schedulePrefix.future",
        const [1],
        options: JetStreamScheduleOptions(
          schedule: JetStreamSchedule.at(
            DateTime.now().toUtc().add(const Duration(hours: 1)),
          ),
          target: "$schedulePrefix.future-output",
        ),
      );
      await jetStream.cancelSchedule(
        "$schedulePrefix.cancelled",
        "$schedulePrefix.future",
        expectedScheduleSequence: scheduled.sequence,
        payload: const [2],
      );
      expect(
        (await jetStream.streamInfo(streamName)).messages,
        beforeCancellation + BigInt.one,
      );
      await expectLater(
        jetStream.cancelSchedule(
          "$schedulePrefix.cancelled-again",
          "$schedulePrefix.future",
          expectedScheduleSequence: scheduled.sequence,
        ),
        throwsA(
          isA<JetStreamApiException>().having(
            (error) => error.errorCode,
            "errorCode",
            anyOf(10071, 10164),
          ),
        ),
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 15)),
  );

  test(
    "recurring schedule samples latest value with TTL and rollup",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final streamName = "R$suffix";
      final prefix = "recurring.$suffix";
      final scheduleSubject = "$prefix.schedule";
      final sourceSubject = "$prefix.source";
      final targetSubject = "$prefix.target";
      final cancelSubject = "$prefix.cancelled";
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(serverUrl!)]),
      );
      final jetStream = JetStreamContext(connection);
      addTearDown(() async {
        try {
          await jetStream.deleteStream(streamName);
        } on JetStreamApiException {
          // Cleanup follows the test owned stream state.
        }
        await connection.close();
      });
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: streamName,
          subjects: ["$prefix.>"],
          storage: JetStreamStorage.memory,
          allowRollup: true,
          allowMessageTtl: true,
          allowMessageSchedules: true,
        ),
      );
      await jetStream.publish(targetSubject, const [0]);
      await jetStream.publish(sourceSubject, const [1]);
      await jetStream.publish(sourceSubject, const [2]);

      final scheduled = await jetStream.scheduleMessage(
        scheduleSubject,
        const [99],
        options: JetStreamScheduleOptions(
          schedule: JetStreamSchedule.every(const Duration(seconds: 1)),
          target: targetSubject,
          source: sourceSubject,
          generatedMessageTtl: JetStreamScheduleTtl.duration(
            const Duration(seconds: 2),
          ),
          rollupGeneratedSubject: true,
        ),
      );
      final recurrent = await _awaitState(
        jetStream,
        streamName,
        (state) => state.lastSequence >= scheduled.sequence + BigInt.two,
      );
      expect(recurrent.messages, BigInt.from(4));

      final sampled = await jetStream.getLastMessage(streamName, targetSubject);
      expect(
        sampled.sequence,
        greaterThanOrEqualTo(scheduled.sequence + BigInt.two),
      );
      expect(sampled.payload, [2]);
      expect(sampled.headers?.first("Nats-Scheduler"), scheduleSubject);
      expect(sampled.headers?.first("Nats-Schedule-Next"), isNot("purge"));
      expect(sampled.headers?.first("Nats-TTL"), "2000000us");
      expect(sampled.headers?.first("Nats-Rollup"), "sub");

      await jetStream.cancelSchedule(
        cancelSubject,
        scheduleSubject,
        expectedScheduleSequence: scheduled.sequence,
      );
      await _awaitTargetExpiry(jetStream, streamName, targetSubject);
      expect((await jetStream.streamInfo(streamName)).messages, BigInt.from(3));
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 15)),
  );
}

Future<JetStreamStreamInfo> _awaitState(
  JetStreamContext jetStream,
  String stream,
  bool Function(JetStreamStreamInfo state) matches,
) async {
  final deadline = DateTime.now().add(const Duration(seconds: 8));
  JetStreamStreamInfo? last;
  while (DateTime.now().isBefore(deadline)) {
    last = await jetStream.streamInfo(stream);
    if (matches(last)) return last;
    await Future<void>.delayed(const Duration(milliseconds: 25));
  }
  throw StateError(
    "Stream $stream did not reach the expected state. "
    "Last sequence: ${last?.lastSequence}. Last messages: ${last?.messages}",
  );
}

Future<void> _awaitTargetExpiry(
  JetStreamContext jetStream,
  String stream,
  String subject,
) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (DateTime.now().isBefore(deadline)) {
    try {
      await jetStream.getLastMessage(stream, subject);
    } on JetStreamApiException catch (error) {
      if (error.errorCode == 10037) return;
      rethrow;
    }
    await Future<void>.delayed(const Duration(milliseconds: 25));
  }
  throw StateError("Scheduled target $subject did not expire");
}
