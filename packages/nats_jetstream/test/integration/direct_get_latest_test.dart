import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  final server = Platform.environment["NATS_URL"];
  final skip = server == null ? "NATS_URL is not configured" : false;

  test(
    "direct single, batch, payload, and multiple subject reads round trip",
    () async {
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(server!)]),
      );
      final jetStream = JetStreamContext(connection);
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final name = "DIRECT$suffix";
      final prefix = "direct.$suffix";
      addTearDown(() async {
        try {
          await jetStream.deleteStream(name);
        } on JetStreamApiException {
          // The test may fail before stream creation.
        }
        await connection.close();
      });

      await jetStream.createStream(
        JetStreamStreamConfig(
          name: name,
          subjects: ["$prefix.>"],
          storage: JetStreamStorage.memory,
          allowDirect: true,
        ),
      );
      final first = await jetStream.publish(
        "$prefix.a",
        const [1],
        headers: NatsHeaders(
          entries: const [MapEntry("X-Test", "one"), MapEntry("X-Test", "two")],
        ),
      );
      final second = await jetStream.publish("$prefix.b", const [2]);
      final third = await jetStream.publish("$prefix.a", const [3]);

      final exact = await jetStream.direct.get(name, first.sequence);
      expect(exact.subject, "$prefix.a");
      expect(exact.payload, [1]);
      expect(exact.headers?.values("X-Test"), ["one", "two"]);
      expect(exact.headers?.first("Nats-Stream"), isNull);
      expect(await jetStream.direct.getPayload(name, second.sequence), [2]);

      final nextAcrossGap = await jetStream.direct.getNext(
        name,
        "$prefix.a",
        startSequence: second.sequence,
      );
      expect(nextAcrossGap.sequence, third.sequence);
      expect(
        await jetStream.direct.getNextPayload(
          name,
          "$prefix.a",
          startSequence: second.sequence,
        ),
        [3],
      );

      final last = await jetStream.direct.getLastBySubject(name, "$prefix.a");
      expect(last.sequence, third.sequence);
      expect(last.payload, [3]);
      expect(
        await jetStream.direct.getLastPayloadBySubject(name, "$prefix.b"),
        [2],
      );

      final page = await jetStream.direct.batch(
        name,
        JetStreamDirectBatchRequest(
          maximumMessages: 2,
          maximumBufferedBytes: 64 * 1024,
          maximumServerBytes: 32 * 1024,
          startSequence: BigInt.one,
          nextBySubject: "$prefix.>",
        ),
      );
      expect(page.messages.map((message) => message.payload), [
        [1],
        [2],
      ]);
      expect(page.pending, BigInt.one);
      expect(page.lastSequence, second.sequence);

      final multiple = await jetStream.direct.batch(
        name,
        JetStreamDirectBatchRequest(
          maximumMessages: 1,
          maximumBufferedBytes: 64 * 1024,
          multipleLastSubjects: ["$prefix.>"],
        ),
      );
      expect(multiple.messages, hasLength(1));
      expect(multiple.pending, BigInt.one);
      expect(multiple.upToSequence, third.sequence);

      final remainder = await jetStream.direct.batch(
        name,
        JetStreamDirectBatchRequest(
          maximumMessages: 2,
          maximumBufferedBytes: 64 * 1024,
          startSequence: multiple.lastSequence + BigInt.one,
          multipleLastSubjects: ["$prefix.>"],
          upToSequence: multiple.upToSequence,
        ),
      );
      expect(remainder.messages, hasLength(1));
      expect(remainder.pending, BigInt.zero);
      expect(remainder.upToSequence, third.sequence);

      await expectLater(
        jetStream.direct.getLast(name, "$prefix.missing"),
        throwsA(
          isA<JetStreamDirectGetException>().having(
            (error) => error.statusCode,
            "statusCode",
            404,
          ),
        ),
      );
    },
    skip: skip,
  );
}
