@TestOn("vm")
library;

import "dart:io";
import "dart:typed_data";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  final serverUrl = Platform.environment["NATS_URL"];
  final skipReason = serverUrl == null ? "NATS_URL is not configured" : false;

  test(
    "bounded snapshot and restore preserve stream bytes and consumer state",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final stream = "TRANSFER$suffix";
      final subject = "transfer.$suffix.events";
      const consumer = "DURABLE";
      final connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(serverUrl!)],
          name: "nats-dart-stream-transfer-$suffix",
        ),
      );
      final jetStream = JetStreamContext(connection);
      addTearDown(() async {
        try {
          await jetStream.deleteStream(stream);
        } on JetStreamApiException {
          // Cleanup is idempotent after transfer failure.
        }
        await connection.close();
      });

      await jetStream.createStream(
        JetStreamStreamConfig(name: stream, subjects: [subject]),
      );
      await jetStream.publish(
        subject,
        const [0, 255, 13, 10],
        headers: NatsHeaders(
          entries: const [
            MapEntry("Traceparent", "first"),
            MapEntry("traceparent", "second"),
          ],
        ),
      );
      await jetStream.publish(
        subject,
        List<int>.generate(8192, (index) => index & 0xff),
      );
      await jetStream.publish(subject, const [7, 8, 9]);
      await jetStream.deleteMessage(stream, BigInt.two);
      await jetStream.consumers.createPull(
        stream,
        JetStreamPullConsumerConfig(name: consumer),
      );

      final archiveSink = BytesBuilder(copy: false);
      final snapshot = await jetStream.snapshotStreamTo(
        stream,
        archiveSink.add,
        maximumBytes: BigInt.from(16 * 1024 * 1024),
        options: JetStreamSnapshotOptions(
          checkMessages: true,
          chunkSize: 1024,
          windowSize: 1024,
          maximumBufferedBytes: 4096,
        ),
      );
      final archive = archiveSink.takeBytes();
      expect(snapshot.metadata.stream, stream);
      expect(snapshot.bytes, BigInt.from(archive.length));
      expect(snapshot.chunks, greaterThan(1));

      var sourceListened = false;
      final rejectedSource = Stream<List<int>>.multi((controller) {
        sourceListened = true;
        controller
          ..addSync(archive)
          ..closeSync();
      });
      await expectLater(
        jetStream.restoreStreamFrom(
          snapshot.metadata,
          rejectedSource,
          maximumBytes: BigInt.from(16 * 1024 * 1024),
        ),
        throwsA(
          isA<JetStreamApiException>().having(
            (error) => error.errorCode,
            "errorCode",
            10130,
          ),
        ),
      );
      expect(sourceListened, isFalse);

      await jetStream.deleteStream(stream);
      final corrupt = Uint8List.fromList(archive);
      corrupt[corrupt.length ~/ 2] ^= 0xff;
      await expectLater(
        jetStream.restoreStreamFrom(
          snapshot.metadata,
          Stream.value(corrupt),
          maximumBytes: BigInt.from(16 * 1024 * 1024),
          options: JetStreamRestoreOptions(uploadChunkSize: 4096),
        ),
        throwsA(
          isA<JetStreamRestoreException>().having(
            (error) => error.cause,
            "cause",
            isA<JetStreamApiException>(),
          ),
        ),
      );
      await expectLater(
        jetStream.streamInfo(stream),
        throwsA(isA<JetStreamApiException>()),
      );

      final split = archive.length ~/ 2;
      final restored = await jetStream.restoreStreamFrom(
        snapshot.metadata,
        Stream.fromIterable([
          const <int>[],
          Uint8List.sublistView(archive, 0, split),
          const <int>[],
          Uint8List.sublistView(archive, split),
        ]),
        maximumBytes: BigInt.from(16 * 1024 * 1024),
        options: JetStreamRestoreOptions(uploadChunkSize: 4096),
      );
      expect(restored.state.messages, BigInt.two);
      expect(restored.state.firstSequence, BigInt.one);
      expect(restored.state.lastSequence, BigInt.from(3));

      final first = await jetStream.getMessage(stream, BigInt.one);
      expect(first.payload, [0, 255, 13, 10]);
      expect(first.headers?.values("traceparent"), ["first", "second"]);
      await expectLater(
        jetStream.getMessage(stream, BigInt.two),
        throwsA(isA<JetStreamApiException>()),
      );
      expect((await jetStream.getMessage(stream, BigInt.from(3))).payload, [
        7,
        8,
        9,
      ]);
      expect((await jetStream.consumers.info(stream, consumer)).name, consumer);
    },
    skip: skipReason,
    timeout: const Timeout(Duration(minutes: 1)),
  );
}
