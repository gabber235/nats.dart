import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  final server = Platform.environment["NATS_URL"];
  final skip = server == null ? "NATS_URL is not configured" : false;

  test("bounded asynchronous publish snapshots payload and drains "
      "deterministically", () async {
    final connection = await NatsConnection.connect(
      NatsOptions(servers: [NatsServer.parse(server!)]),
    );
    final jetStream = JetStreamContext(connection);
    final suffix = DateTime.now().microsecondsSinceEpoch;
    final stream = "ASYNC$suffix";
    await jetStream.createStream(
      JetStreamStreamConfig(
        name: stream,
        subjects: ["async.$suffix"],
        storage: JetStreamStorage.memory,
      ),
    );
    final publisher = jetStream.createAsyncPublisher(
      options: JetStreamAsyncPublisherOptions(
        maximumPendingMessages: 1,
        maximumPendingBytes: 1024,
        retryPolicy: JetStreamAsyncPublishRetryPolicy.none,
      ),
    );
    addTearDown(() async {
      try {
        await publisher.close();
      } on JetStreamAsyncPublisherDrainException {
        // The assertion path already observes this bounded failure.
      }
      await jetStream.deleteStream(stream);
      await connection.close();
    });

    final payload = [1, 2, 3];
    final pending = publisher.publish("async.$suffix", payload);
    payload[0] = 9;
    expect(
      () => publisher.publish("async.$suffix", const [4]),
      throwsA(isA<JetStreamAsyncPublishCapacityException>()),
    );

    final acknowledgement = await pending.acknowledgement;
    expect(pending.state, JetStreamAsyncPublishState.acknowledged);
    expect(pending.attempts, 1);
    expect(publisher.pendingMessages, 0);
    expect(publisher.pendingBytes, 0);
    expect(
      (await jetStream.getMessage(stream, acknowledgement.sequence)).payload,
      [1, 2, 3],
    );

    final drain = publisher.drain();
    expect(identical(drain, publisher.close()), isTrue);
    await drain;
    expect(
      () => publisher.publish("async.$suffix", const []),
      throwsA(isA<JetStreamAsyncPublisherClosedException>()),
    );
  }, skip: skip);

  test(
    "asynchronous publish exposes definite API rejection in drain summary",
    () async {
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(server!)]),
      );
      final jetStream = JetStreamContext(connection);
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final stream = "REJECT$suffix";
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: stream,
          subjects: ["reject.$suffix"],
          storage: JetStreamStorage.memory,
        ),
      );
      final publisher = jetStream.createAsyncPublisher();
      addTearDown(() async {
        await jetStream.deleteStream(stream);
        await connection.close();
      });

      final pending = publisher.publish("reject.$suffix", const [
        1,
      ], options: JetStreamPublishOptions(expectedStream: "WRONG"));
      await expectLater(
        pending.acknowledgement,
        throwsA(
          isA<JetStreamAsyncPublishException>().having(
            (error) => error.disposition,
            "disposition",
            JetStreamPublishDisposition.definitelyNotStored,
          ),
        ),
      );
      await expectLater(
        publisher.drain(),
        throwsA(
          isA<JetStreamAsyncPublisherDrainException>()
              .having(
                (error) => error.definitelyRejected,
                "definitelyRejected",
                1,
              )
              .having((error) => error.ambiguous, "ambiguous", 0),
        ),
      );
    },
    skip: skip,
  );
}
