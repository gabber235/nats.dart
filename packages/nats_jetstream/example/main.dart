import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";

Future<void> main() async {
  final server = Platform.environment["NATS_URL"] ?? "nats://127.0.0.1:4222";
  final suffix = DateTime.now().microsecondsSinceEpoch;
  final stream = "EXAMPLE$suffix";
  final subject = "example.$suffix.events";
  final connection = await NatsConnection.connect(
    NatsOptions(
      servers: [NatsServer.parse(server)],
      name: "nats_jetstream example",
    ),
  );
  final jetStream = JetStreamContext(connection);
  try {
    await jetStream.createStream(
      JetStreamStreamConfig(name: stream, subjects: [subject]),
    );
    final acknowledgement = await jetStream.publish(subject, const [
      1,
      2,
      3,
    ], options: JetStreamPublishOptions(messageId: "event-42"));
    final stored = await jetStream.getMessage(stream, acknowledgement.sequence);
    print(stored.payload);
    await jetStream.deleteStream(stream);
    await connection.drain();
  } finally {
    await connection.close();
  }
}
