import "dart:convert";
import "dart:io";

import "package:nats_core/nats_core.dart";

Future<void> main() async {
  final server = Platform.environment["NATS_URL"] ?? "nats://127.0.0.1:4222";
  final connection = await NatsConnection.connect(
    NatsOptions(servers: [NatsServer.parse(server)], name: "nats_core example"),
  );
  try {
    final subscription = await connection.subscribe("events.created");
    final received = subscription.messages.first;
    await connection.publish("events.created", utf8.encode("42"));
    await connection.flush();
    print(utf8.decode((await received).payload));
    await subscription.unsubscribe();
    await connection.drain();
  } finally {
    await connection.close();
  }
}
