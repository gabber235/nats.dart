import "package:nats_core/nats_core.dart";

Future<void> main() async {
  final connection = await NatsConnection.connect(
    NatsOptions(servers: [NatsServer.parse("wss://localhost/nats")]),
  );
  await connection.close();
}
