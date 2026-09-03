@TestOn("vm")
library;

import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:test/test.dart";

import "../support/nats_server_process.dart";

void main() {
  final serverBinary = Platform.environment["NATS_SERVER_BINARY"];
  final skipReason = serverBinary == null
      ? "NATS_SERVER_BINARY is not configured"
      : false;

  test(
    "client keepalive closes a one way stalled generation",
    () async {
      final directory = await Directory.systemTemp.createTemp(
        "nats-dart-keepalive-",
      );
      final config = File("${directory.path}/nats.conf");
      await config.writeAsString("# Isolated keepalive suspension test\n");
      final server = await NatsServerProcess.start(
        binary: serverBinary!,
        clientPort: await reserveLoopbackPort(),
        monitorPort: await reserveLoopbackPort(),
        configPath: config.path,
        storeDirectory: "${directory.path}/jetstream",
      );
      NatsConnection? connection;
      var serverPaused = false;
      addTearDown(() async {
        await connection?.close();
        if (serverPaused) {
          server.resume();
        }
        await server.stop();
        await directory.delete(recursive: true);
      });

      connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(server.clientUri.toString())],
          reconnect: NatsReconnectPolicy(maximumAttempts: 0),
          pingInterval: const Duration(milliseconds: 100),
        ),
      );
      final disconnected = connection.events
          .firstWhere((event) => event is NatsDisconnected)
          .then((event) => event as NatsDisconnected);

      server.pause();
      serverPaused = true;
      final event = await disconnected.timeout(const Duration(seconds: 2));
      expect(event.willReconnect, isFalse);
      expect(event.error, isA<NatsConnectionException>());
      expect(event.error.message, contains("keepalive"));
      await connection.closed.timeout(const Duration(seconds: 2));
      expect(connection.isConnected, isFalse);

      server.resume();
      serverPaused = false;
      await server.stop();
      connection = null;
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}
