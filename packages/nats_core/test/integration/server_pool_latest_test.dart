@TestOn("vm")
library;

import "dart:async";
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
    "setServers and selector switch to the chosen live server",
    () async {
      final directory = await Directory.systemTemp.createTemp(
        "nats-dart-server-pool-",
      );
      final firstConfig = File("${directory.path}/first.conf");
      final secondConfig = File("${directory.path}/second.conf");
      await firstConfig.writeAsString("server_name: first\n");
      await secondConfig.writeAsString("server_name: second\n");
      NatsServerProcess? first;
      NatsServerProcess? second;
      NatsConnection? connection;
      addTearDown(() async {
        await connection?.close();
        await first?.stop();
        await second?.stop();
        await directory.delete(recursive: true);
      });

      first = await _startServer(
        serverBinary!,
        directory,
        firstConfig,
        "first",
      );
      second = await _startServer(
        serverBinary,
        directory,
        secondConfig,
        "second",
      );
      final unavailable = NatsServer.nats(
        InternetAddress.loopbackIPv4.address,
        port: await reserveLoopbackPort(),
      );
      var selectorCalls = 0;
      connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(first.clientUri.toString())],
          reconnect: NatsReconnectPolicy(
            maximumAttempts: 2,
            baseDelay: const Duration(milliseconds: 20),
            maximumDelay: const Duration(milliseconds: 20),
            jitter: 0,
          ),
          reconnectToServer: (pool, serverInfo) {
            selectorCalls += 1;
            return NatsReconnectServerSelection(server: pool.last);
          },
        ),
      );
      final reconnecting = connection.events
          .where((event) => event is NatsReconnecting)
          .cast<NatsReconnecting>()
          .first;
      final reconnected = connection.events
          .where((event) => event is NatsConnected)
          .cast<NatsConnected>()
          .firstWhere((event) => event.generation == 2);
      connection.setServers([
        unavailable,
        NatsServer.parse(second.clientUri.toString()),
      ]);

      await first.stop();
      first = null;
      final attempt = await reconnecting.timeout(const Duration(seconds: 5));
      await reconnected.timeout(const Duration(seconds: 5));

      expect(selectorCalls, 1);
      expect(attempt.server.uri.authority, second.clientUri.authority);
      expect(connection.serverInfo.serverAuthority, second.clientUri.authority);
      expect(connection.statistics.reconnectAttempts, 1);
      expect(connection.getServers().map((server) => server.uri.authority), [
        unavailable.uri.authority,
        second.clientUri.authority,
      ]);
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 20)),
  );
}

Future<NatsServerProcess> _startServer(
  String binary,
  Directory directory,
  File config,
  String name,
) async => NatsServerProcess.start(
  binary: binary,
  clientPort: await reserveLoopbackPort(),
  monitorPort: await reserveLoopbackPort(),
  configPath: config.path,
  storeDirectory: "${directory.path}/$name-store",
  jetStream: false,
  requireJetStreamReady: false,
);
