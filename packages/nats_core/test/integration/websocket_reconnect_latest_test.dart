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
    "WebSocket discovery preserves transport, path, and subscription replay",
    () async {
      final environment = await _createEnvironment();
      NatsServerProcess? firstServer;
      NatsServerProcess? secondServer;
      NatsConnection? connection;
      NatsConnection? secondConnection;
      StreamSubscription<NatsMessage>? deliveryListener;
      StreamSubscription<NatsConnectionEvent>? eventListener;
      final deliveries = StreamController<NatsMessage>.broadcast(sync: true);
      addTearDown(() async {
        await eventListener?.cancel();
        await deliveryListener?.cancel();
        await deliveries.close();
        await connection?.close();
        await secondConnection?.close();
        await firstServer?.stop();
        await secondServer?.stop();
        await environment.directory.delete(recursive: true);
      });

      firstServer = await _startServer(serverBinary!, environment.first);
      connection = await NatsConnection.connect(
        NatsOptions(
          servers: [
            NatsServer.websocket(
              InternetAddress.loopbackIPv4.address,
              port: environment.first.webSocketPort,
              path: "/nats",
            ),
          ],
          reconnect: NatsReconnectPolicy(
            maximumAttempts: 20,
            baseDelay: const Duration(milliseconds: 20),
            maximumDelay: const Duration(milliseconds: 50),
            jitter: 0,
          ),
        ),
      );
      final events = <String>[];
      final disconnected = Completer<void>();
      final reconnected = Completer<NatsConnected>();
      eventListener = connection.events.listen(
        (event) {
          events.add(_describeEvent(event));
          if (event is NatsDisconnected && !disconnected.isCompleted) {
            disconnected.complete();
          }
          if (event case NatsConnected(generation: 2)) {
            reconnected.complete(event);
          }
        },
        onDone: () {
          if (!reconnected.isCompleted) {
            reconnected.completeError(
              StateError(
                "Reconnect stream closed. First WebSocket: "
                "${environment.first.webSocketPort}. Second WebSocket: "
                "${environment.second.webSocketPort}. Events: $events",
              ),
            );
          }
        },
      );

      final suffix = DateTime.now().microsecondsSinceEpoch;
      final subject = "websocket.$suffix.reconnect";
      final subscription = await connection.subscribe(subject);
      deliveryListener = subscription.messages.listen(
        deliveries.add,
        onError: (_) {},
      );
      final firstDelivery = deliveries.stream.first;
      await connection.publish(subject, const [1]);
      await connection.flush();
      expect((await firstDelivery).payload, [1]);

      secondServer = await _startServer(serverBinary, environment.second);
      await _awaitWebSocketDiscovery(
        firstServer,
        environment.second.webSocketPort,
      );
      final secondProbe = await WebSocket.connect(
        "ws://127.0.0.1:${environment.second.webSocketPort}/nats",
      );
      await secondProbe.close();
      secondConnection = await NatsConnection.connect(
        NatsOptions(
          servers: [
            NatsServer.websocket(
              InternetAddress.loopbackIPv4.address,
              port: environment.second.webSocketPort,
              path: "/nats",
            ),
          ],
          reconnect: NatsReconnectPolicy(maximumAttempts: 0),
        ),
      );
      await secondConnection.flush();
      await connection.flush();

      await firstServer.stop();
      firstServer = null;
      await secondConnection.flush();
      await disconnected.future.timeout(const Duration(seconds: 5));
      final connected = await reconnected.future.timeout(
        const Duration(seconds: 10),
      );
      expect(connected.server.scheme, "ws");
      expect(connected.server.port, environment.second.webSocketPort);
      expect(connected.server.path, "/nats");

      final replayedDelivery = deliveries.stream.first;
      await connection.publish(subject, const [2]);
      await connection.flush();
      expect(
        (await replayedDelivery.timeout(const Duration(seconds: 5))).payload,
        [2],
      );

      await subscription.unsubscribe();
      await connection.drain();
      await connection.closed;
      connection = null;
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}

String _describeEvent(NatsConnectionEvent event) => switch (event) {
  NatsReconnecting(:final server, :final attempt) =>
    "reconnecting $attempt $server",
  NatsConnected(:final server, :final generation) =>
    "connected $generation $server",
  NatsDisconnected(:final generation) => "disconnected $generation",
  NatsClosed(:final error) => "closed $error",
  _ => event.runtimeType.toString(),
};

Future<
  ({Directory directory, _ServerEnvironment first, _ServerEnvironment second})
>
_createEnvironment() async {
  final directory = await Directory.systemTemp.createTemp(
    "nats-dart-websocket-cluster-",
  );
  final ports = await _reserveLoopbackPorts(8);
  final first = _ServerEnvironment(
    name: "websocket-a",
    clientPort: ports[0],
    monitorPort: ports[1],
    webSocketPort: ports[2],
    routePort: ports[3],
    config: File("${directory.path}/a.conf"),
    storeDirectory: "${directory.path}/a-store",
  );
  final second = _ServerEnvironment(
    name: "websocket-b",
    clientPort: ports[4],
    monitorPort: ports[5],
    webSocketPort: ports[6],
    routePort: ports[7],
    config: File("${directory.path}/b.conf"),
    storeDirectory: "${directory.path}/b-store",
  );
  await first.writeConfig(routePort: second.routePort);
  await second.writeConfig();
  return (directory: directory, first: first, second: second);
}

Future<NatsServerProcess> _startServer(
  String binary,
  _ServerEnvironment server,
) {
  return NatsServerProcess.start(
    binary: binary,
    clientPort: server.clientPort,
    monitorPort: server.monitorPort,
    configPath: server.config.path,
    storeDirectory: server.storeDirectory,
    jetStream: false,
  );
}

Future<List<int>> _reserveLoopbackPorts(int count) async {
  final sockets = await Future.wait(
    List.generate(
      count,
      (_) => ServerSocket.bind(InternetAddress.loopbackIPv4, 0),
    ),
  );
  try {
    return sockets.map((socket) => socket.port).toList(growable: false);
  } finally {
    await Future.wait(sockets.map((socket) => socket.close()));
  }
}

Future<void> _awaitWebSocketDiscovery(
  NatsServerProcess server,
  int expectedPort,
) async {
  final expected = "${InternetAddress.loopbackIPv4.address}:$expectedPort";
  final deadline = DateTime.timestamp().add(const Duration(seconds: 10));
  Object? lastValue;
  while (DateTime.timestamp().isBefore(deadline)) {
    final varz = await server.varz();
    lastValue = varz["ws_connect_urls"];
    if (lastValue case final List<Object?> values
        when values.contains(expected)) {
      return;
    }
    await Future<void>.delayed(const Duration(milliseconds: 20));
  }
  throw StateError(
    "WebSocket discovery did not include $expected. Last value: $lastValue",
  );
}

final class _ServerEnvironment {
  const _ServerEnvironment({
    required this.name,
    required this.clientPort,
    required this.monitorPort,
    required this.webSocketPort,
    required this.routePort,
    required this.config,
    required this.storeDirectory,
  });

  final String name;
  final int clientPort;
  final int monitorPort;
  final int webSocketPort;
  final int routePort;
  final File config;
  final String storeDirectory;

  Future<void> writeConfig({int? routePort}) {
    return config.writeAsString("""
server_name: "$name"
websocket {
  host: "127.0.0.1"
  port: $webSocketPort
  no_tls: true
  advertise: "127.0.0.1:$webSocketPort"
}
cluster {
  name: "websocket"
  host: "127.0.0.1"
  port: ${this.routePort}
  ${routePort == null ? "" : "routes: [\"nats-route://127.0.0.1:$routePort\"]"}
}
""");
  }
}
