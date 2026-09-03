import "dart:async";
import "dart:convert";
import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";

import "../../../nats_core/test/support/nats_server_process.dart";

final class NatsClusterEnvironment {
  NatsClusterEnvironment._(this.directory, this.servers);

  final Directory directory;
  final List<NatsClusterServer> servers;
  final Map<String, NatsServerProcess> running = {};

  static Future<NatsClusterEnvironment> create() async {
    final directory = await Directory.systemTemp.createTemp(
      "nats-dart-jetstream-cluster-",
    );
    final ports = await _reserveLoopbackPorts(9);
    final servers = List.generate(
      3,
      (index) => NatsClusterServer(
        name: String.fromCharCode("A".codeUnitAt(0) + index),
        clientPort: ports[index * 3],
        monitorPort: ports[index * 3 + 1],
        routePort: ports[index * 3 + 2],
        config: File("${directory.path}/server-$index.conf"),
        storeDirectory: "${directory.path}/store-$index",
      ),
    );
    for (final server in servers) {
      await server.writeConfig(servers);
    }
    return NatsClusterEnvironment._(directory, servers);
  }

  Future<void> start(String binary) async {
    for (final server in servers) {
      running[server.name] = await server.start(binary);
    }
    await eventually(
      () => _status(this),
      _ready,
      "NATS route mesh and JetStream meta cluster did not become ready",
      timeout: const Duration(seconds: 45),
    );
    for (final server in servers) {
      final probe = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(server.clientUri.toString())],
          reconnect: NatsReconnectPolicy(maximumAttempts: 0),
        ),
      );
      await probe.flush();
      await probe.close();
    }
  }

  NatsClusterServer byName(String name) =>
      servers.singleWhere((server) => server.name == name);

  String get logs => running.entries
      .map((entry) => "${entry.key}\n${entry.value.logs.join("\n")}")
      .join("\n");

  Future<void> close() async {
    for (final server in running.values.toList(growable: false)) {
      await server.stop();
    }
    running.clear();
    await directory.delete(recursive: true);
  }
}

final class NatsClusterServer {
  const NatsClusterServer({
    required this.name,
    required this.clientPort,
    required this.monitorPort,
    required this.routePort,
    required this.config,
    required this.storeDirectory,
  });

  final String name;
  final int clientPort;
  final int monitorPort;
  final int routePort;
  final File config;
  final String storeDirectory;

  Uri get clientUri => Uri(
    scheme: "nats",
    host: InternetAddress.loopbackIPv4.address,
    port: clientPort,
  );

  Uri get monitorUri => Uri(
    scheme: "http",
    host: InternetAddress.loopbackIPv4.address,
    port: monitorPort,
  );

  Future<void> writeConfig(List<NatsClusterServer> cluster) {
    final seed = cluster.first;
    final routeTarget = name == seed.name ? cluster[1] : seed;
    final routes =
        '\n  routes: ["nats-route://127.0.0.1:${routeTarget.routePort}"]';
    return config.writeAsString("""
server_name: "$name"
jetstream {
  store_dir: "$storeDirectory"
  max_memory_store: 256MB
  max_file_store: 1GB
}
cluster {
  name: "nats-dart"
  host: "127.0.0.1"
  port: $routePort$routes
}
""");
  }

  Future<NatsServerProcess> start(String binary) {
    return NatsServerProcess.start(
      binary: binary,
      clientPort: clientPort,
      monitorPort: monitorPort,
      configPath: config.path,
      storeDirectory: storeDirectory,
      jetStream: false,
      requireJetStreamReady: false,
    );
  }
}

bool streamReplicasCurrent(JetStreamStreamInfo info) {
  final cluster = info.cluster;
  return cluster?.leader != null &&
      cluster!.replicas.length == 2 &&
      cluster.replicas.every(
        (replica) =>
            replica.current && !replica.offline && replica.lag == BigInt.zero,
      );
}

Future<T> eventually<T>(
  Future<T> Function() operation,
  bool Function(T) condition,
  String failure, {
  Duration timeout = const Duration(seconds: 15),
}) async {
  final deadline = DateTime.timestamp().add(timeout);
  Object? lastFailure;
  T? lastValue;
  while (DateTime.timestamp().isBefore(deadline)) {
    try {
      final value = await operation();
      lastValue = value;
      if (condition(value)) return value;
    } on Object catch (error) {
      lastFailure = error;
    }
    await Future<void>.delayed(const Duration(milliseconds: 20));
  }
  throw StateError(
    "$failure. Last value: $lastValue. Last error: $lastFailure",
  );
}

Future<
  ({List<Map<String, Object?>> routes, List<Map<String, Object?>> jetStream})
>
_status(NatsClusterEnvironment cluster) async {
  final routes = await Future.wait(
    cluster.servers.map((server) => _monitorJson(server.monitorUri, "/routez")),
  );
  final jetStream = await Future.wait(
    cluster.servers.map((server) => _monitorJson(server.monitorUri, "/jsz")),
  );
  return (routes: routes, jetStream: jetStream);
}

bool _ready(
  ({List<Map<String, Object?>> routes, List<Map<String, Object?>> jetStream})
  status,
) {
  const expectedServerNames = {"A", "B", "C"};
  for (final routeStatus in status.routes) {
    final serverName = routeStatus["server_name"];
    if (serverName is! String || !expectedServerNames.contains(serverName)) {
      return false;
    }
    final routes = routeStatus["routes"];
    if (routes is! List<Object?>) return false;
    final remoteNames = routes
        .whereType<Map<String, Object?>>()
        .map((route) => route["remote_name"])
        .whereType<String>()
        .toSet();
    if (!remoteNames.containsAll(
      expectedServerNames.difference({serverName}),
    )) {
      return false;
    }
  }
  final meta = status.jetStream
      .map((value) => value["meta_cluster"])
      .whereType<Map<String, Object?>>()
      .toList(growable: false);
  if (meta.length != 3 || meta.any((value) => value["cluster_size"] != 3)) {
    return false;
  }
  final leaders = meta
      .map((value) => value["leader"])
      .whereType<String>()
      .where((value) => value.isNotEmpty)
      .toSet();
  return leaders.length == 1;
}

Future<Map<String, Object?>> _monitorJson(Uri monitor, String path) async {
  final client = HttpClient();
  try {
    final request = await client.getUrl(monitor.replace(path: path));
    final response = await request.close();
    final body = await utf8.decodeStream(response);
    final decoded = jsonDecode(body);
    if (response.statusCode == HttpStatus.ok &&
        decoded is Map<String, Object?>) {
      return decoded;
    }
    throw StateError("Invalid NATS monitor response $path: $decoded");
  } finally {
    client.close(force: true);
  }
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
