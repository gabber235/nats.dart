import "dart:async";
import "dart:convert";
import "dart:io";

import "../packages/nats_core/test/support/nats_server_process.dart";

const nativeSecurityFixtureFiles = {
  "ca.pem",
  "server.pem",
  "server.key",
  "client.pem",
  "client.key",
  "outsider.pem",
  "outsider.key",
  "auth.conf",
  "user.creds",
};

Future<void> main() async {
  final binary = Platform.environment["NATS_SERVER_BINARY"];
  if (binary == null || binary.isEmpty) {
    throw StateError("NATS_SERVER_BINARY is required");
  }

  final environment = await NativeSecurityEnvironment.start(
    binary: binary,
    fixtures: Directory("tool/native_security_fixtures"),
  );
  try {
    final process = await Process.start(
      Platform.resolvedExecutable,
      const [
        "test",
        "--chain-stack-traces",
        "--concurrency=1",
        "packages/nats_core/test/integration/tls_latest_test.dart",
        "packages/nats_core/test/integration/websocket_latest_test.dart",
        "packages/nats_core/test/integration/auth_latest_test.dart",
      ],
      environment: environment.testEnvironment,
      mode: ProcessStartMode.inheritStdio,
    );
    try {
      final result = await process.exitCode.timeout(const Duration(minutes: 5));
      if (result != 0) {
        environment.writeServerLogs(stderr);
        exitCode = result;
      }
    } on TimeoutException {
      process.kill(ProcessSignal.sigkill);
      await process.exitCode;
      environment.writeServerLogs(stderr);
      throw StateError("Native security integration exceeded 5 minutes");
    }
  } finally {
    await environment.close();
  }
}

final class NativeSecurityEnvironment {
  NativeSecurityEnvironment._(
    this.directory,
    this._servers,
    this.testEnvironment,
  );

  final Directory directory;
  final Map<String, NatsServerProcess> _servers;
  final Map<String, String> testEnvironment;

  static Future<NativeSecurityEnvironment> start({
    required String binary,
    required Directory fixtures,
  }) async {
    validateNativeSecurityFixtures(fixtures);
    final root = await Directory.systemTemp.createTemp(
      "nats-dart-native-security-",
    );
    final fixturePath = fixtures.absolute.path;
    final ca = "${fixtures.absolute.path}${Platform.pathSeparator}ca.pem";
    final serverCertificate = "$fixturePath${Platform.pathSeparator}server.pem";
    final serverKey = "$fixturePath${Platform.pathSeparator}server.key";
    final clientCertificate = "$fixturePath${Platform.pathSeparator}client.pem";
    final clientKey = "$fixturePath${Platform.pathSeparator}client.key";
    final outsiderCertificate =
        "$fixturePath${Platform.pathSeparator}outsider.pem";
    final outsiderKey = "$fixturePath${Platform.pathSeparator}outsider.key";
    final credentials = "$fixturePath${Platform.pathSeparator}user.creds";
    final servers = <String, NatsServerProcess>{};
    try {
      final tls = await _startServer(
        binary: binary,
        root: root,
        name: "tls",
        config: tlsServerConfig(
          certificate: serverCertificate,
          key: serverKey,
          ca: ca,
        ),
      );
      servers["tls"] = tls;
      final tlsFirst = await _startServer(
        binary: binary,
        root: root,
        name: "tls-first",
        config: tlsServerConfig(
          certificate: serverCertificate,
          key: serverKey,
          ca: ca,
          handshakeFirst: true,
        ),
      );
      servers["tls-first"] = tlsFirst;
      final mutualTls = await _startServer(
        binary: binary,
        root: root,
        name: "mtls",
        config: tlsServerConfig(
          certificate: serverCertificate,
          key: serverKey,
          ca: ca,
          verifyClients: true,
        ),
      );
      servers["mtls"] = mutualTls;

      final plainWebSocketPort = await reserveLoopbackPort();
      final plainWebSocket = await _startServer(
        binary: binary,
        root: root,
        name: "ws",
        config: webSocketServerConfig(port: plainWebSocketPort),
      );
      servers["ws"] = plainWebSocket;
      final secureWebSocketPort = await reserveLoopbackPort();
      final secureWebSocket = await _startServer(
        binary: binary,
        root: root,
        name: "wss",
        config: webSocketServerConfig(
          port: secureWebSocketPort,
          certificate: serverCertificate,
          key: serverKey,
        ),
      );
      servers["wss"] = secureWebSocket;

      final auth = await _startServer(
        binary: binary,
        root: root,
        name: "auth",
        config: nativeAuthenticationConfig,
      );
      servers["auth"] = auth;
      final tokenAuth = await _startServer(
        binary: binary,
        root: root,
        name: "token-auth",
        config: nativeTokenAuthenticationConfig,
      );
      servers["token-auth"] = tokenAuth;
      final credentialsAuth = await _startServer(
        binary: binary,
        root: root,
        name: "credentials",
        config: await File(
          "$fixturePath${Platform.pathSeparator}auth.conf",
        ).readAsString(),
      );
      servers["credentials"] = credentialsAuth;

      return NativeSecurityEnvironment._(root, servers, {
        "NATS_TLS_URL": "tls://127.0.0.1:${tls.clientPort}",
        "NATS_TLS_FIRST_URL": "tls://127.0.0.1:${tlsFirst.clientPort}",
        "NATS_CA": ca,
        "NATS_MTLS_URL": "tls://127.0.0.1:${mutualTls.clientPort}",
        "NATS_MTLS_CA": ca,
        "NATS_MTLS_CLIENT_CERT": clientCertificate,
        "NATS_MTLS_CLIENT_KEY": clientKey,
        "NATS_MTLS_OUTSIDER_CERT": outsiderCertificate,
        "NATS_MTLS_OUTSIDER_KEY": outsiderKey,
        "NATS_WS_URL": "ws://127.0.0.1:$plainWebSocketPort/nats",
        "NATS_WSS_URL": "wss://127.0.0.1:$secureWebSocketPort/nats",
        "NATS_WS_CA": ca,
        "NATS_AUTH_URL": "nats://127.0.0.1:${auth.clientPort}",
        "NATS_TOKEN_AUTH_URL": "nats://127.0.0.1:${tokenAuth.clientPort}",
        "NATS_CREDENTIALS_URL":
            "nats://127.0.0.1:${credentialsAuth.clientPort}",
        "NATS_CREDENTIALS_FILE": credentials,
      });
    } on Object catch (error, stackTrace) {
      try {
        await _closeServersAndDelete(servers.values, root);
      } on Object catch (cleanupError) {
        stderr.writeln(
          "Native security startup cleanup also failed: $cleanupError",
        );
      }
      Error.throwWithStackTrace(error, stackTrace);
    }
  }

  Future<void> close() => _closeServersAndDelete(_servers.values, directory);

  void writeServerLogs(IOSink output) {
    for (final entry in _servers.entries) {
      output.writeln("Native security server ${entry.key} logs:");
      for (final line in entry.value.logs) {
        output.writeln(line);
      }
    }
  }
}

void validateNativeSecurityFixtures(Directory fixtures) {
  final missing =
      nativeSecurityFixtureFiles
          .where(
            (name) => !File(
              "${fixtures.path}${Platform.pathSeparator}$name",
            ).existsSync(),
          )
          .toList(growable: false)
        ..sort();
  if (missing.isNotEmpty) {
    throw StateError(
      "Native security fixtures are missing: ${missing.join(", ")}",
    );
  }
}

String natsConfigPath(String path) => jsonEncode(path.replaceAll(r"\", "/"));

String tlsServerConfig({
  required String certificate,
  required String key,
  required String ca,
  bool handshakeFirst = false,
  bool verifyClients = false,
}) =>
    """
tls {
  cert_file: ${natsConfigPath(certificate)}
  key_file: ${natsConfigPath(key)}
  ca_file: ${natsConfigPath(ca)}
  handshake_first: $handshakeFirst
  verify: $verifyClients
  timeout: 5
}
""";

String webSocketServerConfig({
  required int port,
  String? certificate,
  String? key,
}) {
  if ((certificate == null) != (key == null)) {
    throw ArgumentError(
      "WebSocket certificate and key must be supplied together",
    );
  }
  final tls = certificate == null
      ? "no_tls: true"
      : """
tls {
    cert_file: ${natsConfigPath(certificate)}
    key_file: ${natsConfigPath(key!)}
  }""";
  return """
websocket {
  host: "127.0.0.1"
  port: $port
  $tls
}
""";
}

const nativeAuthenticationConfig = """
max_subscriptions: 2
authorization {
  users: [
    {
      user: "alice"
      password: "wonderland"
      permissions: {
        publish: {allow: ["allowed.>"]}
        subscribe: {allow: ["allowed.>"]}
      }
    },
    {nkey: "UD466L6EBCM3YY5HEGHJANNTN4LSKTSUXTH7RILHCKEQMQHTBNLHJJXT"}
  ]
}
""";

const nativeTokenAuthenticationConfig = """
authorization {
  token: "latest-token"
}
""";

Future<NatsServerProcess> _startServer({
  required String binary,
  required Directory root,
  required String name,
  required String config,
}) async {
  final configFile = File("${root.path}${Platform.pathSeparator}$name.conf");
  await configFile.writeAsString(config);
  return NatsServerProcess.start(
    binary: binary,
    clientPort: await reserveLoopbackPort(),
    monitorPort: await reserveLoopbackPort(),
    configPath: configFile.path,
    storeDirectory: "${root.path}${Platform.pathSeparator}$name-store",
    jetStream: false,
    requireJetStreamReady: false,
  );
}

Future<void> _closeServers(Iterable<NatsServerProcess> servers) async {
  Object? firstFailure;
  StackTrace? firstStackTrace;
  for (final server in servers.toList().reversed) {
    try {
      await server.stop();
    } on Object catch (error, stackTrace) {
      firstFailure ??= error;
      firstStackTrace ??= stackTrace;
    }
  }
  if (firstFailure != null) {
    Error.throwWithStackTrace(firstFailure, firstStackTrace!);
  }
}

Future<void> _closeServersAndDelete(
  Iterable<NatsServerProcess> servers,
  Directory directory,
) async {
  Object? firstFailure;
  StackTrace? firstStackTrace;
  try {
    await _closeServers(servers);
  } on Object catch (error, stackTrace) {
    firstFailure = error;
    firstStackTrace = stackTrace;
  }
  try {
    if (directory.existsSync()) {
      await directory.delete(recursive: true);
    }
  } on Object catch (error, stackTrace) {
    firstFailure ??= error;
    firstStackTrace ??= stackTrace;
  }
  if (firstFailure != null) {
    Error.throwWithStackTrace(firstFailure, firstStackTrace!);
  }
}
