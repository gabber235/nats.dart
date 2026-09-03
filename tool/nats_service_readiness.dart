import "dart:async";
import "dart:convert";
import "dart:io";

import "package:nats_core/nats_core.dart";

const _defaultTimeout = Duration(seconds: 20);
const _defaultProbeTimeout = Duration(seconds: 2);
const _defaultPollInterval = Duration(milliseconds: 100);

Future<void> main() async {
  final environment = Platform.environment;
  await waitForNatsService(
    serverUrl: _required(environment, "NATS_READINESS_URL"),
    monitorUrl: _required(environment, "NATS_READINESS_MONITOR_URL"),
    environment: environment,
    requireJetStream: environment["NATS_READINESS_JETSTREAM"] == "true",
  );
}

/// Waits until health checks and one NATS connect plus flush all succeed.
///
/// [timeout] is one absolute deadline across every attempt and cleanup.
/// [probeTimeout] limits one operation without extending that deadline.
Future<void> waitForNatsService({
  required String serverUrl,
  required String monitorUrl,
  required Map<String, String> environment,
  bool requireJetStream = false,
  Duration timeout = _defaultTimeout,
  Duration probeTimeout = _defaultProbeTimeout,
  Duration pollInterval = _defaultPollInterval,
}) async {
  if (timeout <= Duration.zero) {
    throw ArgumentError.value(timeout, "timeout", "must be positive");
  }
  if (probeTimeout <= Duration.zero) {
    throw ArgumentError.value(probeTimeout, "probeTimeout", "must be positive");
  }
  if (pollInterval < Duration.zero) {
    throw ArgumentError.value(
      pollInterval,
      "pollInterval",
      "must not be negative",
    );
  }
  final deadline = _ReadinessDeadline(timeout, probeTimeout);
  final readinessEnvironment = Map<String, String>.unmodifiable(environment);
  var attempts = 0;
  var lastObservation = "no probe completed";

  while (!deadline.expired) {
    attempts += 1;
    var ordinary = "not completed";
    var jetStream = requireJetStream ? "not completed" : "not required";
    var connect = "not completed";
    try {
      ordinary = await _health(Uri.parse("$monitorUrl/healthz"), deadline);
      if (requireJetStream) {
        jetStream = await _health(
          Uri.parse("$monitorUrl/healthz?js-enabled-only=true"),
          deadline,
        );
      }
      await _connectAndFlush(serverUrl, readinessEnvironment, deadline);
      connect = "ready";
      stdout.writeln(
        "NATS ready after $attempts attempt(s): health=$ordinary, "
        "jetstream=$jetStream, connect=$connect",
      );
      return;
    } on Object catch (error) {
      lastObservation =
          "health=$ordinary, jetstream=$jetStream, connect=$connect, "
          "error=${_safeError(error)}";
    }

    await deadline.pause(pollInterval);
  }

  throw StateError(
    "NATS readiness failed after $attempts attempt(s) within "
    "${timeout.inMilliseconds} milliseconds. "
    "Last observation: $lastObservation. "
    "The Dagger trace retains the bound server stdout and stderr logs.",
  );
}

Future<String> _health(Uri uri, _ReadinessDeadline deadline) async {
  final client = HttpClient();
  try {
    final request = await deadline.run("HTTP connection", (budget) {
      client.connectionTimeout = budget;
      return client.getUrl(uri);
    });
    final response = await deadline.run("HTTP response headers", (_) {
      return request.close();
    });
    final body = await deadline.run("HTTP response body", (_) {
      return utf8.decoder.bind(response).join();
    });
    if (response.statusCode != HttpStatus.ok) {
      final boundedBody = _boundedBody(body);
      throw HttpException(
        "${uri.path} returned HTTP ${response.statusCode}: $boundedBody",
        uri: uri,
      );
    }
    final decoded = jsonDecode(body);
    if (decoded is! Map<String, Object?> || decoded["status"] != "ok") {
      throw FormatException(
        "${uri.path} returned an unexpected body: ${_boundedBody(body)}",
      );
    }
    return "HTTP ${response.statusCode} ${decoded["status"]}";
  } finally {
    client.close(force: true);
  }
}

Future<void> _connectAndFlush(
  String serverUrl,
  Map<String, String> environment,
  _ReadinessDeadline deadline,
) async {
  final tls = await _tlsOptions(environment, deadline);
  final authentication = await _authentication(environment, deadline);
  NatsConnection? connection;
  Object? primaryFailure;
  StackTrace? primaryStackTrace;
  try {
    final parsedServer = NatsServer.parse(serverUrl);
    final server = environment["NATS_READINESS_TLS_FIRST"] == "true"
        ? NatsServer.tlsFirst(parsedServer.host, port: parsedServer.port)
        : parsedServer;
    connection = await deadline.run("NATS connect", (budget) {
      return NatsConnection.connect(
        NatsOptions(
          servers: [server],
          name: "dagger-readiness",
          connectTimeout: budget,
          flushTimeout: budget,
          tls: tls,
          authentication: authentication,
          reconnect: NatsReconnectPolicy(maximumAttempts: 0),
        ),
      );
    });
    await deadline.run("NATS flush", (budget) {
      return connection!.flush(timeout: budget);
    });
  } on Object catch (error, stackTrace) {
    primaryFailure = error;
    primaryStackTrace = stackTrace;
  }

  Object? cleanupFailure;
  StackTrace? cleanupStackTrace;
  if (connection != null) {
    try {
      await deadline.cleanup("NATS close", connection.close);
    } on Object catch (error, stackTrace) {
      cleanupFailure = error;
      cleanupStackTrace = stackTrace;
    }
  }
  if (primaryFailure != null) {
    Error.throwWithStackTrace(primaryFailure, primaryStackTrace!);
  }
  if (cleanupFailure != null) {
    Error.throwWithStackTrace(cleanupFailure, cleanupStackTrace!);
  }
}

Future<NatsTlsOptions> _tlsOptions(
  Map<String, String> environment,
  _ReadinessDeadline deadline,
) async {
  final caPath = environment["NATS_READINESS_CA"];
  final certificatePath = environment["NATS_READINESS_CLIENT_CERTIFICATE"];
  final keyPath = environment["NATS_READINESS_CLIENT_KEY"];
  return NatsTlsOptions(
    trustedCertificates: caPath == null
        ? null
        : await deadline.run("read trusted certificates", (_) {
            return File(caPath).readAsBytes();
          }),
    clientCertificateChain: certificatePath == null
        ? null
        : await deadline.run("read client certificate", (_) {
            return File(certificatePath).readAsBytes();
          }),
    clientPrivateKey: keyPath == null
        ? null
        : await deadline.run("read client private key", (_) {
            return File(keyPath).readAsBytes();
          }),
  );
}

Future<NatsAuthentication?> _authentication(
  Map<String, String> environment,
  _ReadinessDeadline deadline,
) async {
  if (environment["NATS_READINESS_CREDENTIALS_FILE"] case final path?) {
    return deadline.run("read credentials file", (_) {
      return loadNatsCredentialsFile(path);
    });
  }
  if (environment["NATS_READINESS_JWT_FILE"] case final path?) {
    final jwt = await deadline.run("read JWT file", (_) {
      return File(path).readAsString();
    });
    return NatsAuthentication.provider(
      (_) async => NatsConnectCredentials(jwt: jwt),
    );
  }
  if (environment["NATS_READINESS_TOKEN"] case final token?) {
    return NatsAuthentication.token(token);
  }
  final username = environment["NATS_READINESS_USERNAME"];
  final password = environment["NATS_READINESS_PASSWORD"];
  if ((username == null) != (password == null)) {
    throw StateError(
      "Readiness username and password must be supplied together",
    );
  }
  return username == null
      ? null
      : NatsAuthentication.userPassword(username, password!);
}

final class _ReadinessDeadline {
  _ReadinessDeadline(this.timeout, this.probeTimeout)
    : _elapsed = Stopwatch()..start();

  final Stopwatch _elapsed;
  final Duration probeTimeout;
  final Duration timeout;

  bool get expired => _elapsed.elapsed >= timeout;

  Duration _operationBudget(String phase) {
    final remaining = timeout - _elapsed.elapsed;
    if (remaining <= Duration.zero) throw _timeout(phase);
    return remaining < probeTimeout ? remaining : probeTimeout;
  }

  Future<T> run<T>(
    String phase,
    Future<T> Function(Duration budget) operation,
  ) {
    final budget = _operationBudget(phase);
    return operation(
      budget,
    ).timeout(budget, onTimeout: () => throw _timeout(phase));
  }

  Future<void> cleanup(String phase, Future<void> Function() operation) async {
    final cleanup = operation();
    final remaining = timeout - _elapsed.elapsed;
    if (remaining <= Duration.zero) {
      unawaited(
        cleanup.then<void>((_) {}, onError: (Object _, StackTrace _) {}),
      );
      throw _timeout(phase);
    }
    final budget = remaining < probeTimeout ? remaining : probeTimeout;
    await cleanup.timeout(budget, onTimeout: () => throw _timeout(phase));
  }

  Future<void> pause(Duration interval) async {
    if (interval == Duration.zero || expired) return;
    final remaining = timeout - _elapsed.elapsed;
    await Future<void>.delayed(remaining < interval ? remaining : interval);
  }

  TimeoutException _timeout(String phase) => TimeoutException(
    "NATS readiness exceeded its total deadline during $phase",
    timeout,
  );
}

String _required(Map<String, String> environment, String name) {
  final value = environment[name];
  if (value == null || value.isEmpty) {
    throw StateError("$name is required");
  }
  return value;
}

String _boundedBody(String body) {
  final normalized = body.replaceAll(RegExp(r"\s+"), " ").trim();
  return normalized.length <= 256 ? normalized : normalized.substring(0, 256);
}

String _safeError(Object error) {
  final text = error.toString().replaceAll(RegExp(r"\s+"), " ").trim();
  return text.length <= 512 ? text : text.substring(0, 512);
}
