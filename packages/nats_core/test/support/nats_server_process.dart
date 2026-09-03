import "dart:async";
import "dart:convert";
import "dart:io";

final class NatsServerProcess {
  NatsServerProcess._(
    this._process,
    this.clientPort,
    this.monitorPort,
    this._stdout,
    this._stderr,
    this._logs,
  );

  final Process _process;
  final int clientPort;
  final int monitorPort;
  final StreamSubscription<String> _stdout;
  final StreamSubscription<String> _stderr;
  final List<String> _logs;

  List<String> get logs => List.unmodifiable(_logs);

  static Future<NatsServerProcess> start({
    required String binary,
    required int clientPort,
    required int monitorPort,
    required String configPath,
    required String storeDirectory,
    bool jetStream = true,
    bool requireJetStreamReady = true,
  }) async {
    final arguments = [
      "-c",
      configPath,
      "-p",
      "$clientPort",
      "-m",
      "$monitorPort",
      if (jetStream) ...["-js", "-sd", storeDirectory],
    ];
    final process = await Process.start(binary, arguments);
    final logs = <String>[];
    // ignore: cancel_subscriptions
    final stdout = process.stdout
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .listen((line) => logs.add("stdout: $line"));
    // ignore: cancel_subscriptions
    final stderr = process.stderr
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .listen((line) => logs.add("stderr: $line"));
    final server = NatsServerProcess._(
      process,
      clientPort,
      monitorPort,
      stdout,
      stderr,
      logs,
    );
    try {
      await server._awaitReady(requireJetStreamReady);
      return server;
    } on Object {
      await server.stop();
      rethrow;
    }
  }

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

  Future<Map<String, Object?>> varz() async {
    final client = HttpClient();
    try {
      final request = await client.getUrl(monitorUri.replace(path: "/varz"));
      final response = await request.close();
      final decoded = jsonDecode(await utf8.decodeStream(response));
      if (response.statusCode == HttpStatus.ok &&
          decoded is Map<String, Object?>) {
        return decoded;
      }
      throw StateError("Invalid NATS varz response: $decoded");
    } finally {
      client.close(force: true);
    }
  }

  Future<void> _awaitReady(bool requireJetStreamReady) async {
    final deadline = DateTime.timestamp().add(const Duration(seconds: 10));
    Object? lastFailure;
    while (DateTime.timestamp().isBefore(deadline)) {
      final client = HttpClient();
      try {
        final request = await client.getUrl(
          monitorUri.replace(
            path: requireJetStreamReady ? "/healthz" : "/varz",
          ),
        );
        final response = await request.close();
        await response.drain<void>();
        if (response.statusCode == HttpStatus.ok) {
          return;
        }
        lastFailure = "HTTP ${response.statusCode}";
      } on Object catch (error) {
        lastFailure = error;
      } finally {
        client.close(force: true);
      }
      await Future<void>.delayed(const Duration(milliseconds: 20));
    }
    final serverLogs = _logs.join("\n");
    throw StateError(
      "NATS readiness timed out. Last failure: $lastFailure\n$serverLogs",
    );
  }

  Future<List<String>> subscriptionsFor(String clientName) async {
    final client = HttpClient();
    try {
      final request = await client.getUrl(
        monitorUri.replace(
          path: "/connz",
          queryParameters: const {
            "state": "open",
            "subs": "1",
            "subscriptions_detail": "true",
          },
        ),
      );
      final response = await request.close();
      final decoded = jsonDecode(await utf8.decodeStream(response));
      if (decoded case {"connections": final List<Object?> connections}) {
        for (final connection in connections) {
          if (connection case {
            "name": final String name,
            "subscriptions_list": final List<Object?> subscriptions,
          } when name == clientName) {
            return subscriptions
                .map(
                  (subscription) => switch (subscription) {
                    String() => subscription,
                    {"subject": final String subject} => subject,
                    _ => null,
                  },
                )
                .whereType<String>()
                .toList(growable: false);
          }
        }
      }
      return const [];
    } finally {
      client.close(force: true);
    }
  }

  Future<void> stop() async {
    _process.kill();
    try {
      await _process.exitCode.timeout(const Duration(seconds: 5));
    } on TimeoutException {
      _process.kill(ProcessSignal.sigkill);
      await _process.exitCode.timeout(const Duration(seconds: 5));
    } finally {
      await Future.wait([_stdout.cancel(), _stderr.cancel()]);
    }
  }

  Future<void> crash() async {
    _process.kill(ProcessSignal.sigkill);
    try {
      await _process.exitCode.timeout(const Duration(seconds: 5));
    } finally {
      await Future.wait([_stdout.cancel(), _stderr.cancel()]);
    }
  }

  void pause() {
    if (!_process.kill(ProcessSignal.sigstop)) {
      throw StateError("Could not suspend NATS Server");
    }
  }

  void resume() {
    if (!_process.kill(ProcessSignal.sigcont)) {
      throw StateError("Could not resume NATS Server");
    }
  }
}

Future<int> reserveLoopbackPort() async {
  final socket = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  final port = socket.port;
  await socket.close();
  return port;
}
