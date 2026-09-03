import "dart:async";
import "dart:convert";
import "dart:io";

import "package:test/test.dart";

import "../tool/nats_service_readiness.dart";

void main() {
  test("HTTP headers and body share one total readiness deadline", () async {
    final fixture = await _HttpFixture.start(
      (request) =>
          _splitHealthResponse(request, const Duration(milliseconds: 300)),
    );
    addTearDown(fixture.close);
    const timeout = Duration(milliseconds: 450);
    final elapsed = Stopwatch()..start();

    await expectLater(
      waitForNatsService(
        serverUrl: "nats://127.0.0.1:1",
        monitorUrl: fixture.url,
        environment: const {},
        timeout: timeout,
        pollInterval: Duration.zero,
      ),
      throwsA(
        isA<StateError>().having(
          (error) => error.message,
          "message",
          contains("HTTP response body"),
        ),
      ),
    );
    elapsed.stop();

    expect(fixture.requests, 1);
    expect(
      elapsed.elapsed,
      lessThan(timeout + const Duration(milliseconds: 250)),
    );
  }, timeout: const Timeout(Duration(seconds: 3)));

  test(
    "health phases and NATS connect share one total readiness deadline",
    () async {
      final monitor = await _HttpFixture.start(
        (request) =>
            _delayedHealthResponse(request, const Duration(milliseconds: 120)),
      );
      addTearDown(monitor.close);
      final nats = await _NatsFixture.start();
      addTearDown(nats.close);
      const timeout = Duration(seconds: 2);
      final elapsed = Stopwatch()..start();

      await expectLater(
        waitForNatsService(
          serverUrl: nats.url,
          monitorUrl: monitor.url,
          environment: const {},
          requireJetStream: true,
          timeout: timeout,
          pollInterval: Duration.zero,
        ),
        throwsA(
          isA<StateError>().having(
            (error) => error.message,
            "message",
            contains("connect=not completed"),
          ),
        ),
      );
      elapsed.stop();

      expect(monitor.requests, 2);
      expect(nats.acceptedConnections, 1);
      expect(
        elapsed.elapsed,
        lessThan(timeout + const Duration(milliseconds: 500)),
      );
    },
    timeout: const Timeout(Duration(seconds: 5)),
  );

  test(
    "health, connect, flush, and cleanup share one total deadline",
    () async {
      final monitor = await _HttpFixture.start(
        (request) =>
            _delayedHealthResponse(request, const Duration(milliseconds: 120)),
      );
      addTearDown(monitor.close);
      final nats = await _NatsFixture.start(sendInfo: true, pongLimit: 1);
      addTearDown(nats.close);
      const timeout = Duration(milliseconds: 350);
      final elapsed = Stopwatch()..start();

      await expectLater(
        waitForNatsService(
          serverUrl: nats.url,
          monitorUrl: monitor.url,
          environment: const {},
          timeout: timeout,
          pollInterval: Duration.zero,
        ),
        throwsA(
          isA<StateError>().having(
            (error) => error.message,
            "message",
            contains("Flush timed out"),
          ),
        ),
      );
      elapsed.stop();

      expect(monitor.requests, 1);
      expect(nats.acceptedConnections, 1);
      expect(nats.receivedPings, 2);
      expect(
        elapsed.elapsed,
        lessThan(timeout + const Duration(milliseconds: 150)),
      );
    },
    timeout: const Timeout(Duration(seconds: 3)),
  );
}

Future<void> _splitHealthResponse(HttpRequest request, Duration delay) async {
  try {
    await Future<void>.delayed(delay);
    request.response
      ..headers.contentType = ContentType.json
      ..write('{"status":');
    await request.response.flush();
    await Future<void>.delayed(delay);
    request.response.write('"ok"}');
    await request.response.close();
  } on Object {
    unawaited(
      request.response.close().then<void>(
        (_) {},
        onError: (Object _, StackTrace _) {},
      ),
    );
  }
}

Future<void> _delayedHealthResponse(HttpRequest request, Duration delay) async {
  try {
    await Future<void>.delayed(delay);
    request.response
      ..headers.contentType = ContentType.json
      ..write('{"status":"ok"}');
    await request.response.close();
  } on Object {
    unawaited(
      request.response.close().then<void>(
        (_) {},
        onError: (Object _, StackTrace _) {},
      ),
    );
  }
}

final class _HttpFixture {
  _HttpFixture._(this._server, this._subscription, this._tasks);

  final HttpServer _server;
  final StreamSubscription<HttpRequest> _subscription;
  final List<Future<void>> _tasks;
  var requests = 0;

  String get url => "http://${_server.address.address}:${_server.port}";

  static Future<_HttpFixture> start(
    Future<void> Function(HttpRequest request) handler,
  ) async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    final tasks = <Future<void>>[];
    late final _HttpFixture fixture;
    // Subscription ownership transfers to the fixture.
    // ignore: cancel_subscriptions
    final subscription = server.listen((request) {
      fixture.requests += 1;
      tasks.add(handler(request));
    });
    // ignore: join_return_with_assignment
    fixture = _HttpFixture._(server, subscription, tasks);
    return fixture;
  }

  Future<void> close() async {
    await _subscription.cancel();
    await _server.close(force: true);
    await Future.wait(_tasks);
  }
}

final class _NatsFixture {
  _NatsFixture._(
    this._server,
    this._subscription,
    this._sockets,
    this._readers,
  );

  final ServerSocket _server;
  final StreamSubscription<Socket> _subscription;
  final List<Socket> _sockets;
  final List<StreamSubscription<List<int>>> _readers;
  var acceptedConnections = 0;
  var receivedPings = 0;

  String get url => "nats://${_server.address.address}:${_server.port}";

  static Future<_NatsFixture> start({
    bool sendInfo = false,
    int pongLimit = 0,
  }) async {
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final sockets = <Socket>[];
    final readers = <StreamSubscription<List<int>>>[];
    late final _NatsFixture fixture;
    // Subscription ownership transfers to the fixture.
    // ignore: cancel_subscriptions
    final subscription = server.listen((socket) {
      fixture.acceptedConnections += 1;
      sockets.add(socket);
      if (!sendInfo) return;
      var incoming = "";
      // Reader ownership transfers to the fixture.
      // ignore: cancel_subscriptions
      final reader = socket.listen((bytes) {
        incoming += ascii.decode(bytes);
        while (true) {
          final lineEnd = incoming.indexOf("\r\n");
          if (lineEnd == -1) return;
          final line = incoming.substring(0, lineEnd);
          incoming = incoming.substring(lineEnd + 2);
          if (line != "PING") continue;
          fixture.receivedPings += 1;
          if (fixture.receivedPings <= pongLimit) {
            socket.add(ascii.encode("PONG\r\n"));
          }
        }
      });
      readers.add(reader);
      socket.add(
        ascii.encode(
          "INFO {\"server_id\":\"fixture\",\"headers\":true,"
          "\"max_payload\":1048576}\r\n",
        ),
      );
    });
    // ignore: join_return_with_assignment
    fixture = _NatsFixture._(server, subscription, sockets, readers);
    return fixture;
  }

  Future<void> close() async {
    await _subscription.cancel();
    for (final reader in _readers) {
      await reader.cancel();
    }
    for (final socket in _sockets) {
      socket.destroy();
    }
    await _server.close();
  }
}
