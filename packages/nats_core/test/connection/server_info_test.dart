@TestOn("vm")
library;

import "dart:async";
import "dart:convert";
import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:test/test.dart";

void main() {
  test("excessively nested INFO fails as a bounded protocol error", () async {
    final peer = await _InfoPeer.start({"nested": _nestedLists(66)});
    addTearDown(peer.close);

    await expectLater(
      NatsConnection.connect(
        NatsOptions(
          servers: [
            NatsServer.nats(
              InternetAddress.loopbackIPv4.address,
              port: peer.port,
            ),
          ],
          reconnect: NatsReconnectPolicy(maximumAttempts: 0),
        ),
      ),
      throwsA(isA<NatsProtocolException>()),
    );
  });

  test("CONNECT serializes echo and INFO snapshots future fields", () async {
    final peer = await _InfoPeer.start({
      "server_id": "scripted",
      "server_name": "primary",
      "version": "2.14.6",
      "host": "127.0.0.1",
      "port": 4222,
      "client_id": 42,
      "client_ip": "127.0.0.2",
      "proto": 1,
      "jetstream": true,
      "tls_available": false,
      "tls_required": false,
      "headers": true,
      "max_payload": 16 * 1024 * 1024,
      "connect_urls": ["node-a.example:4223"],
      "future": {
        "nested": [1, 2],
      },
    });
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [
          NatsServer.nats(
            InternetAddress.loopbackIPv4.address,
            port: peer.port,
          ),
        ],
        name: "info-client",
        echo: false,
        reconnect: NatsReconnectPolicy(maximumAttempts: 0),
      ),
    );

    expect(
      await peer.connectLine,
      "CONNECT {\"verbose\":false,\"pedantic\":false,\"lang\":\"dart\","
      "\"version\":\"0.0.1\",\"protocol\":1,\"echo\":false,"
      "\"headers\":true,\"no_responders\":true,\"tls_required\":false,"
      "\"name\":\"info-client\"}",
    );
    final initial = connection.serverInfo;
    expect(initial.serverAuthority, "127.0.0.1:${peer.port}");
    expect(initial.serverAuthority, isNot(contains("@")));
    expect(initial.headersSupported, isTrue);
    expect(initial.maximumPayloadBytes, 8 * 1024 * 1024);
    expect(initial.discoveredServerAuthorities, ["node-a.example:4223"]);
    expect(initial.isLameDuck, isFalse);
    expect(initial.rawFields["server_id"], "scripted");
    expect(initial.serverId, "scripted");
    expect(initial.serverName, "primary");
    expect(initial.serverVersion, "2.14.6");
    expect(initial.host, "127.0.0.1");
    expect(initial.port, 4222);
    expect(initial.clientId, 42);
    expect(initial.clientIp, "127.0.0.2");
    expect(initial.protocolVersion, 1);
    expect(initial.jetStreamAvailable, isTrue);
    expect(initial.tlsAvailable, isFalse);
    expect(initial.tlsRequired, isFalse);
    final future = initial.rawFields["future"]! as Map<String, Object?>;
    final nested = future["nested"]! as List<Object?>;
    expect(() => nested.add(3), throwsUnsupportedError);
    expect(() => initial.rawFields["changed"] = true, throwsUnsupportedError);

    await peer.sendInfo({
      "headers": "invalid",
      "max_payload": "invalid",
      "connect_urls": "invalid",
      "ldm": "invalid",
      "server_id": "replacement",
      "server_name": 42,
      "version": false,
      "host": [],
      "port": 65536,
      "client_id": -1,
      "client_ip": 42,
      "proto": -1,
      "jetstream": "invalid",
      "tls_available": "invalid",
      "tls_required": "invalid",
    });
    await connection.flush();
    final afterMalformedInfo = connection.serverInfo;
    expect(afterMalformedInfo.headersSupported, isTrue);
    expect(afterMalformedInfo.maximumPayloadBytes, 8 * 1024 * 1024);
    expect(afterMalformedInfo.discoveredServerAuthorities, [
      "node-a.example:4223",
    ]);
    expect(afterMalformedInfo.isLameDuck, isFalse);
    expect(afterMalformedInfo.serverId, "scripted");
    expect(afterMalformedInfo.serverName, "primary");
    expect(afterMalformedInfo.serverVersion, "2.14.6");
    expect(afterMalformedInfo.host, "127.0.0.1");
    expect(afterMalformedInfo.port, 4222);
    expect(afterMalformedInfo.clientId, 42);
    expect(afterMalformedInfo.clientIp, "127.0.0.2");
    expect(afterMalformedInfo.protocolVersion, 1);
    expect(afterMalformedInfo.jetStreamAvailable, isTrue);
    expect(afterMalformedInfo.tlsAvailable, isFalse);
    expect(afterMalformedInfo.tlsRequired, isFalse);

    final events = <NatsLameDuckMode>[];
    final listener = connection.events
        .where((event) => event is NatsLameDuckMode)
        .cast<NatsLameDuckMode>()
        .listen(events.add);
    await peer.sendInfo({
      "ldm": true,
      "connect_urls": ["node-b.example:4333"],
      "future_after": {"value": 2},
    });
    await connection.flush();
    await peer.sendInfo({"ldm": true});
    await connection.flush();

    expect(events, hasLength(1));
    expect(events.single.generation, connection.generation);
    expect(events.single.serverAuthority, "127.0.0.1:${peer.port}");
    final lameDuck = connection.serverInfo;
    expect(lameDuck.isLameDuck, isTrue);
    expect(lameDuck.rawFields["server_id"], "scripted");
    expect(lameDuck.rawFields["future_after"], {"value": 2});
    expect(lameDuck.discoveredServerAuthorities, ["node-b.example:4333"]);
    expect(initial.isLameDuck, isFalse);

    await peer.sendInfo({"ldm": false});
    await connection.flush();
    await peer.sendInfo({"ldm": true});
    await connection.flush();
    expect(events, hasLength(2));

    await listener.cancel();
    await connection.close();
    await peer.close();
  });

  test("discovery updates own one bounded atomic implicit pool", () async {
    final peer = await _InfoPeer.start({
      "server_id": "topology",
      "headers": true,
      "max_payload": 1024 * 1024,
      "connect_urls": ["node-a.example:4223", "configured.example:4224"],
    });
    final seed = NatsServer.nats(
      InternetAddress.loopbackIPv4.address,
      port: peer.port,
    );
    const configured = NatsServer.nats("configured.example", port: 4224);
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [seed, configured],
        reconnect: NatsReconnectPolicy(maximumAttempts: 0),
      ),
    );
    addTearDown(() async {
      await connection.close();
      await peer.close();
    });

    expect(connection.getServers().map((server) => server.uri.authority), [
      seed.uri.authority,
      configured.uri.authority,
      "node-a.example:4223",
    ]);
    expect(connection.serverInfo.discoveredServerAuthorities, [
      "node-a.example:4223",
    ]);

    await peer.sendInfo({
      "connect_urls": ["node-b.example:4333"],
    });
    await connection.flush();
    expect(connection.getServers().map((server) => server.uri.authority), [
      seed.uri.authority,
      configured.uri.authority,
      "node-b.example:4333",
    ]);
    expect(connection.serverInfo.discoveredServerAuthorities, [
      "node-b.example:4333",
    ]);

    final beforeMalformed = connection.getServers();
    await peer.sendInfo({
      "connect_urls": ["node-c.example:4444", 42],
    });
    await connection.flush();
    expect(connection.getServers(), beforeMalformed);
    expect(connection.serverInfo.discoveredServerAuthorities, [
      "node-b.example:4333",
    ]);

    for (var index = 0; index < 50; index += 1) {
      await peer.sendInfo({
        "connect_urls": ["node-$index.example:${4500 + index}"],
      });
    }
    await connection.flush();
    expect(connection.getServers(), hasLength(3));
    expect(connection.serverInfo.discoveredServerAuthorities, [
      "node-49.example:4549",
    ]);

    const replacement = NatsServer.nats("replacement.example", port: 4555);
    connection.setServers([replacement]);
    expect(connection.getServers(), [replacement]);
    expect(connection.serverInfo.discoveredServerAuthorities, isEmpty);
  });

  test("discovery count is bounded and replacement stays atomic", () async {
    const maximum = 2048;
    final peer = await _InfoPeer.start({
      "server_id": "bounded-topology",
      "headers": true,
      "max_payload": 1024 * 1024,
    });
    final seed = NatsServer.nats(
      InternetAddress.loopbackIPv4.address,
      port: peer.port,
    );
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [seed],
        maximumDiscoveredServers: maximum,
        reconnect: NatsReconnectPolicy(maximumAttempts: 0),
      ),
    );
    addTearDown(() async {
      await connection.close();
      await peer.close();
    });

    final exact = [
      for (var index = 0; index < maximum; index += 1)
        "node-$index.example:${5000 + index}",
    ];
    final clock = Stopwatch()..start();
    await peer.sendInfo({"connect_urls": exact});
    await connection.flush();
    clock.stop();
    expect(clock.elapsed, lessThan(const Duration(seconds: 2)));
    expect(connection.serverInfo.discoveredServerAuthorities, exact);
    expect(connection.getServers(), hasLength(maximum + 1));

    final beforeOverflow = connection.getServers();
    await peer.sendInfo({
      "connect_urls": [...exact, "overflow.example:9000"],
    });
    await connection.flush();
    expect(connection.getServers(), beforeOverflow);
    expect(connection.serverInfo.discoveredServerAuthorities, exact);
  });

  test("trace headers fail before write when not negotiated", () async {
    final peer = await _InfoPeer.start({
      "server_id": "no-headers",
      "headers": false,
      "max_payload": 1024 * 1024,
    });
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [
          NatsServer.nats(
            InternetAddress.loopbackIPv4.address,
            port: peer.port,
          ),
        ],
        reconnect: NatsReconnectPolicy(maximumAttempts: 0),
      ),
    );
    final trace = NatsTraceOptions(destination: "trace.events");

    await expectLater(
      connection.publish("events", const [1], trace: trace),
      throwsA(isA<NatsHeaderException>()),
    );
    await expectLater(
      connection.publishOnGeneration(
        "events",
        const [1],
        generation: connection.generation,
        trace: trace,
      ),
      throwsA(isA<NatsHeaderException>()),
    );
    await connection.flush();

    expect(
      peer.receivedLines.where((line) => line.startsWith("HPUB ")),
      isEmpty,
    );
    await connection.close();
    await peer.close();
  });

  test("request headers fail before mux or wire mutation", () async {
    final peer = await _InfoPeer.start({
      "server_id": "no-request-headers",
      "headers": false,
      "max_payload": 1024 * 1024,
    });
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [
          NatsServer.nats(
            InternetAddress.loopbackIPv4.address,
            port: peer.port,
          ),
        ],
        reconnect: NatsReconnectPolicy(maximumAttempts: 0),
      ),
    );
    final trace = NatsTraceOptions(destination: "trace.events");

    await expectLater(
      connection.request("service.one", const [1], trace: trace),
      throwsA(isA<NatsHeaderException>()),
    );
    await expectLater(
      Future.sync(
        () => connection.requestMany("service.many", const [2], trace: trace),
      ),
      throwsA(isA<NatsHeaderException>()),
    );
    await connection.flush();

    expect(connection.statistics.activeRequests, 0);
    expect(
      peer.receivedLines.where(
        (line) =>
            line.startsWith("SUB ") ||
            line.startsWith("PUB ") ||
            line.startsWith("HPUB "),
      ),
      isEmpty,
    );
    await connection.close();
    await peer.close();
  });

  test("oversized requests fail before mux or wire mutation", () async {
    final peer = await _InfoPeer.start({
      "server_id": "small-payload",
      "headers": true,
      "max_payload": 4,
    });
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [
          NatsServer.nats(
            InternetAddress.loopbackIPv4.address,
            port: peer.port,
          ),
        ],
        reconnect: NatsReconnectPolicy(maximumAttempts: 0),
      ),
    );

    await expectLater(
      connection.request("service.one", const [1, 2, 3, 4, 5]),
      throwsA(isA<NatsMaxPayloadException>()),
    );
    await expectLater(
      Future.sync(
        () => connection.requestMany("service.many", const [1, 2, 3, 4, 5]),
      ),
      throwsA(isA<NatsMaxPayloadException>()),
    );
    await connection.flush();

    expect(connection.statistics.activeRequests, 0);
    expect(
      peer.receivedLines.where(
        (line) =>
            line.startsWith("SUB ") ||
            line.startsWith("PUB ") ||
            line.startsWith("HPUB "),
      ),
      isEmpty,
    );
    await connection.close();
    await peer.close();
  });

  test("requests snapshot payloads before inbox setup yields", () async {
    final peer = await _InfoPeer.start({
      "server_id": "payload-snapshot",
      "headers": true,
      "max_payload": 1024 * 1024,
    });
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [
          NatsServer.nats(
            InternetAddress.loopbackIPv4.address,
            port: peer.port,
          ),
        ],
        reconnect: NatsReconnectPolicy(maximumAttempts: 0),
      ),
    );
    final singlePayload = <int>[65, 65, 65];
    final manyPayload = <int>[67, 67, 67];

    final singleResult = connection
        .request("service.snapshot.one", singlePayload)
        .then<Object?>((message) => message, onError: (Object error) => error);
    singlePayload.fillRange(0, singlePayload.length, 66);
    final manyFuture = connection.requestMany(
      "service.snapshot.many",
      manyPayload,
    );
    manyPayload.fillRange(0, manyPayload.length, 68);

    await peer.waitForLine("AAA").timeout(const Duration(seconds: 1));
    await peer.waitForLine("CCC").timeout(const Duration(seconds: 1));
    final many = await manyFuture;
    await many.stop();
    await connection.close();
    expect(await singleResult, isA<NatsClosedException>());
    expect(peer.receivedLines, isNot(contains("BBB")));
    expect(peer.receivedLines, isNot(contains("DDD")));
    await peer.close();
  });
}

final class _InfoPeer {
  _InfoPeer._(this._listener, this._initialInfo);

  final ServerSocket _listener;
  final Map<String, Object?> _initialInfo;
  final Completer<String> _connectLine = Completer<String>();
  final Map<String, List<Completer<void>>> _lineWaiters = {};
  StreamSubscription<List<int>>? _reader;
  Socket? _socket;
  String _incoming = "";
  final List<String> _receivedLines = [];

  int get port => _listener.port;

  Future<String> get connectLine => _connectLine.future;

  List<String> get receivedLines => List.unmodifiable(_receivedLines);

  Future<void> waitForLine(String line) {
    if (_receivedLines.contains(line)) {
      return Future.value();
    }
    final completer = Completer<void>();
    _lineWaiters.putIfAbsent(line, () => []).add(completer);
    return completer.future;
  }

  static Future<_InfoPeer> start(Map<String, Object?> initialInfo) async {
    final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final peer = _InfoPeer._(listener, initialInfo);
    listener.first.then(peer._accept).ignore();
    return peer;
  }

  void _accept(Socket socket) {
    _socket = socket;
    _reader = socket.listen(_onData);
    socket.add(ascii.encode("INFO ${jsonEncode(_initialInfo)}\r\n"));
  }

  void _onData(List<int> bytes) {
    _incoming += ascii.decode(bytes);
    while (true) {
      final end = _incoming.indexOf("\r\n");
      if (end == -1) {
        return;
      }
      final line = _incoming.substring(0, end);
      _incoming = _incoming.substring(end + 2);
      _receivedLines.add(line);
      for (final waiter
          in _lineWaiters.remove(line) ?? const <Completer<void>>[]) {
        waiter.complete();
      }
      if (line.startsWith("CONNECT ") && !_connectLine.isCompleted) {
        _connectLine.complete(line);
      }
      if (line == "PING") {
        _socket?.add(ascii.encode("PONG\r\n"));
      }
    }
  }

  Future<void> sendInfo(Map<String, Object?> fields) async {
    await (_socket!..add(ascii.encode("INFO ${jsonEncode(fields)}\r\n")))
        .flush();
  }

  Future<void> close() async {
    await _reader?.cancel();
    await _socket?.close();
    await _listener.close();
  }
}

Object? _nestedLists(int count) {
  Object? value = true;
  for (var index = 0; index < count; index += 1) {
    value = <Object?>[value];
  }
  return value;
}
