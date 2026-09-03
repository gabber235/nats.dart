@TestOn("vm")
library;

import "dart:async";
import "dart:convert";
import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:test/test.dart";

void main() {
  test("server pool snapshots are immutable redacted and atomic", () async {
    final peer = await _NatsPeer.start(
      connectUrls: const ["discovered.example:4333"],
    );
    final seed = NatsServer.nats(
      InternetAddress.loopbackIPv4.address,
      port: peer.port,
    );
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [seed],
        reconnect: NatsReconnectPolicy(maximumAttempts: 0),
      ),
    );
    addTearDown(() async {
      await connection.close();
      await peer.close();
    });

    final original = connection.getServers();
    expect(original.map((server) => server.uri.authority), [
      seed.uri.authority,
      "discovered.example:4333",
    ]);
    expect(
      original.every(
        (server) =>
            server.uri.userInfo.isEmpty && !server.toString().contains("@"),
      ),
      isTrue,
    );
    expect(
      () => original.add(const NatsServer.nats("other.example")),
      throwsUnsupportedError,
    );

    final replacement = <NatsServer>[
      const NatsServer.nats("replacement.example", port: 4555),
    ];
    connection.setServers(replacement);
    replacement.clear();
    expect(
      connection.getServers().single.uri.authority,
      "replacement.example:4555",
    );
    expect(connection.serverInfo.discoveredServerAuthorities, isEmpty);
    expect(original, hasLength(2));

    final beforeFailure = connection.getServers();
    expect(
      () => connection.setServers(const [
        NatsServer.nats("valid.example"),
        NatsServer.nats("bad host"),
      ]),
      throwsArgumentError,
    );
    expect(connection.getServers(), beforeFailure);
    expect(connection.isConnected, isTrue);
  });

  test("selector chooses reconnect server with bounded delay", () async {
    final first = await _NatsPeer.start();
    final second = await _NatsPeer.start();
    final unavailable = NatsServer.nats(
      InternetAddress.loopbackIPv4.address,
      port: await _reserveLoopbackPort(),
    );
    var calls = 0;
    NatsServerInfo? latestInfo;
    late final NatsConnection connection;
    connection = await NatsConnection.connect(
      NatsOptions(
        servers: [first.server],
        reconnect: NatsReconnectPolicy(
          maximumAttempts: 2,
          baseDelay: const Duration(milliseconds: 20),
          maximumDelay: const Duration(milliseconds: 20),
          jitter: 0,
        ),
        reconnectToServer: (pool, serverInfo) {
          calls += 1;
          expect(
            () => pool.add(const NatsServer.nats("mutation.example")),
            throwsUnsupportedError,
          );
          latestInfo = serverInfo;
          return NatsReconnectServerSelection(
            server: pool.last,
            delay: const Duration(milliseconds: 10),
          );
        },
      ),
    );
    addTearDown(() async {
      await connection.close();
      await first.close();
      await second.close();
    });
    connection.setServers([unavailable, second.server]);
    final reconnecting = connection.events
        .where((event) => event is NatsReconnecting)
        .cast<NatsReconnecting>()
        .first;
    final reconnected = connection.events
        .where((event) => event is NatsConnected)
        .cast<NatsConnected>()
        .firstWhere((event) => event.generation == 2);

    await first.close();
    final event = await reconnecting.timeout(const Duration(seconds: 2));
    await reconnected.timeout(const Duration(seconds: 2));

    expect(calls, 1);
    expect(latestInfo?.serverAuthority, first.server.uri.authority);
    expect(event.server.uri.authority, second.server.uri.authority);
    expect(event.delay, const Duration(milliseconds: 10));
    expect(connection.serverInfo.serverAuthority, second.server.uri.authority);
    expect(connection.statistics.reconnectAttempts, 1);
  });

  test("selector delay above reconnect maximum closes reconnect", () async {
    final peer = await _NatsPeer.start();
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [peer.server],
        reconnect: NatsReconnectPolicy(
          maximumAttempts: 1,
          baseDelay: const Duration(milliseconds: 20),
          maximumDelay: const Duration(milliseconds: 20),
        ),
        reconnectToServer: (pool, serverInfo) => NatsReconnectServerSelection(
          server: pool.first,
          delay: const Duration(milliseconds: 21),
        ),
      ),
    );
    addTearDown(() async {
      await connection.close();
      await peer.close();
    });
    final closed = connection.events
        .where((event) => event is NatsClosed)
        .cast<NatsClosed>()
        .first;

    await peer.close();
    final event = await closed.timeout(const Duration(seconds: 2));

    expect(event.error?.message, "Reconnect server selector failed");
    expect(event.error?.cause, isA<ArgumentError>());
  });

  test("selector failures do not render callback credentials", () async {
    const secret = "highly-secret-value";
    for (final selector in <NatsReconnectServerSelector>[
      (pool, serverInfo) => throw StateError(secret),
      (pool, serverInfo) => NatsReconnectServerSelection(
        server: const NatsServer.nats("user:$secret@nats.example"),
      ),
    ]) {
      final peer = await _NatsPeer.start();
      final connection = await NatsConnection.connect(
        NatsOptions(
          servers: [peer.server],
          reconnect: NatsReconnectPolicy(
            maximumAttempts: 1,
            baseDelay: const Duration(milliseconds: 20),
            maximumDelay: const Duration(milliseconds: 20),
          ),
          reconnectToServer: selector,
        ),
      );
      final closed = connection.events
          .where((event) => event is NatsClosed)
          .cast<NatsClosed>()
          .first;

      await peer.close();
      final event = await closed.timeout(const Duration(seconds: 2));
      await connection.close();

      expect(event.error, isA<NatsConnectionException>());
      expect(event.error.toString(), isNot(contains(secret)));
      expect(event.error?.cause, isNotNull);
    }
  });

  test("default reconnect selection remains round robin", () async {
    final first = await _NatsPeer.start();
    final second = await _NatsPeer.start();
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [first.server, second.server],
        reconnect: NatsReconnectPolicy(
          maximumAttempts: 1,
          baseDelay: const Duration(milliseconds: 20),
          maximumDelay: const Duration(milliseconds: 20),
          jitter: 0,
        ),
      ),
    );
    addTearDown(() async {
      await connection.close();
      await first.close();
      await second.close();
    });
    final reconnected = connection.events
        .where((event) => event is NatsConnected)
        .cast<NatsConnected>()
        .firstWhere((event) => event.generation == 2);

    await first.close();
    await reconnected.timeout(const Duration(seconds: 2));

    expect(connection.serverInfo.serverAuthority, second.server.uri.authority);
    expect(connection.statistics.reconnectAttempts, 1);
  });

  test("unsubscribe without a listener completes subscription done", () async {
    final peer = await _NatsPeer.start();
    final connection = await _connectWithoutReconnect(peer);
    addTearDown(() async {
      await connection.close();
      await peer.close();
    });
    final subscription = await connection.subscribe("unlistened.unsubscribe");

    await subscription.unsubscribe();

    await subscription.done.timeout(const Duration(seconds: 2));
  });

  test("subscription drain without a listener succeeds", () async {
    final peer = await _NatsPeer.start();
    final connection = await _connectWithoutReconnect(peer);
    addTearDown(() async {
      await connection.close();
      await peer.close();
    });
    final subscription = await connection.subscribe("unlistened.drain");

    await subscription.drain(timeout: const Duration(seconds: 2));

    await subscription.done.timeout(const Duration(seconds: 2));
  });

  test("connection close completes unlistened subscription done", () async {
    final peer = await _NatsPeer.start();
    final connection = await _connectWithoutReconnect(peer);
    addTearDown(peer.close);
    final subscription = await connection.subscribe("unlistened.close");

    await connection.close();

    await subscription.done.timeout(const Duration(seconds: 2));
  });

  test(
    "connection failure preserves typed unlistened terminal result",
    () async {
      final peer = await _NatsPeer.start();
      final connection = await _connectWithoutReconnect(peer);
      addTearDown(connection.close);
      final subscription = await connection.subscribe(
        "unlistened.terminal",
        options: NatsSubscriptionOptions(
          reconnect: NatsSubscriptionReconnectPolicy.close,
        ),
      );

      await peer.close();

      await expectLater(
        subscription.done.timeout(const Duration(seconds: 2)),
        throwsA(isA<NatsConnectionException>()),
      );
    },
  );
}

Future<NatsConnection> _connectWithoutReconnect(_NatsPeer peer) {
  return NatsConnection.connect(
    NatsOptions(
      servers: [peer.server],
      reconnect: NatsReconnectPolicy(maximumAttempts: 0),
      pingInterval: const Duration(minutes: 10),
    ),
  );
}

final class _NatsPeer {
  _NatsPeer._(this._listener, this._connectUrls);

  final ServerSocket _listener;
  final List<String> _connectUrls;
  StreamSubscription<Socket>? _acceptor;
  StreamSubscription<List<int>>? _reader;
  Socket? _socket;
  String _incoming = "";

  int get port => _listener.port;

  NatsServer get server =>
      NatsServer.nats(InternetAddress.loopbackIPv4.address, port: port);

  static Future<_NatsPeer> start({List<String> connectUrls = const []}) async {
    final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final peer = _NatsPeer._(listener, List.unmodifiable(connectUrls));
    peer._acceptor = listener.listen(peer._accept);
    return peer;
  }

  void _accept(Socket socket) {
    _socket = socket;
    _reader = socket.listen(_onData);
    final info = {
      "server_id": "server-pool-test",
      "headers": true,
      "max_payload": 1024 * 1024,
      "connect_urls": _connectUrls,
    };
    socket.add(ascii.encode("INFO ${jsonEncode(info)}\r\n"));
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
      if (line == "PING") {
        _socket?.add(ascii.encode("PONG\r\n"));
      }
    }
  }

  Future<void> close() async {
    await _reader?.cancel();
    await _socket?.close();
    await _acceptor?.cancel();
    await _listener.close();
  }
}

Future<int> _reserveLoopbackPort() async {
  final socket = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  final port = socket.port;
  await socket.close();
  return port;
}
