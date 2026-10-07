@TestOn("vm")
library;

import "dart:async";
import "dart:convert";
import "dart:io";
import "dart:math";

import "package:nats_core/nats_core.dart";
import "package:test/test.dart";

void main() {
  test("drain is shared before synchronous lifecycle callbacks", () async {
    final peer = await _ScriptedNatsPeer.start([
      const _PeerAttempt(replyToPings: true),
    ]);
    addTearDown(peer.close);
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [peer.server],
        reconnect: NatsReconnectPolicy(maximumAttempts: 0),
      ),
    );
    Future<void>? nestedDrain;
    Future<void>? nestedClose;
    var drainingEvents = 0;
    final listener = connection.events.listen((event) {
      if (event is NatsDraining) {
        drainingEvents += 1;
        nestedDrain = connection.drain();
        nestedClose = connection.close();
      }
    });
    addTearDown(listener.cancel);

    final drain = connection.drain();

    expect(nestedDrain, same(drain));
    await Future.wait([
      drain,
      nestedClose!,
    ]).timeout(const Duration(seconds: 1));
    await connection.closed.timeout(const Duration(seconds: 1));
    expect(drainingEvents, 1);
  });

  test("close from reconnecting prevents delayed reconnect work", () async {
    final peer = await _ScriptedNatsPeer.start([
      const _PeerAttempt(replyToPings: true),
      const _PeerAttempt(replyToPings: true),
    ]);
    addTearDown(peer.close);
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [peer.server],
        reconnect: NatsReconnectPolicy(
          maximumAttempts: -1,
          initialDelay: const Duration(seconds: 30),
          baseDelay: const Duration(seconds: 30),
          maximumDelay: const Duration(seconds: 30),
          jitter: 0,
        ),
      ),
    );
    Future<void>? close;
    final reconnecting = Completer<void>();
    final listener = connection.events.listen((event) {
      if (event is NatsReconnecting) {
        close = connection.close();
        reconnecting.complete();
      }
    });
    addTearDown(listener.cancel);

    await peer.closeConnection(0);
    await reconnecting.future.timeout(const Duration(seconds: 1));
    await close!.timeout(const Duration(seconds: 1));
    await connection.closed.timeout(const Duration(seconds: 1));
    await Future<void>.delayed(Duration.zero);
    expect(peer.acceptedConnections, 1);
  });

  test("close from connecting prevents a new transport dial", () async {
    final peer = await _ScriptedNatsPeer.start([
      const _PeerAttempt(replyToPings: true),
      const _PeerAttempt(replyToPings: true),
    ]);
    addTearDown(peer.close);
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [peer.server],
        reconnect: NatsReconnectPolicy(
          maximumAttempts: -1,
          initialDelay: const Duration(milliseconds: 1),
          baseDelay: const Duration(milliseconds: 1),
          maximumDelay: const Duration(milliseconds: 1),
          jitter: 0,
        ),
      ),
    );
    Future<void>? close;
    final connecting = Completer<void>();
    final listener = connection.events.listen((event) {
      if (event is NatsConnecting && event.attempt == 1) {
        close = connection.close();
        connecting.complete();
      }
    });
    addTearDown(listener.cancel);

    await peer.closeConnection(0);
    await connecting.future.timeout(const Duration(seconds: 1));
    await close!.timeout(const Duration(seconds: 1));
    await connection.closed.timeout(const Duration(seconds: 1));
    await Future<void>.delayed(Duration.zero);
    expect(peer.acceptedConnections, 1);
  });

  test("close does not await an already pending reconnect transport", () async {
    final peer = await _ScriptedNatsPeer.start([
      const _PeerAttempt(replyToPings: true),
    ]);
    final webServer = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    final requestReceived = Completer<HttpRequest>();
    final allowUpgrade = Completer<void>();
    final serverSocket = Completer<WebSocket>();
    final serverSocketClosed = Completer<void>();
    final requestHandled = Completer<void>();
    requestHandled.future.ignore();
    final webServerSubscription = webServer.listen((request) async {
      if (!requestReceived.isCompleted) {
        requestReceived.complete(request);
      }
      try {
        await allowUpgrade.future;
        final socket = await WebSocketTransformer.upgrade(request);
        serverSocket.complete(socket);
        await socket.drain<void>();
        serverSocketClosed.complete();
        requestHandled.complete();
      } on Object catch (error, stackTrace) {
        if (!requestHandled.isCompleted) {
          requestHandled.completeError(error, stackTrace);
        }
      }
    });
    addTearDown(() async {
      if (!allowUpgrade.isCompleted) {
        allowUpgrade.complete();
      }
      await peer.close();
      await webServerSubscription.cancel();
      if (serverSocket.isCompleted) {
        await (await serverSocket.future).close();
      }
      await webServer.close(force: true);
    });

    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [peer.server],
        connectTimeout: const Duration(seconds: 30),
        reconnect: NatsReconnectPolicy(
          maximumAttempts: -1,
          baseDelay: const Duration(milliseconds: 1),
          maximumDelay: const Duration(milliseconds: 1),
          jitter: 0,
        ),
      ),
    );
    connection.setServers([
      NatsServer.websocket(
        InternetAddress.loopbackIPv4.address,
        port: webServer.port,
      ),
    ]);
    var terminalEvents = 0;
    final eventsDone = Completer<void>();
    final eventListener = connection.events.listen((event) {
      if (event is NatsClosed) {
        terminalEvents += 1;
      }
    }, onDone: eventsDone.complete);
    addTearDown(eventListener.cancel);

    await peer.closeConnection(0);
    await requestReceived.future.timeout(const Duration(seconds: 2));

    await connection.close().timeout(const Duration(seconds: 1));
    await connection.closed.timeout(const Duration(seconds: 1));
    await eventsDone.future.timeout(const Duration(seconds: 1));
    expect(terminalEvents, 1);

    allowUpgrade.complete();
    await serverSocket.future.timeout(const Duration(seconds: 1));
    await serverSocketClosed.future.timeout(const Duration(seconds: 1));
    await requestHandled.future.timeout(const Duration(seconds: 1));
    await Future<void>.delayed(Duration.zero);
    expect(terminalEvents, 1);
  });

  test("handshake timeout releases its PING before server failover", () async {
    final stalled = await _ScriptedNatsPeer.start([
      const _PeerAttempt(replyToPings: false, delayInfo: true),
    ]);
    final healthy = await _ScriptedNatsPeer.start([
      const _PeerAttempt(replyToPings: true),
    ]);
    addTearDown(() async {
      await stalled.close();
      await healthy.close();
    });

    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [stalled.server, healthy.server],
        connectTimeout: const Duration(milliseconds: 250),
        reconnect: NatsReconnectPolicy(maximumAttempts: 0),
      ),
    ).timeout(const Duration(seconds: 2));
    addTearDown(connection.close);

    expect(connection.serverInfo.serverAuthority, healthy.server.uri.authority);
    expect(connection.statistics.outstandingPings, 0);
    expect(stalled.acceptedConnections, 1);
    expect(healthy.acceptedConnections, 1);
  });

  test("failed reconnect handshake cannot poison the next attempt", () async {
    final peer = await _ScriptedNatsPeer.start([
      const _PeerAttempt(replyToPings: true),
      const _PeerAttempt(replyToPings: false, delayInfo: true),
      const _PeerAttempt(replyToPings: true),
      const _PeerAttempt(replyToPings: true),
    ]);
    addTearDown(peer.close);
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [peer.server],
        connectTimeout: const Duration(milliseconds: 250),
        reconnect: NatsReconnectPolicy(
          maximumAttempts: 3,
          baseDelay: const Duration(milliseconds: 1),
          maximumDelay: const Duration(milliseconds: 1),
          jitter: 0,
        ),
      ),
    );
    addTearDown(connection.close);
    final reconnectSuccessesAtConnected = connection.events
        .where((event) => event is NatsConnected)
        .cast<NatsConnected>()
        .where((event) => event.generation == 2)
        .map((_) => connection.statistics.reconnectSuccesses)
        .first;

    await peer.closeConnection(0);
    expect(
      await reconnectSuccessesAtConnected.timeout(const Duration(seconds: 2)),
      1,
    );

    expect(peer.acceptedConnections, 3);
    expect(connection.statistics.reconnectAttempts, 2);
    expect(connection.statistics.reconnectSuccesses, 1);
    expect(connection.statistics.outstandingPings, 0);
  });

  test("authentication timeout signals cancellation and settles", () async {
    final peer = await _ScriptedNatsPeer.start([
      const _PeerAttempt(replyToPings: true),
    ]);
    addTearDown(peer.close);
    var invocations = 0;
    final providerEntered = Completer<void>();
    final cancelled = Completer<void>();

    final connectionFailure = expectLater(
      NatsConnection.connect(
        NatsOptions(
          servers: [peer.server],
          authentication: NatsAuthentication.provider((context) async {
            invocations += 1;
            if (!providerEntered.isCompleted) {
              providerEntered.complete();
            }
            expect(context.deadline.isAfter(DateTime.timestamp()), isTrue);
            await context.cancelled;
            if (!cancelled.isCompleted) {
              cancelled.complete();
            }
            return NatsConnectCredentials(username: "late", password: "late");
          }),
          reconnect: NatsReconnectPolicy(maximumAttempts: 0),
        ),
      ).timeout(const Duration(seconds: 10)),
      throwsA(isA<NatsTimeoutException>()),
    );
    await providerEntered.future.timeout(const Duration(seconds: 5));
    await connectionFailure;
    await cancelled.future.timeout(const Duration(seconds: 5));
    expect(invocations, 1);
  });

  test("stale authentication provider never overlaps another call", () async {
    final peer = await _ScriptedNatsPeer.start(
      List.filled(8, const _PeerAttempt(replyToPings: true)),
    );
    addTearDown(peer.close);
    final never = Completer<NatsConnectCredentials>();
    var invocations = 0;
    var activeProviders = 0;
    var maximumActiveProviders = 0;
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [peer.server],
        connectTimeout: const Duration(seconds: 1),
        authentication: NatsAuthentication.provider((_) async {
          invocations += 1;
          activeProviders += 1;
          maximumActiveProviders = max(maximumActiveProviders, activeProviders);
          if (invocations == 1) {
            activeProviders -= 1;
            return NatsConnectCredentials(username: "first", password: "first");
          }
          return never.future.whenComplete(() {
            activeProviders -= 1;
          });
        }),
        reconnect: NatsReconnectPolicy(
          maximumAttempts: -1,
          baseDelay: const Duration(milliseconds: 1),
          maximumDelay: const Duration(milliseconds: 1),
          jitter: 0,
        ),
      ),
    );
    addTearDown(connection.close);
    final fourthAttempt = connection.events
        .where((event) => event is NatsReconnecting)
        .cast<NatsReconnecting>()
        .firstWhere((event) => event.attempt == 4);

    await peer.closeConnection(0);
    await fourthAttempt.timeout(const Duration(seconds: 10));

    expect(invocations, 2);
    expect(activeProviders, 1);
    expect(maximumActiveProviders, 1);
    await connection.close().timeout(const Duration(seconds: 1));
    await connection.closed.timeout(const Duration(seconds: 1));
  });

  test(
    "silent authenticated closures can recover on a later reconnect",
    () async {
      final peer = await _ScriptedNatsPeer.start([
        const _PeerAttempt(replyToPings: true),
        const _PeerAttempt(replyToPings: false, closeAfterConnect: true),
        const _PeerAttempt(replyToPings: false, closeAfterConnect: true),
        const _PeerAttempt(replyToPings: true),
      ]);
      addTearDown(peer.close);
      final connection = await NatsConnection.connect(
        NatsOptions(
          servers: [peer.server],
          authentication: NatsAuthentication.userPassword("alice", "secret"),
          reconnect: NatsReconnectPolicy(
            maximumAttempts: -1,
            baseDelay: const Duration(milliseconds: 1),
            maximumDelay: const Duration(milliseconds: 1),
            jitter: 0,
          ),
        ),
      );
      addTearDown(connection.close);
      final recovered = connection.events
          .where((event) => event is NatsConnected)
          .first;

      await peer.closeConnection(0);
      await recovered.timeout(const Duration(seconds: 5));
      await connection.flush();

      expect(peer.acceptedConnections, 4);
      expect(connection.statistics.reconnectAttempts, 3);
      expect(connection.statistics.reconnectSuccesses, 1);
    },
  );

  test(
    "silent authenticated closures respect the connection retry budget",
    () async {
      final peer = await _ScriptedNatsPeer.start([
        const _PeerAttempt(replyToPings: true),
        const _PeerAttempt(replyToPings: false, closeAfterConnect: true),
        const _PeerAttempt(replyToPings: false, closeAfterConnect: true),
      ]);
      addTearDown(peer.close);
      final connection = await NatsConnection.connect(
        NatsOptions(
          servers: [peer.server],
          authentication: NatsAuthentication.userPassword("alice", "secret"),
          reconnect: NatsReconnectPolicy(
            maximumAttempts: 2,
            baseDelay: const Duration(milliseconds: 1),
            maximumDelay: const Duration(milliseconds: 1),
            jitter: 0,
          ),
        ),
      );
      addTearDown(connection.close);
      final closed = connection.events
          .where((event) => event is NatsClosed)
          .cast<NatsClosed>()
          .first;

      await peer.closeConnection(0);
      final terminal = await closed.timeout(const Duration(seconds: 5));
      await connection.closed.timeout(const Duration(seconds: 5));

      expect(terminal.error, isA<NatsConnectionException>());
      expect(peer.acceptedConnections, 3);
      expect(connection.statistics.reconnectAttempts, 2);
    },
  );

  test(
    "caller close cancels pending authentication without a rejection",
    () async {
      final peer = await _ScriptedNatsPeer.start([
        const _PeerAttempt(replyToPings: true),
        const _PeerAttempt(replyToPings: true),
      ]);
      addTearDown(peer.close);
      final providerEntered = Completer<void>();
      final cancelled = Completer<void>();
      var invocations = 0;
      final connection = await NatsConnection.connect(
        NatsOptions(
          servers: [peer.server],
          authentication: NatsAuthentication.provider((context) async {
            invocations += 1;
            if (invocations > 1) {
              providerEntered.complete();
              await context.cancelled;
              cancelled.complete();
            }
            return NatsConnectCredentials(
              username: "alice",
              password: "secret",
            );
          }),
          reconnect: NatsReconnectPolicy(
            maximumAttempts: -1,
            baseDelay: const Duration(milliseconds: 1),
            maximumDelay: const Duration(milliseconds: 1),
            jitter: 0,
          ),
        ),
      );
      addTearDown(connection.close);
      final closed = connection.events
          .where((event) => event is NatsClosed)
          .cast<NatsClosed>()
          .first;

      await peer.closeConnection(0);
      await providerEntered.future.timeout(const Duration(seconds: 5));
      await connection.close().timeout(const Duration(seconds: 1));
      await cancelled.future.timeout(const Duration(seconds: 1));
      final terminal = await closed.timeout(const Duration(seconds: 1));
      await connection.closed.timeout(const Duration(seconds: 1));

      expect(terminal.error, isNull);
      expect(invocations, 2);
      expect(peer.acceptedConnections, 2);
      expect(connection.statistics.reconnectAttempts, 1);
    },
  );

  test(
    "repeated explicit authorization rejections terminate unlimited reconnect",
    () async {
      final peer = await _ScriptedNatsPeer.start([
        const _PeerAttempt(replyToPings: true),
        const _PeerAttempt(
          replyToPings: false,
          errorAfterConnect: "Authorization Violation",
        ),
        const _PeerAttempt(
          replyToPings: false,
          errorAfterConnect: "Authorization Violation",
        ),
      ]);
      addTearDown(peer.close);
      final connection = await NatsConnection.connect(
        NatsOptions(
          servers: [peer.server],
          authentication: NatsAuthentication.userPassword("alice", "secret"),
          reconnect: NatsReconnectPolicy(
            maximumAttempts: -1,
            baseDelay: const Duration(milliseconds: 1),
            maximumDelay: const Duration(milliseconds: 1),
            jitter: 0,
          ),
        ),
      );
      addTearDown(connection.close);
      final closed = connection.events
          .where((event) => event is NatsClosed)
          .cast<NatsClosed>()
          .map((event) => event.error)
          .first;

      await peer.closeConnection(0);

      await expectLater(
        closed.timeout(const Duration(seconds: 5)),
        completion(
          isA<NatsAuthenticationException>().having(
            (error) => error.reason,
            "reason",
            NatsAuthenticationFailure.authorizationViolation,
          ),
        ),
      );
      await connection.closed.timeout(const Duration(seconds: 5));
      expect(peer.acceptedConnections, 3);
      expect(connection.statistics.reconnectAttempts, 2);
    },
  );

  test(
    "repeated authentication timeouts terminate unlimited reconnect",
    () async {
      final peer = await _ScriptedNatsPeer.start([
        const _PeerAttempt(replyToPings: true),
        const _PeerAttempt(
          replyToPings: false,
          errorAfterConnect: "Authentication Timeout",
        ),
        const _PeerAttempt(
          replyToPings: false,
          errorAfterConnect: "Authentication Timeout",
        ),
      ]);
      addTearDown(peer.close);
      final connection = await NatsConnection.connect(
        NatsOptions(
          servers: [peer.server],
          authentication: NatsAuthentication.userPassword("alice", "secret"),
          reconnect: NatsReconnectPolicy(
            maximumAttempts: -1,
            baseDelay: const Duration(milliseconds: 1),
            maximumDelay: const Duration(milliseconds: 1),
            jitter: 0,
          ),
        ),
      );
      addTearDown(connection.close);
      final closed = connection.events
          .where((event) => event is NatsClosed)
          .cast<NatsClosed>()
          .map((event) => event.error)
          .first;

      await peer.closeConnection(0);

      await expectLater(
        closed.timeout(const Duration(seconds: 5)),
        completion(
          isA<NatsAuthenticationException>().having(
            (error) => error.reason,
            "reason",
            NatsAuthenticationFailure.authenticationTimeout,
          ),
        ),
      );
      await connection.closed.timeout(const Duration(seconds: 5));
      expect(peer.acceptedConnections, 3);
      expect(connection.statistics.reconnectAttempts, 2);
    },
  );

  test("authentication timeout retries can recover when enabled", () async {
    final peer = await _ScriptedNatsPeer.start([
      const _PeerAttempt(replyToPings: true),
      const _PeerAttempt(
        replyToPings: false,
        errorAfterConnect: "Authentication Timeout",
      ),
      const _PeerAttempt(
        replyToPings: false,
        errorAfterConnect: "Authentication Timeout",
      ),
      const _PeerAttempt(replyToPings: true),
    ]);
    addTearDown(peer.close);
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [peer.server],
        authentication: NatsAuthentication.userPassword("alice", "secret"),
        reconnect: NatsReconnectPolicy(
          maximumAttempts: -1,
          baseDelay: const Duration(milliseconds: 1),
          maximumDelay: const Duration(milliseconds: 1),
          jitter: 0,
          retryAuthenticationFailures: true,
        ),
      ),
    );
    addTearDown(connection.close);
    final reconnected = connection.events
        .where((event) => event is NatsConnected)
        .cast<NatsConnected>()
        .firstWhere((event) => event.generation == 2);

    await peer.closeConnection(0);
    await reconnected.timeout(const Duration(seconds: 5));

    expect(peer.acceptedConnections, 4);
    expect(connection.statistics.reconnectAttempts, 3);
    expect(connection.isConnected, isTrue);
  });

  test(
    "WebSocket handshake transport errors preserve their exact cause",
    () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final socket = Completer<WebSocket>();
      final serverSubscription = server
          .transform(WebSocketTransformer())
          .listen((connection) {
            socket.complete(connection);
            connection.add("INFO must be sent in a binary frame");
          });
      addTearDown(() async {
        await serverSubscription.cancel();
        if (socket.isCompleted) {
          await (await socket.future).close();
        }
        await server.close(force: true);
      });

      Object? failure;
      try {
        await NatsConnection.connect(
          NatsOptions(
            servers: [
              NatsServer.websocket(
                InternetAddress.loopbackIPv4.address,
                port: server.port,
              ),
            ],
            connectTimeout: const Duration(seconds: 30),
            reconnect: NatsReconnectPolicy(maximumAttempts: 0),
          ),
        ).timeout(const Duration(seconds: 2));
      } on Object catch (error) {
        failure = error;
      }

      expect(failure, isA<NatsConnectionException>());
      final connectionFailure = failure! as NatsConnectionException;
      expect(connectionFailure.cause, isA<FormatException>());
      expect(connectionFailure.causeStackTrace, isNotNull);
    },
  );
}

final class _PeerAttempt {
  const _PeerAttempt({
    required this.replyToPings,
    this.delayInfo = false,
    this.closeAfterConnect = false,
    this.errorAfterConnect,
  });

  final bool replyToPings;
  final bool delayInfo;
  final bool closeAfterConnect;
  final String? errorAfterConnect;
}

final class _ScriptedNatsPeer {
  _ScriptedNatsPeer._(this._listener, this._attempts) {
    _listenerSubscription = _listener.listen(
      _accept,
      onError: (Object error, StackTrace stackTrace) {
        if (error is SocketException) {
          return;
        }
        Zone.current.handleUncaughtError(error, stackTrace);
      },
    );
  }

  final ServerSocket _listener;
  final List<_PeerAttempt> _attempts;
  final List<Socket> _sockets = [];
  final List<StreamSubscription<List<int>>> _readers = [];
  final List<Timer> _timers = [];
  final Map<int, Completer<Socket>> _accepted = {};
  late final StreamSubscription<Socket> _listenerSubscription;

  int get acceptedConnections => _sockets.length;

  NatsServer get server => NatsServer.nats(
    InternetAddress.loopbackIPv4.address,
    port: _listener.port,
  );

  static Future<_ScriptedNatsPeer> start(List<_PeerAttempt> attempts) async {
    final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    return _ScriptedNatsPeer._(listener, attempts);
  }

  void _accept(Socket socket) {
    final index = _sockets.length;
    final attempt = _attempts[index];
    _sockets.add(socket);
    (_accepted[index] ??= Completer<Socket>()).complete(socket);
    var incoming = "";
    _readers.add(
      socket.listen((bytes) {
        incoming += ascii.decode(bytes);
        while (true) {
          final lineEnd = incoming.indexOf("\r\n");
          if (lineEnd == -1) {
            return;
          }
          final line = incoming.substring(0, lineEnd);
          incoming = incoming.substring(lineEnd + 2);
          if (line.startsWith("CONNECT ") && attempt.closeAfterConnect) {
            socket.destroy();
            return;
          }
          final error = attempt.errorAfterConnect;
          if (line.startsWith("CONNECT ") && error != null) {
            socket.add(ascii.encode("-ERR '$error'\r\n"));
            unawaited(socket.flush().whenComplete(socket.destroy));
            return;
          }
          if (line == "PING" && attempt.replyToPings) {
            socket.add(ascii.encode("PONG\r\n"));
          }
        }
      }),
    );
    void sendInfo() {
      socket.add(
        ascii.encode(
          "INFO {\"server_id\":\"peer-$index\",\"headers\":true,"
          "\"max_payload\":1048576}\r\n",
        ),
      );
    }

    if (attempt.delayInfo) {
      _timers.add(Timer(const Duration(milliseconds: 75), sendInfo));
    } else {
      sendInfo();
    }
  }

  Future<void> closeConnection(int index) async {
    final accepted = _accepted[index] ??= Completer<Socket>();
    (await accepted.future).destroy();
  }

  Future<void> close() async {
    for (final timer in _timers) {
      timer.cancel();
    }
    await _listenerSubscription.cancel();
    for (final reader in _readers) {
      await reader.cancel();
    }
    for (final socket in _sockets) {
      socket.destroy();
    }
    await _listener.close();
  }
}
