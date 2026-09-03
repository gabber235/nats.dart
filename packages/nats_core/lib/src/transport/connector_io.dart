import "dart:async";
import "dart:io";
import "dart:math";
import "dart:typed_data";

import "package:nats_core/src/error.dart";
import "package:nats_core/src/options.dart";
import "package:nats_core/src/transport/transport.dart";

/// Native runtimes support every validated transport option.
void validateTransportOptions(NatsOptions options) {}

/// Resolves every native address for one configured hostname.
typedef NativeAddressResolver =
    Future<List<InternetAddress>> Function(String host);

/// Orders resolved addresses for one connection attempt.
typedef NativeAddressOrderer =
    List<InternetAddress> Function(List<InternetAddress> addresses);

/// Connects one resolved native address within its candidate budget.
typedef NativeCandidateConnector =
    Future<NatsTransport> Function(
      NatsServer server,
      InternetAddress address,
      NatsTlsOptions tls,
      Duration timeout,
    );

/// Connects a native socket transport.
Future<NatsTransport> connectTransport(
  NatsServer server,
  NatsTlsOptions tls,
  NatsWebSocketOptions webSocket,
  Duration timeout,
) async {
  if (server.scheme == "ws" || server.scheme == "wss") {
    return _connectWebSocket(server, tls, webSocket, timeout);
  }
  return connectNativeTransport(server, tls, timeout);
}

/// Resolves, rotates, and connects native addresses within one deadline.
Future<NatsTransport> connectNativeTransport(
  NatsServer server,
  NatsTlsOptions tls,
  Duration timeout, {
  NativeAddressResolver resolver = InternetAddress.lookup,
  NativeAddressOrderer orderer = _randomizeAddresses,
  NativeCandidateConnector connector = _connectNativeCandidate,
}) async {
  final clock = Stopwatch()..start();
  final literal = InternetAddress.tryParse(server.host);
  late final List<InternetAddress> resolved;
  if (literal != null) {
    resolved = [literal];
  } else {
    try {
      resolved = await resolver(server.host).timeout(timeout);
    } on Object catch (error, stackTrace) {
      throw NatsDnsException(
        server.host,
        cause: error,
        causeStackTrace: stackTrace,
      );
    }
  }
  if (resolved.isEmpty) {
    throw NatsDnsException(
      server.host,
      cause: const SocketException("DNS resolution returned no addresses"),
    );
  }

  final candidates = orderer(List.of(resolved));
  final failures = <NatsConnectCandidateFailure>[];
  for (var index = 0; index < candidates.length; index += 1) {
    final remaining = timeout - clock.elapsed;
    if (remaining <= Duration.zero) {
      failures.add(
        NatsConnectCandidateFailure(
          candidates[index].address,
          TimeoutException("Connect attempt deadline elapsed", timeout),
          StackTrace.current,
        ),
      );
      break;
    }
    final candidatesRemaining = candidates.length - index;
    final candidateTimeout = Duration(
      microseconds: max(1, remaining.inMicroseconds ~/ candidatesRemaining),
    );
    final address = candidates[index];
    try {
      return await connector(server, address, tls, candidateTimeout);
    } on Object catch (error, stackTrace) {
      failures.add(
        NatsConnectCandidateFailure(address.address, error, stackTrace),
      );
    }
  }
  throw NatsConnectCandidatesException(server.host, server.port, failures);
}

List<InternetAddress> _randomizeAddresses(List<InternetAddress> addresses) {
  addresses.shuffle(Random.secure());
  return addresses;
}

Future<NatsTransport> _connectNativeCandidate(
  NatsServer server,
  InternetAddress address,
  NatsTlsOptions tls,
  Duration timeout,
) async {
  final clock = Stopwatch()..start();
  // The returned transport owns and closes this socket.
  // ignore: close_sinks
  final plain = await Socket.connect(address, server.port, timeout: timeout);
  var socket = plain;
  if (server.tlsMode == NatsTlsMode.first) {
    final remaining = timeout - clock.elapsed;
    if (remaining <= Duration.zero) {
      plain.destroy();
      throw TimeoutException("TLS connection timed out", timeout);
    }
    try {
      socket = await SecureSocket.secure(
        plain,
        host: server.host,
        context: _securityContext(tls),
        onBadCertificate: _badCertificateHandler(tls),
      ).timeout(remaining);
    } on Object catch (error, stackTrace) {
      plain.destroy();
      throw NatsTlsException(
        server.host,
        cause: error,
        causeStackTrace: stackTrace,
      );
    }
  }
  socket.setOption(SocketOption.tcpNoDelay, true);
  return _SocketTransport(socket);
}

Future<NatsTransport> _connectWebSocket(
  NatsServer server,
  NatsTlsOptions tls,
  NatsWebSocketOptions options,
  Duration timeout,
) async {
  final client = HttpClient(context: _securityContext(tls))
    ..badCertificateCallback = tls.allowInvalidCertificates
        ? (_, _, _) => true
        : null;
  try {
    // The returned transport owns and closes this socket.
    // ignore: close_sinks
    final socket = await WebSocket.connect(
      server.uri.toString(),
      headers: options.headers,
      customClient: client,
    ).timeout(timeout);
    socket.pingInterval = null;
    return _WebSocketTransport(socket, client);
  } on TimeoutException catch (error, stackTrace) {
    client.close(force: true);
    Error.throwWithStackTrace(
      NatsTimeoutException("WebSocket connection"),
      stackTrace,
    );
  } on HandshakeException catch (error, stackTrace) {
    client.close(force: true);
    throw NatsTlsException(
      server.host,
      cause: error,
      causeStackTrace: stackTrace,
    );
  } on SocketException catch (error, stackTrace) {
    client.close(force: true);
    if (error.address == null) {
      throw NatsDnsException(
        server.host,
        cause: error,
        causeStackTrace: stackTrace,
      );
    }
    throw NatsConnectCandidatesException(server.host, server.port, [
      NatsConnectCandidateFailure(error.address!.address, error, stackTrace),
    ]);
  } on WebSocketException catch (error, stackTrace) {
    client.close(force: true);
    throw NatsWebSocketUpgradeException(
      cause: error,
      causeStackTrace: stackTrace,
    );
  } on Object {
    client.close(force: true);
    rethrow;
  }
}

SecurityContext _securityContext(NatsTlsOptions options) {
  final context = SecurityContext(withTrustedRoots: true);
  if (options.trustedCertificates case final certificates?) {
    context.setTrustedCertificatesBytes(certificates);
  }
  if (options.clientCertificateChain case final certificate?) {
    context.useCertificateChainBytes(certificate);
  }
  if (options.clientPrivateKey case final key?) {
    context.usePrivateKeyBytes(key, password: options.clientPrivateKeyPassword);
  }
  return context;
}

bool Function(X509Certificate)? _badCertificateHandler(
  NatsTlsOptions options,
) => options.allowInvalidCertificates ? (_) => true : null;

final class _SocketTransport implements NatsTransport {
  _SocketTransport(this._socket) {
    _listen();
  }

  Socket _socket;
  final StreamController<Uint8List> _incoming = StreamController();
  StreamSubscription<Uint8List>? _socketSubscription;
  bool _closed = false;
  var _incomingListened = false;

  @override
  Stream<Uint8List> get incoming {
    _incomingListened = true;
    return _incoming.stream;
  }

  void _listen() {
    final listenedSocket = _socket;
    listenedSocket.done.ignore();
    _socketSubscription = listenedSocket.listen(
      _incoming.add,
      onError: (Object error, StackTrace stackTrace) {
        if (identical(listenedSocket, _socket)) {
          _incoming.addError(error, stackTrace);
        }
      },
      onDone: () {
        if (!_closed && identical(listenedSocket, _socket)) {
          unawaited(_incoming.close());
        }
      },
      cancelOnError: false,
    );
  }

  @override
  void add(Uint8List bytes) => _socket.add(bytes);

  @override
  Future<void> flush() => _socket.flush();

  @override
  Future<void> startTls({
    required String host,
    required NatsTlsOptions options,
    required Duration timeout,
  }) async {
    if (_socket is SecureSocket) {
      return;
    }
    _socketSubscription?.pause();
    try {
      // The transport stores and closes the upgraded socket.
      // ignore: close_sinks
      final secure = await SecureSocket.secure(
        _socket,
        host: host,
        context: _securityContext(options),
        onBadCertificate: _badCertificateHandler(options),
      ).timeout(timeout);
      _socket = secure;
      _listen();
    } on Object {
      _socket.destroy();
      rethrow;
    }
  }

  @override
  Future<void> close() async {
    if (_closed) {
      return;
    }
    _closed = true;
    await _socketSubscription?.cancel();
    await _socket.close();
    _socket.destroy();
    if (!_incoming.isClosed) {
      final closed = _incoming.close();
      if (_incomingListened) {
        await closed;
      }
    }
  }
}

final class _WebSocketTransport implements NatsTransport {
  _WebSocketTransport(this._socket, this._client) {
    _subscription = _socket.listen(
      (event) {
        if (event is Uint8List) {
          _incoming.add(event);
        } else if (event is List<int>) {
          _incoming.add(Uint8List.fromList(event));
        } else {
          _incoming.addError(
            const FormatException("NATS WebSocket frame must be binary"),
          );
        }
      },
      onError: _incoming.addError,
      onDone: () {
        final code = _socket.closeCode;
        if (!_closed && code != null && code != 1000 && code != 1005) {
          _incoming.addError(
            StateError(
              "WebSocket closed with code $code: ${_socket.closeReason ?? ""}",
            ),
          );
        }
        unawaited(_incoming.close());
      },
      cancelOnError: false,
    );
  }

  final WebSocket _socket;
  final HttpClient _client;
  final StreamController<Uint8List> _incoming = StreamController();
  late final StreamSubscription<Object?> _subscription;
  var _closed = false;
  var _incomingListened = false;

  @override
  Stream<Uint8List> get incoming {
    _incomingListened = true;
    return _incoming.stream;
  }

  @override
  void add(Uint8List bytes) => _socket.add(bytes);

  @override
  Future<void> flush() => Future.value();

  @override
  Future<void> startTls({
    required String host,
    required NatsTlsOptions options,
    required Duration timeout,
  }) => throw UnsupportedError("WebSocket TLS cannot start after upgrade");

  @override
  Future<void> close() async {
    if (_closed) {
      return;
    }
    _closed = true;
    try {
      await _subscription.cancel();
      await _socket.close();
    } finally {
      _client.close(force: true);
      if (!_incoming.isClosed) {
        final closed = _incoming.close();
        if (_incomingListened) {
          await closed;
        }
      }
    }
  }
}
