import "dart:async";
import "dart:js_interop";
import "dart:typed_data";

import "package:nats_core/src/error.dart";
import "package:nats_core/src/options.dart";
import "package:nats_core/src/transport/transport.dart";
import "package:web/web.dart" as browser;

/// Rejects browser incompatible options before constructing a WebSocket.
void validateTransportOptions(NatsOptions options) {
  for (final server in options.servers) {
    if (server.scheme != "ws" && server.scheme != "wss") {
      throw const NatsUnsupportedRuntimeException(
        operation: "native NATS transport",
        option: "servers",
      );
    }
  }
  if (options.webSocket.headers.isNotEmpty) {
    throw const NatsUnsupportedRuntimeException(
      operation: "custom WebSocket upgrade headers",
      option: "webSocket.headers",
    );
  }
  final tls = options.tls;
  if (tls.trustedCertificates != null) {
    throw const NatsUnsupportedRuntimeException(
      operation: "custom TLS trust",
      option: "tls.trustedCertificates",
    );
  }
  if (tls.clientCertificateChain != null) {
    throw const NatsUnsupportedRuntimeException(
      operation: "mutual TLS",
      option: "tls.clientCertificateChain",
    );
  }
  if (tls.allowInvalidCertificates) {
    throw const NatsUnsupportedRuntimeException(
      operation: "custom certificate policy",
      option: "tls.allowInvalidCertificates",
    );
  }
}

/// Connects the browser WebSocket transport.
Future<NatsTransport> connectTransport(
  NatsServer server,
  NatsTlsOptions tls,
  NatsWebSocketOptions webSocket,
  Duration timeout,
) async {
  if (server.scheme != "ws" && server.scheme != "wss") {
    throw const NatsUnsupportedRuntimeException(
      operation: "native NATS transport",
      option: "servers",
    );
  }
  if (webSocket.headers.isNotEmpty) {
    throw const NatsUnsupportedRuntimeException(
      operation: "custom WebSocket upgrade headers",
      option: "webSocket.headers",
    );
  }
  if (tls.trustedCertificates != null ||
      tls.clientCertificateChain != null ||
      tls.allowInvalidCertificates) {
    throw const NatsUnsupportedRuntimeException(
      operation: "custom TLS policy",
      option: "tls",
    );
  }

  final transport = _BrowserWebSocketTransport(server.uri);
  try {
    await transport.ready.timeout(
      timeout,
      onTimeout: () async {
        await transport.close();
        throw TimeoutException("WebSocket connection timed out", timeout);
      },
    );
    return transport;
  } on Object {
    await transport.close();
    rethrow;
  }
}

final class _BrowserWebSocketTransport implements NatsTransport {
  _BrowserWebSocketTransport(Uri uri)
    : _socket = browser.WebSocket(uri.toString()) {
    _socket.binaryType = "arraybuffer";
    _messageSubscription = _socket.onMessage.listen(_onMessage);
    unawaited(_socket.onOpen.first.then((_) => _completeReady()));
    unawaited(
      _socket.onError.first.then((_) {
        final error = StateError("Browser WebSocket failed");
        if (!_ready.isCompleted) {
          _ready.completeError(error);
        } else {
          _finish(error);
        }
      }),
    );
    unawaited(
      _socket.onClose.first.then((event) {
        final close = event;
        if (!_ready.isCompleted) {
          _ready.completeError(
            StateError(
              "Browser WebSocket closed during connection with code "
              "${close.code}: ${close.reason}",
            ),
          );
        }
        final abnormal = close.code != 1000 && close.code != 1005;
        _finish(
          abnormal && !_closed
              ? StateError(
                  "Browser WebSocket closed with code ${close.code}: "
                  "${close.reason}",
                )
              : null,
        );
      }),
    );
  }

  final browser.WebSocket _socket;
  final Completer<void> _ready = Completer<void>();
  final StreamController<Uint8List> _incoming = StreamController();
  late final StreamSubscription<browser.MessageEvent> _messageSubscription;
  var _closed = false;
  var _incomingListened = false;

  Future<void> get ready => _ready.future;

  @override
  Stream<Uint8List> get incoming {
    _incomingListened = true;
    return _incoming.stream;
  }

  void _completeReady() {
    if (!_ready.isCompleted) {
      _ready.complete();
    }
  }

  void _onMessage(browser.MessageEvent event) {
    if (_incoming.isClosed) {
      return;
    }
    final data = event.data;
    if (data.typeofEquals("string")) {
      _incoming.addError(
        const FormatException("NATS WebSocket frame must be binary"),
      );
      return;
    }
    if (data.typeofEquals("object") &&
        (data as JSObject).instanceOfString("ArrayBuffer")) {
      _incoming.add((data as JSArrayBuffer).toDart.asUint8List());
      return;
    }
    _incoming.addError(StateError("Unexpected browser WebSocket message type"));
  }

  void _finish([Object? error]) {
    if (_incoming.isClosed) {
      return;
    }
    if (error != null) {
      _incoming.addError(error);
    }
    unawaited(_incoming.close());
  }

  @override
  void add(Uint8List bytes) {
    if (_closed || _socket.readyState != browser.WebSocket.OPEN) {
      throw StateError("Browser WebSocket is not open");
    }
    _socket.send(bytes.toJS);
  }

  @override
  Future<void> flush() => Future.value();

  @override
  Future<void> startTls({
    required String host,
    required NatsTlsOptions options,
    required Duration timeout,
  }) => throw const NatsUnsupportedRuntimeException(
    operation: "WebSocket TLS upgrade",
  );

  @override
  Future<void> close() async {
    if (_closed) {
      return;
    }
    _closed = true;
    try {
      await _messageSubscription.cancel();
      _socket.close();
    } finally {
      if (!_incoming.isClosed) {
        final closed = _incoming.close();
        // A single subscription close waits until its first listener exists.
        // Failed upgrades never transfer incoming stream ownership.
        if (_incomingListened) {
          await closed;
        }
      }
    }
  }
}
