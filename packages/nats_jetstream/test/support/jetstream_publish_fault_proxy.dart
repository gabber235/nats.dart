import "dart:async";
import "dart:convert";
import "dart:io";
import "dart:typed_data";

import "package:nats_core/nats_core.dart";

import "nats_wire_frame_parser.dart";

/// One deterministic fault applied to JetStream publication replies.
enum JetStreamPublishFault {
  /// Drops the first valid acknowledgement and resets the TCP connection.
  resetAfterFirstAck,

  /// Drops every valid acknowledgement while leaving TCP connected.
  dropAllAcks,

  /// Replaces the first valid acknowledgement with invalid JSON.
  malformedFirstAck,

  /// Replaces the first valid acknowledgement with error 10158.
  duplicateInProcessFirstAck,

  /// Replaces every valid acknowledgement with error 10158.
  duplicateInProcessEveryAck,

  /// Forwards no responders, then holds the retry until explicitly released.
  holdRetryAfterNoResponders,
}

/// Frame preserving TCP proxy for one JetStream publication subject.
final class JetStreamPublishFaultProxy {
  JetStreamPublishFaultProxy._({
    required this.upstream,
    required this.publishSubject,
    required this.expectedStream,
    required this.fault,
    required ServerSocket listener,
  }) : _listener = listener {
    _firstAckObserved.future.ignore();
    _firstNoRespondersForwarded.future.ignore();
    _retryHeld.future.ignore();
  }

  /// Starts a loopback proxy connected to [upstream].
  static Future<JetStreamPublishFaultProxy> start({
    required Uri upstream,
    required String publishSubject,
    required String expectedStream,
    required JetStreamPublishFault fault,
  }) async {
    if (upstream.host.isEmpty || !upstream.hasPort) {
      throw ArgumentError.value(upstream, "upstream", "Host and port required");
    }
    final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final proxy = JetStreamPublishFaultProxy._(
      upstream: upstream,
      publishSubject: publishSubject,
      expectedStream: expectedStream,
      fault: fault,
      listener: listener,
    );
    proxy._acceptSubscription = listener.listen((socket) {
      unawaited(proxy._accept(socket));
    });
    return proxy;
  }

  /// Real NATS Server endpoint.
  final Uri upstream;

  /// Publication subject whose replies are faulted.
  final String publishSubject;

  /// Stream required in each valid storage acknowledgement.
  final String expectedStream;

  /// Configured finite fault behavior.
  final JetStreamPublishFault fault;

  final ServerSocket _listener;
  late final StreamSubscription<Socket> _acceptSubscription;
  final Set<_ProxySession> _sessions = {};
  final Set<String> _acknowledgementSubjects = {};
  final List<String?> _targetMessageIds = [];
  final Completer<void> _firstAckObserved = Completer<void>();
  final Completer<void> _firstNoRespondersForwarded = Completer<void>();
  final Completer<void> _retryHeld = Completer<void>();

  _HeldPublish? _heldPublish;
  Object? _failure;
  var _closing = false;
  var _faultTriggered = false;
  var _noRespondersForwarded = false;
  var _downstreamConnections = 0;
  var _targetPublishes = 0;
  var _acknowledgementsObserved = 0;
  var _acknowledgementsDropped = 0;
  var _acknowledgementsReplaced = 0;
  var _connectionResets = 0;
  var _noResponders = 0;
  var _heldRetries = 0;
  var _releasedRetries = 0;

  /// Loopback endpoint for the client under test.
  Uri get clientUri => Uri(
    scheme: "nats",
    host: InternetAddress.loopbackIPv4.address,
    port: _listener.port,
  );

  /// Completes after the proxy receives the first valid storage ack.
  Future<void> get firstAckObserved => _firstAckObserved.future;

  /// Completes after the first no responders reply reaches the client.
  Future<void> get firstNoRespondersForwarded =>
      _firstNoRespondersForwarded.future;

  /// Completes after the retry publication is retained by the proxy.
  Future<void> get retryHeld => _retryHeld.future;

  /// First parser or proxy invariant failure.
  Object? get failure => _failure;

  /// Accepted client TCP connection count.
  int get downstreamConnections => _downstreamConnections;

  /// Target publication frames observed from the client.
  int get targetPublishes => _targetPublishes;

  /// Message identifiers observed on target publication frames.
  List<String?> get targetMessageIds => List.unmodifiable(_targetMessageIds);

  /// Valid storage acknowledgement frames received upstream.
  int get acknowledgementsObserved => _acknowledgementsObserved;

  /// Valid storage acknowledgements not forwarded downstream.
  int get acknowledgementsDropped => _acknowledgementsDropped;

  /// Valid storage acknowledgements replaced downstream.
  int get acknowledgementsReplaced => _acknowledgementsReplaced;

  /// Abortive downstream TCP closes performed by the proxy.
  int get connectionResets => _connectionResets;

  /// Core no responders replies observed upstream.
  int get noResponders => _noResponders;

  /// Retry publication frames retained before upstream delivery.
  int get heldRetries => _heldRetries;

  /// Retained retry publication frames released upstream.
  int get releasedRetries => _releasedRetries;

  /// Forwards the publication retained by holdRetryAfterNoResponders.
  Future<void> releaseHeldRetry() async {
    final held = _heldPublish;
    if (held == null) {
      throw StateError("No JetStream retry is held");
    }
    if (held.session.isClosed) {
      throw StateError("Held JetStream retry session is closed");
    }
    _heldPublish = null;
    _releasedRetries += 1;
    held.session.upstream.add(held.frame.bytes);
    await held.session.upstream.flush();
  }

  Future<void> _accept(Socket downstream) async {
    _downstreamConnections += 1;
    Socket? upstreamSocket;
    try {
      upstreamSocket = await Socket.connect(upstream.host, upstream.port);
      if (_closing) {
        downstream.destroy();
        upstreamSocket.destroy();
        return;
      }
      final session = _ProxySession(this, downstream, upstreamSocket);
      _sessions.add(session);
      session.start();
    } on Object catch (error) {
      downstream.destroy();
      upstreamSocket?.destroy();
      _recordFailure(error);
    }
  }

  void _clientFrame(_ProxySession session, NatsWireFrame frame) {
    if (frame.subject != publishSubject ||
        (frame.command != "PUB" && frame.command != "HPUB")) {
      session.upstream.add(frame.bytes);
      return;
    }

    final replySubject = frame.replySubject;
    if (replySubject == null) {
      _recordFailure(StateError("JetStream publication has no reply subject"));
      unawaited(session.close());
      return;
    }
    _targetPublishes += 1;
    _targetMessageIds.add(_messageId(frame));
    _acknowledgementSubjects.add(replySubject);

    if (fault == JetStreamPublishFault.holdRetryAfterNoResponders &&
        _noRespondersForwarded &&
        _heldPublish == null) {
      _heldPublish = _HeldPublish(session, frame);
      _heldRetries += 1;
      if (!_retryHeld.isCompleted) _retryHeld.complete();
      return;
    }
    session.upstream.add(frame.bytes);
  }

  void _serverFrame(_ProxySession session, NatsWireFrame frame) {
    final subject = frame.subject;
    if (subject == null || !_acknowledgementSubjects.remove(subject)) {
      session.downstream.add(frame.bytes);
      return;
    }

    if (_isNoResponders(frame)) {
      _noResponders += 1;
      session.downstream.add(frame.bytes);
      _noRespondersForwarded = true;
      if (!_firstNoRespondersForwarded.isCompleted) {
        _firstNoRespondersForwarded.complete();
      }
      return;
    }

    if (!_isValidStorageAck(frame.body)) {
      _recordFailure(StateError("Upstream returned an invalid publish ack"));
      session.downstream.add(frame.bytes);
      return;
    }
    _acknowledgementsObserved += 1;
    if (!_firstAckObserved.isCompleted) _firstAckObserved.complete();

    switch (fault) {
      case JetStreamPublishFault.resetAfterFirstAck:
        if (!_faultTriggered) {
          _faultTriggered = true;
          _acknowledgementsDropped += 1;
          _connectionResets += 1;
          unawaited(session.close(resetDownstream: true));
          return;
        }
      case JetStreamPublishFault.dropAllAcks:
        _acknowledgementsDropped += 1;
        return;
      case JetStreamPublishFault.malformedFirstAck:
        if (!_faultTriggered) {
          _faultTriggered = true;
          _acknowledgementsReplaced += 1;
          session.downstream.add(_malformedAck(frame));
          return;
        }
        break;
      case JetStreamPublishFault.duplicateInProcessFirstAck:
        if (!_faultTriggered) {
          _faultTriggered = true;
          _acknowledgementsReplaced += 1;
          session.downstream.add(_duplicateInProcessAck(frame));
          return;
        }
        break;
      case JetStreamPublishFault.duplicateInProcessEveryAck:
        _acknowledgementsReplaced += 1;
        session.downstream.add(_duplicateInProcessAck(frame));
        return;
      case JetStreamPublishFault.holdRetryAfterNoResponders:
        break;
    }
    session.downstream.add(frame.bytes);
  }

  bool _isNoResponders(NatsWireFrame frame) {
    final headerLength = frame.headerLength;
    if (frame.command != "HMSG" ||
        headerLength == null ||
        frame.body.isNotEmpty) {
      return false;
    }
    try {
      return NatsHeaders.parse(frame.headerBytes).statusCode == 503;
    } on NatsHeaderException {
      return false;
    }
  }

  bool _isValidStorageAck(Uint8List payload) {
    try {
      final decoded = jsonDecode(utf8.decode(payload));
      return decoded is Map<String, Object?> &&
          decoded["stream"] == expectedStream &&
          decoded["seq"] is int &&
          (decoded["seq"]! as int) > 0;
    } on FormatException {
      return false;
    }
  }

  String? _messageId(NatsWireFrame frame) {
    if (frame.command != "HPUB") return null;
    try {
      return NatsHeaders.parse(frame.headerBytes).first("Nats-Msg-Id");
    } on NatsHeaderException catch (error) {
      _recordFailure(error);
      return null;
    }
  }

  Uint8List _malformedAck(NatsWireFrame original) {
    return _message(original, const [123]);
  }

  Uint8List _duplicateInProcessAck(NatsWireFrame original) {
    return _message(
      original,
      ascii.encode(
        '{"error":{"code":409,"err_code":10158,'
        '"description":"duplicate message id is in process"}}',
      ),
    );
  }

  Uint8List _message(NatsWireFrame original, List<int> payload) {
    final reply = original.replySubject == null
        ? ""
        : " ${original.replySubject}";
    return Uint8List.fromList([
      ...ascii.encode(
        "MSG ${original.subject} ${original.subscriptionId}$reply "
        "${payload.length}\r\n",
      ),
      ...payload,
      13,
      10,
    ]);
  }

  void _recordFailure(Object error) {
    _failure ??= error;
  }

  /// Closes the listener, every session, and any retained frame.
  Future<void> close() async {
    if (_closing) return;
    _closing = true;
    _heldPublish = null;
    await _acceptSubscription.cancel();
    await _listener.close();
    await Future.wait(
      _sessions.toList(growable: false).map((session) => session.close()),
    );
  }
}

final class _HeldPublish {
  const _HeldPublish(this.session, this.frame);

  final _ProxySession session;
  final NatsWireFrame frame;
}

final class _ProxySession {
  _ProxySession(this.owner, this.downstream, this.upstream);

  static const _linuxSoLinger = 13;

  final JetStreamPublishFaultProxy owner;
  final Socket downstream;
  final Socket upstream;
  final NatsWireFrameParser _clientParser = NatsWireFrameParser(
    NatsWireDirection.client,
  );
  final NatsWireFrameParser _serverParser = NatsWireFrameParser(
    NatsWireDirection.server,
  );

  late final StreamSubscription<Uint8List> _clientSubscription;
  late final StreamSubscription<Uint8List> _serverSubscription;
  var _closed = false;

  bool get isClosed => _closed;

  void start() {
    _clientSubscription = downstream.listen(
      _clientBytes,
      onError: _socketError,
      onDone: _socketDone,
      cancelOnError: false,
    );
    _serverSubscription = upstream.listen(
      _serverBytes,
      onError: _socketError,
      onDone: _socketDone,
      cancelOnError: false,
    );
  }

  void _clientBytes(Uint8List bytes) {
    try {
      for (final frame in _clientParser.add(bytes)) {
        if (_closed) return;
        owner._clientFrame(this, frame);
      }
    } on Object catch (error) {
      owner._recordFailure(error);
      unawaited(close());
    }
  }

  void _serverBytes(Uint8List bytes) {
    try {
      for (final frame in _serverParser.add(bytes)) {
        if (_closed) return;
        owner._serverFrame(this, frame);
      }
    } on Object catch (error) {
      owner._recordFailure(error);
      unawaited(close());
    }
  }

  void _socketError(Object _, StackTrace _) {
    unawaited(close());
  }

  void _socketDone() {
    unawaited(close());
  }

  Future<void> close({bool resetDownstream = false}) async {
    if (_closed) return;
    _closed = true;
    if (resetDownstream) _enableAbortiveClose(downstream);
    downstream.destroy();
    upstream.destroy();
    await Future.wait([
      _clientSubscription.cancel(),
      _serverSubscription.cancel(),
    ]);
    owner._sessions.remove(this);
  }

  void _enableAbortiveClose(Socket socket) {
    if (!Platform.isLinux) {
      owner._recordFailure(
        UnsupportedError("Abortive TCP close requires Linux"),
      );
      return;
    }
    final value = Uint8List(8);
    ByteData.sublistView(value)
      ..setInt32(0, 1, Endian.host)
      ..setInt32(4, 0, Endian.host);
    socket.setRawOption(
      RawSocketOption(RawSocketOption.levelSocket, _linuxSoLinger, value),
    );
  }
}
