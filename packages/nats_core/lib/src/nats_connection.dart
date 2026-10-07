import "dart:async";
import "dart:collection";
import "dart:math";
import "dart:typed_data";

import "package:nats_core/src/authentication.dart";
import "package:nats_core/src/byte_snapshot.dart";
import "package:nats_core/src/connection/bounded_diagnostic_events.dart";
import "package:nats_core/src/connection/connection_statistics_owner.dart";
import "package:nats_core/src/connection/publication_preflight.dart";
import "package:nats_core/src/connection/request_mux.dart";
import "package:nats_core/src/connection/server_error.dart";
import "package:nats_core/src/connection/subscription_state.dart";
import "package:nats_core/src/connection/trace_headers.dart";
import "package:nats_core/src/connection_event.dart";
import "package:nats_core/src/error.dart";
import "package:nats_core/src/headers.dart";
import "package:nats_core/src/immutable_json.dart";
import "package:nats_core/src/message.dart";
import "package:nats_core/src/options.dart";
import "package:nats_core/src/protocol/client_encoder.dart";
import "package:nats_core/src/protocol/server_operation.dart";
import "package:nats_core/src/protocol/server_parser.dart";
import "package:nats_core/src/request_many.dart";
import "package:nats_core/src/server_info.dart";
import "package:nats_core/src/statistics.dart";
import "package:nats_core/src/subject.dart";
import "package:nats_core/src/subscription.dart";
import "package:nats_core/src/trace.dart";
import "package:nats_core/src/transport/connector.dart";
import "package:nats_core/src/transport/transport.dart";

part "package:nats_core/src/nats_connection_extensions.dart";

const _immutableInfoFieldNames = {
  "server_id",
  "server_name",
  "version",
  "host",
  "port",
  "client_id",
  "client_ip",
  "proto",
};

/// One reconnecting Core NATS connection.
final class NatsConnection {
  NatsConnection._(this._options)
    : _encoder = ClientProtocolEncoder(
        _options.maximumOutboundControlLineBytes,
      ),
      _configuredServers = List.of(_options.servers),
      _serverPool = List.of(_options.servers) {
    _requestMux = RequestMux(
      openInbox: _openRequestInbox,
      closeInbox: _closeRequestInbox,
      publish: _publishRequest,
      inboxPrefix: _options.requestInboxPrefix,
    );
  }

  /// Connects to the configured pool and completes a protocol barrier.
  static Future<NatsConnection> connect(NatsOptions options) async {
    validateTransportOptions(options);
    final connection = NatsConnection._(options);
    try {
      await connection._connectInitial();
      return connection;
    } on Object catch (error, stackTrace) {
      final failure = error is NatsException
          ? error
          : NatsConnectionException(
              "Initial connection failed: $error",
              cause: error,
              causeStackTrace: stackTrace,
            );
      await connection._closeWith(failure);
      throw failure;
    }
  }

  final NatsOptions _options;
  final ClientProtocolEncoder _encoder;
  final Completer<void> _closed = Completer();
  final BoundedDiagnosticEvents<NatsConnectionEvent> _events =
      BoundedDiagnosticEvents();
  final Queue<_PingWaiter> _pongWaiters = Queue();
  final Queue<_BufferedPublication> _reconnectBuffer = Queue();
  final Map<int, SubscriptionState> _subscriptions = {};
  final Set<SubscriptionState> _retiredSubscriptions = {};
  final Set<Future<void>> _disposals = {};
  final Map<String, Object?> _serverInfoFields = {};
  final List<NatsServer> _configuredServers;
  final List<NatsServer> _implicitServers = [];
  final List<NatsServer> _serverPool;
  final Random _random = Random.secure();
  final ConnectionStatisticsOwner _statistics = ConnectionStatisticsOwner();

  late final RequestMux _requestMux;
  NatsTransport? _transport;
  // ignore: cancel_subscriptions
  StreamSubscription<Uint8List>? _reader;
  ServerProtocolParser? _parser;
  Completer<Map<String, Object?>>? _info;
  Completer<NatsException>? _handshakeFailure;
  Completer<void>? _authenticationCancellation;
  Future<void>? _authenticationResolution;
  Future<void>? _authenticationProviderSettlement;
  NatsServer? _currentServer;
  Future<void>? _reconnectFuture;
  Future<void>? _finishCloseFuture;
  Future<void>? _drainFuture;
  Future<NatsTransport>? _pendingTransport;
  Timer? _delayTimer;
  Timer? _pingTimer;
  Timer? _handshakeTransportFailureTimer;
  Completer<void>? _delayCompleter;

  int _epochCounter = 0;
  int _activeEpoch = 0;
  int _generation = 0;
  int _nextSubscriptionId = 1;
  int _nextServerIndex = 0;
  int _maximumPayload = 1024 * 1024;
  int _lastReadyMaximumPayload = 1024 * 1024;
  int _reconnectBufferBytes = 0;
  int _outstandingPings = 0;
  String? _serverId;
  String? _serverName;
  String? _serverVersion;
  String? _serverHost;
  int? _serverPort;
  int? _clientId;
  String? _clientIp;
  int? _protocolVersion;
  bool? _jetStreamAvailable;
  bool? _serverTlsAvailableInfo;
  bool? _serverTlsRequiredInfo;
  bool _headersSupported = false;
  bool _lameDuck = false;
  bool _lastReadyHeadersSupported = false;
  bool _serverTlsAvailable = false;
  bool _serverTlsRequired = false;
  bool _ready = false;
  bool _transportActive = false;
  bool _publicationWriterActive = false;
  bool _connecting = false;
  bool _draining = false;
  bool _drainingPublications = false;
  bool _closing = false;
  NatsException? _terminalError;
  _DirectSubscription? _requestInbox;

  /// Whether a complete ready generation is active.
  bool get isConnected => _ready && !_closing;

  /// Whether graceful shutdown is rejecting new public work.
  bool get isDraining => _draining && !_closing;

  /// Current ready generation, starting at one.
  int get generation => _generation;

  /// Lossy broadcast lifecycle diagnostics after listener attachment.
  ///
  /// Active listeners receive changes in order. A paused listener retains only
  /// the latest change, so this stream is not an audit log.
  Stream<NatsConnectionEvent> get events => _events.stream;

  /// Completes after terminal resource cleanup.
  Future<void> get closed => _closed.future;

  /// Allocates a unique inbox within this connection's configured namespace.
  ///
  /// Core requests and higher level protocols share this allocator so caller
  /// scoped server permissions apply consistently to every reply subscription.
  String newInbox() {
    final prefix = _options.requestInboxPrefix ?? "_INBOX";
    const alphabet =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    final token = StringBuffer();
    for (var index = 0; index < 22; index += 1) {
      token.write(alphabet[_random.nextInt(alphabet.length)]);
    }
    return "$prefix.$token";
  }

  /// Returns an immutable credential free snapshot of the current pool.
  ///
  /// The snapshot includes servers discovered from INFO messages. Later pool
  /// changes do not mutate a previously returned snapshot.
  List<NatsServer> getServers() => List.unmodifiable(_serverPool);

  /// Atomically replaces the pool used by subsequent connection attempts.
  ///
  /// The active connection remains open. All entries are validated before the
  /// current pool changes. A later valid nonempty INFO topology atomically
  /// replaces the implicit portion while configured entries remain owned.
  void setServers(List<NatsServer> servers) {
    final replacement = validateAndSnapshotServers(servers);
    _configuredServers
      ..clear()
      ..addAll(replacement);
    _implicitServers.clear();
    _serverPool
      ..clear()
      ..addAll(replacement);
    _nextServerIndex = 0;
  }

  Future<void> _connectInitial() async {
    NatsException? lastFailure;
    for (var index = 0; index < _serverPool.length; index += 1) {
      final server = _serverPool[index];
      try {
        await _establish(server, attempt: index + 1, reconnecting: false);
        _nextServerIndex = (index + 1) % _serverPool.length;
        return;
      } on NatsException catch (error) {
        lastFailure = error;
      }
    }
    throw lastFailure ?? const NatsConnectionException("Server pool is empty");
  }

  Future<void> _establish(
    NatsServer server, {
    required int attempt,
    required bool reconnecting,
  }) async {
    if (_closing) {
      throw const NatsClosedException();
    }
    _connecting = true;
    _emit(NatsConnecting(server: server, attempt: attempt));
    if (_closing) {
      throw const NatsClosedException();
    }
    final epoch = ++_epochCounter;
    final attemptClock = Stopwatch()..start();
    NatsTransport transport;
    final pendingTransport = connectTransport(
      server,
      _options.tls,
      _options.webSocket,
      _options.connectTimeout,
    );
    _pendingTransport = pendingTransport;
    try {
      transport = await pendingTransport;
    } on Object catch (error, stackTrace) {
      _connecting = false;
      if (error is NatsException) {
        Error.throwWithStackTrace(error, stackTrace);
      }
      throw NatsConnectionException(
        "Could not connect to $server: $error",
        cause: error,
        causeStackTrace: stackTrace,
      );
    } finally {
      if (identical(_pendingTransport, pendingTransport)) {
        _pendingTransport = null;
      }
    }
    if (_closing) {
      await transport.close();
      throw const NatsClosedException();
    }
    final handshakeTimeout = _options.connectTimeout - attemptClock.elapsed;
    if (handshakeTimeout <= Duration.zero) {
      await transport.close();
      _connecting = false;
      throw NatsTimeoutException("Connect attempt");
    }

    _resetNegotiatedCapabilities();
    _activeEpoch = epoch;
    _currentServer = server;
    _transport = transport;
    _parser = ServerProtocolParser(
      maxPayloadBytes: _options.maximumInboundPayloadBytes,
    );
    _info = Completer();
    _handshakeFailure = Completer<NatsException>();
    _handshakeFailure!.future.ignore();
    _authenticationCancellation = Completer<void>();
    _reader = transport.incoming.listen(
      (bytes) => _onData(epoch, bytes),
      onError: (Object error, StackTrace stackTrace) =>
          _onTransportError(epoch, error, stackTrace),
      onDone: () => _onTransportDone(epoch),
      cancelOnError: false,
    );

    try {
      await _handshake(server, transport, epoch, handshakeTimeout);
      _transportActive = true;
      if (reconnecting) {
        _replaySubscriptions();
        await _ping(_options.flushTimeout, epoch: epoch);
        _validateReplayPublications();
        _replayPublications();
      }
      _lastReadyMaximumPayload = _maximumPayload;
      _lastReadyHeadersSupported = _headersSupported;
      _generation += 1;
      _ready = true;
      _connecting = false;
      _handshakeFailure = null;
      _authenticationCancellation = null;
      _startKeepAlive(epoch);
      if (reconnecting) {
        _statistics.recordReconnectSuccess();
      }
      _publicationWriterActive = true;
      _emit(NatsConnected(server: server, generation: _generation));
      if (reconnecting) {
        for (final subscription in _subscriptions.values) {
          subscription.resumeDelivery();
        }
      }
    } on NatsException catch (error) {
      _transportActive = false;
      _publicationWriterActive = false;
      _connecting = false;
      await _disposeActive(epoch, error: error);
      rethrow;
    } on Object catch (error, stackTrace) {
      _transportActive = false;
      _publicationWriterActive = false;
      _connecting = false;
      final failure = NatsConnectionException(
        "Protocol handshake failed: $error",
        cause: error,
        causeStackTrace: stackTrace,
      );
      await _disposeActive(epoch, error: failure);
      throw failure;
    }
  }

  Future<void> _handshake(
    NatsServer server,
    NatsTransport transport,
    int epoch,
    Duration timeout,
  ) async {
    final handshakeFailure = _handshakeFailure!;
    final deadline = DateTime.timestamp().add(timeout);
    final operation = _performHandshake(server, transport, epoch, deadline);
    try {
      await Future.any<void>([
        operation,
        handshakeFailure.future.then<void>((error) => throw error),
      ]).timeout(timeout);
    } on TimeoutException catch (_, stackTrace) {
      await _cancelAuthenticationAttempt();
      Error.throwWithStackTrace(
        NatsTimeoutException("Connect handshake"),
        stackTrace,
      );
    } on NatsConnectionException catch (error, stackTrace) {
      if (error.cause == null) {
        rethrow;
      }
      final remaining = deadline.difference(DateTime.timestamp());
      if (remaining <= Duration.zero) {
        Error.throwWithStackTrace(error, stackTrace);
      }
      late final NatsException serverError;
      try {
        serverError = await handshakeFailure.future.timeout(remaining);
      } on TimeoutException {
        Error.throwWithStackTrace(error, stackTrace);
      }
      throw serverError;
    }
  }

  Future<void> _performHandshake(
    NatsServer server,
    NatsTransport transport,
    int epoch,
    DateTime deadline,
  ) async {
    final info = await _info!.future;
    switch (server.tlsMode) {
      case NatsTlsMode.none:
        if (_serverTlsRequired && server.scheme != "wss") {
          throw const NatsConnectionException(
            "Server requires TLS but a plain address was configured",
          );
        }
      case NatsTlsMode.upgrade:
        if (!_serverTlsAvailable && !_serverTlsRequired) {
          throw const NatsConnectionException(
            "Server did not offer explicit TLS",
          );
        }
        try {
          await transport.startTls(
            host: server.host,
            options: _options.tls,
            timeout: _options.connectTimeout,
          );
        } on NatsException {
          rethrow;
        } on Object catch (error, stackTrace) {
          throw NatsTlsException(
            server.host,
            cause: error,
            causeStackTrace: stackTrace,
          );
        }
      case NatsTlsMode.first:
        break;
    }

    final connectFields = <String, Object?>{
      "verbose": false,
      "pedantic": false,
      "lang": "dart",
      "version": "0.0.1",
      "protocol": 1,
      "echo": _options.echo,
      "headers": true,
      "no_responders": true,
      "tls_required":
          server.tlsMode != NatsTlsMode.none || server.scheme == "wss",
      "name": ?_options.name,
    };
    if (_options.authentication case final authentication?) {
      try {
        final nonce = info["nonce"];
        final credentials = await _resolveAuthentication(
          authentication,
          NatsAuthContext(
            serverInfo: info,
            nonce: nonce is String ? nonce : null,
            deadline: deadline,
            cancelled: _authenticationCancellation!.future,
          ),
        );
        connectFields.addAll(credentials.toConnectFields());
      } on NatsException {
        rethrow;
      } on Object catch (error, stackTrace) {
        throw NatsAuthenticationException(
          "Authentication provider failed",
          cause: error,
          causeStackTrace: stackTrace,
        );
      }
    }
    _writeRaw(_encoder.connect(connectFields), epoch: epoch);
    await _ping(_options.connectTimeout, epoch: epoch);
  }

  Future<NatsConnectCredentials> _resolveAuthentication(
    NatsAuthentication authentication,
    NatsAuthContext context,
  ) async {
    final cancellation = _authenticationCancellation!;
    if (_authenticationProviderSettlement case final previous?) {
      await Future.any<void>([
        previous,
        _cancelledAuthenticationResult(cancellation),
      ]);
    }
    if (cancellation.isCompleted) {
      throw const NatsClosedException();
    }

    final provider = Future.sync(() => authentication.authenticate(context));
    final providerSettled = Completer<void>();
    final providerSettlement = providerSettled.future;
    _authenticationProviderSettlement = providerSettlement;
    unawaited(
      provider.then<void>(
        (_) =>
            _settleAuthenticationProvider(providerSettled, providerSettlement),
        onError: (Object _, StackTrace _) =>
            _settleAuthenticationProvider(providerSettled, providerSettlement),
      ),
    );

    final resolution = Future.any<NatsConnectCredentials>([
      provider,
      _cancelledAuthenticationResult(cancellation),
    ]);
    final resolutionSettled = resolution.then<void>(
      (_) {},
      onError: (Object _, StackTrace _) {},
    );
    _authenticationResolution = resolutionSettled;
    try {
      return await resolution;
    } finally {
      if (identical(_authenticationResolution, resolutionSettled)) {
        _authenticationResolution = null;
      }
    }
  }

  Future<NatsConnectCredentials> _cancelledAuthenticationResult(
    Completer<void> cancellation,
  ) async {
    await cancellation.future;
    final handshakeFailure = _handshakeFailure;
    if (handshakeFailure != null && handshakeFailure.isCompleted) {
      throw await handshakeFailure.future;
    }
    throw const NatsClosedException();
  }

  void _settleAuthenticationProvider(
    Completer<void> providerSettled,
    Future<void> providerSettlement,
  ) {
    if (!providerSettled.isCompleted) {
      providerSettled.complete();
    }
    if (identical(_authenticationProviderSettlement, providerSettlement)) {
      _authenticationProviderSettlement = null;
    }
  }

  Future<void> _cancelAuthenticationAttempt() async {
    final cancellation = _authenticationCancellation;
    if (cancellation != null && !cancellation.isCompleted) {
      cancellation.complete();
    }
    await _authenticationResolution;
  }

  void _resetNegotiatedCapabilities() {
    _serverInfoFields.clear();
    _maximumPayload = min(1024 * 1024, _options.maximumInboundPayloadBytes);
    _headersSupported = false;
    _lameDuck = false;
    _serverTlsAvailable = false;
    _serverTlsRequired = false;
    _serverId = null;
    _serverName = null;
    _serverVersion = null;
    _serverHost = null;
    _serverPort = null;
    _clientId = null;
    _clientIp = null;
    _protocolVersion = null;
    _jetStreamAvailable = null;
    _serverTlsAvailableInfo = null;
    _serverTlsRequiredInfo = null;
  }

  String? _optionalInfoString(Map<String, Object?> fields, String name) {
    final value = fields[name];
    return value is String ? value : null;
  }

  int? _optionalInfoInteger(
    Map<String, Object?> fields,
    String name, {
    required int minimum,
    int? maximum,
  }) {
    final value = fields[name];
    if (value is! int ||
        value < minimum ||
        (maximum != null && value > maximum)) {
      return null;
    }
    return value;
  }

  void _applyInfo(Map<String, Object?> fields, NatsServer server) {
    for (final entry in freezeJsonObject(fields).entries) {
      if (!_immutableInfoFieldNames.contains(entry.key) ||
          !_serverInfoFields.containsKey(entry.key)) {
        _serverInfoFields[entry.key] = entry.value;
      }
    }
    _serverId ??= _optionalInfoString(fields, "server_id");
    _serverName ??= _optionalInfoString(fields, "server_name");
    _serverVersion ??= _optionalInfoString(fields, "version");
    _serverHost ??= _optionalInfoString(fields, "host");
    _serverPort ??= _optionalInfoInteger(
      fields,
      "port",
      minimum: 1,
      maximum: 65535,
    );
    _clientId ??= _optionalInfoInteger(fields, "client_id", minimum: 0);
    _clientIp ??= _optionalInfoString(fields, "client_ip");
    _protocolVersion ??= _optionalInfoInteger(fields, "proto", minimum: 0);
    if (fields["jetstream"] case final bool value) {
      _jetStreamAvailable = value;
    }
    if (fields["headers"] case final bool value) {
      _headersSupported = value;
    }
    if (fields["max_payload"] case final int value when value >= 0) {
      _maximumPayload = min(value, _options.maximumInboundPayloadBytes);
    }
    if (fields["tls_available"] case final bool value) {
      _serverTlsAvailable = value;
      _serverTlsAvailableInfo = value;
    }
    if (fields["tls_required"] case final bool value) {
      _serverTlsRequired = value;
      _serverTlsRequiredInfo = value;
    }
    if (fields["ldm"] case final bool value) {
      final enteredLameDuck = value && !_lameDuck;
      _lameDuck = value;
      if (enteredLameDuck) {
        _emit(
          NatsLameDuckMode(
            generation: _ready ? _generation : _generation + 1,
            serverAuthority: server.uri.authority,
          ),
        );
      }
    }
    if (fields["connect_urls"] case final List<Object?> values) {
      _replaceImplicitServers(values, server);
    }
    if (_ready) {
      _lastReadyMaximumPayload = _maximumPayload;
      _lastReadyHeadersSupported = _headersSupported;
    }
  }

  void _replaceImplicitServers(List<Object?> authorities, NatsServer source) {
    if (authorities.isEmpty ||
        authorities.length > _options.maximumDiscoveredServers) {
      return;
    }
    final replacement = <String, NatsServer>{};
    for (final authority in authorities) {
      if (authority is! String) {
        return;
      }
      final discovered = _parseDiscoveredServer(authority, source);
      if (discovered == null) {
        return;
      }
      replacement.putIfAbsent(_endpointKey(discovered), () => discovered);
    }

    final configuredKeys = _configuredServers.map(_endpointKey).toSet();
    final implicit = <String, NatsServer>{};
    final current = _currentServer;
    if (current != null && !configuredKeys.contains(_endpointKey(current))) {
      implicit[_endpointKey(current)] = current;
    }
    for (final discovered in replacement.values) {
      final key = _endpointKey(discovered);
      if (!configuredKeys.contains(key)) {
        implicit.putIfAbsent(key, () => discovered);
      }
    }
    _implicitServers
      ..clear()
      ..addAll(implicit.values);
    _serverPool
      ..clear()
      ..addAll(_configuredServers)
      ..addAll(_implicitServers);
    _nextServerIndex = 0;
  }

  NatsServer? _parseDiscoveredServer(String authority, NatsServer source) {
    try {
      final parsed = Uri.parse("${source.scheme}://$authority");
      if (parsed.host.isEmpty ||
          parsed.userInfo.isNotEmpty ||
          (parsed.path.isNotEmpty && parsed.path != "/") ||
          parsed.query.isNotEmpty ||
          parsed.fragment.isNotEmpty) {
        return null;
      }
      final port = parsed.hasPort
          ? parsed.port
          : switch (source.scheme) {
              "ws" => 80,
              "wss" => 443,
              _ => 4222,
            };
      final discovered = switch (source.scheme) {
        "ws" => NatsServer.websocket(
          parsed.host,
          port: port,
          path: source.path,
        ),
        "wss" => NatsServer.secureWebSocket(
          parsed.host,
          port: port,
          path: source.path,
        ),
        _ => switch (source.tlsMode) {
          NatsTlsMode.none => NatsServer.nats(parsed.host, port: port),
          NatsTlsMode.upgrade => NatsServer.tls(parsed.host, port: port),
          NatsTlsMode.first => NatsServer.tlsFirst(parsed.host, port: port),
        },
      };
      validateAndSnapshotServers([discovered]);
      return discovered;
    } on FormatException {
      return null;
    } on ArgumentError {
      return null;
    }
  }

  String _endpointKey(NatsServer server) =>
      "${server.scheme}\u0000${server.host.toLowerCase()}\u0000${server.port}"
      "\u0000${server.tlsMode.name}\u0000${server.path}";

  /// Accepts one publication for active or disconnected ordered writing.
  Future<void> publish(
    String subject,
    List<int> payload, {
    String? replySubject,
    NatsHeaders? headers,
    NatsTraceOptions? trace,
  }) => _publish(
    subject,
    payload,
    replySubject: replySubject,
    headers: headers,
    trace: trace,
    bufferWhenDisconnected: true,
  );

  /// Publishes a response to [message] after validating reply availability.
  Future<void> reply(
    NatsMessage message,
    List<int> payload, {
    NatsHeaders? headers,
    NatsTraceOptions? trace,
  }) {
    final subject = message.replySubject;
    if (subject == null) {
      throw const NatsMissingReplySubjectException();
    }
    return publish(subject, payload, headers: headers, trace: trace);
  }

  /// Publishes only while [generation] is still the active ready generation.
  ///
  /// Unlike [publish], this never enters the reconnect buffer. It is intended
  /// for higher level protocols that cannot safely replay a partial operation.
  Future<void> publishOnGeneration(
    String subject,
    List<int> payload, {
    required int generation,
    String? replySubject,
    NatsHeaders? headers,
    NatsTraceOptions? trace,
  }) => _publish(
    subject,
    payload,
    replySubject: replySubject,
    headers: headers,
    trace: trace,
    bufferWhenDisconnected: false,
    expectedGeneration: generation,
  );

  Future<void> _publishRequest(
    String subject,
    List<int> payload, {
    String? replySubject,
    NatsHeaders? headers,
    NatsTraceOptions? trace,
    int? expectedEpoch,
  }) => _publish(
    subject,
    payload,
    replySubject: replySubject,
    headers: headers,
    trace: trace,
    bufferWhenDisconnected: false,
    expectedEpoch: expectedEpoch,
  );

  Future<void> _publish(
    String subject,
    List<int> payload, {
    required bool bufferWhenDisconnected,
    String? replySubject,
    NatsHeaders? headers,
    NatsTraceOptions? trace,
    int? expectedEpoch,
    int? expectedGeneration,
  }) async {
    _requireOpen();
    if (_drainingPublications && expectedEpoch == null) {
      throw const NatsDrainingException();
    }
    if (expectedGeneration != null &&
        (!_publicationWriterActive || expectedGeneration != _generation)) {
      throw NatsPublicationGenerationException(
        operation: "publication",
        expectedGeneration: expectedGeneration,
        actualGeneration: _generation,
      );
    }
    if (expectedEpoch != null &&
        (!_publicationWriterActive || expectedEpoch != _activeEpoch)) {
      throw const NatsConnectionException(
        "Connection changed before request publication",
      );
    }
    validatePublishSubject(
      subject,
      maximumBytes: _options.maximumOutboundControlLineBytes,
    );
    if (replySubject != null) {
      validatePublishSubject(
        replySubject,
        maximumBytes: _options.maximumOutboundControlLineBytes,
      );
    }
    final effectiveHeaders = mergeTraceHeaders(headers, trace);
    _preflightPublication(payload.length, effectiveHeaders);
    final headerBytes = effectiveHeaders?.encode().length ?? 0;
    final totalBytes = payload.length + headerBytes;
    final snapshot = checkedByteSnapshot(payload);
    final frame = _encoder.publish(
      subject: subject,
      payload: snapshot,
      replySubject: replySubject,
      headers: effectiveHeaders,
    );
    if (_publicationWriterActive) {
      _writeRaw(frame);
      _statistics.recordPublication(snapshot.length);
      return;
    }
    if (!bufferWhenDisconnected) {
      throw const NatsConnectionException(
        "Connection was lost before request publication",
      );
    }
    _bufferPublication(
      frame,
      payloadBytes: totalBytes,
      requiresHeaders: effectiveHeaders != null,
    );
    _statistics.recordPublication(snapshot.length);
  }

  void _preflightPublication(int payloadBytes, NatsHeaders? headers) {
    validatePublicationPreflight(
      payloadBytes: payloadBytes,
      headers: headers,
      headersSupported: _publicationWriterActive
          ? _headersSupported
          : _lastReadyHeadersSupported,
      maximumPayloadBytes: _publicationWriterActive
          ? _maximumPayload
          : _lastReadyMaximumPayload,
    );
  }

  void _bufferPublication(
    Uint8List frame, {
    required int payloadBytes,
    required bool requiresHeaders,
  }) {
    final maximum = _options.reconnect.bufferBytes;
    if (maximum == 0 || _reconnectBufferBytes + frame.length > maximum) {
      throw NatsReconnectBufferException(maximumBytes: maximum);
    }
    _reconnectBuffer.add(
      _BufferedPublication(
        frame,
        payloadBytes: payloadBytes,
        requiresHeaders: requiresHeaders,
      ),
    );
    _reconnectBufferBytes += frame.length;
  }

  /// Creates a bounded subscription on the active generation.
  Future<NatsSubscription> subscribe(
    String subject, {
    String? queue,
    NatsSubscriptionOptions? options,
  }) async {
    _requireAcceptingWork();
    validateSubscriptionSubject(
      subject,
      maximumBytes: _options.maximumOutboundControlLineBytes,
    );
    if (queue != null) {
      validateQueueGroup(
        queue,
        maximumBytes: _options.maximumOutboundControlLineBytes,
      );
    }
    final effectiveOptions = options ?? NatsSubscriptionOptions();
    final id = _nextSubscriptionId;
    _nextSubscriptionId += 1;
    late final SubscriptionState state;
    state = SubscriptionState(
      id: id,
      subject: subject,
      queue: queue,
      maximumMessages:
          effectiveOptions.maximumMessages ??
          _options.subscriptionQueueMessages,
      maximumBytes:
          effectiveOptions.maximumBytes ?? _options.subscriptionQueueBytes,
      replayOnReconnect:
          effectiveOptions.reconnect == NatsSubscriptionReconnectPolicy.replay,
      onCancel: () => _unsubscribe(id, state),
      onOverflow: (error) {
        _statistics.recordSlowConsumer(state.queuedMessages + 1);
        _subscriptionOverflow(id, error);
      },
      maximumDeliveries: effectiveOptions.maximumDeliveries,
      onMaximumDeliveries: () {
        if (identical(_subscriptions[id], state)) {
          _subscriptions.remove(id);
        }
        _retiredSubscriptions.add(state);
        state.drain().whenComplete(() {
          _retiredSubscriptions.remove(state);
        }).ignore();
      },
    );
    _subscriptions[id] = state;
    try {
      _writeRaw(
        _encoder.subscribe(subject: subject, subscriptionId: id, queue: queue),
      );
      if (effectiveOptions.maximumDeliveries case final maximum?) {
        _writeRaw(_encoder.unsubscribe(id, maximumDeliveries: maximum));
      }
    } on Object {
      _subscriptions.remove(id);
      state.close();
      rethrow;
    }
    return CoreNatsSubscription(
      state,
      () => _unsubscribe(id, state),
      (maximumDeliveries) => _autoUnsubscribe(id, maximumDeliveries),
      (timeout) => _drainSubscription(id, state, timeout),
    );
  }

  Future<void> _openRequestInbox(
    String subject,
    void Function(NatsMessage) onMessage,
  ) async {
    _requireWriter();
    final current = _requestInbox;
    if (current != null) {
      return;
    }
    validateSubscriptionSubject(
      subject,
      maximumBytes: _options.maximumOutboundControlLineBytes,
    );
    final id = _nextSubscriptionId;
    _nextSubscriptionId += 1;
    final inbox = _DirectSubscription(
      id: id,
      subject: subject,
      onMessage: onMessage,
    );
    _requestInbox = inbox;
    try {
      _writeRaw(_encoder.subscribe(subject: subject, subscriptionId: id));
    } on Object {
      if (identical(_requestInbox, inbox)) {
        _requestInbox = null;
      }
      rethrow;
    }
  }

  Future<void> _closeRequestInbox() async {
    final inbox = _requestInbox;
    if (inbox == null) {
      return;
    }
    _requestInbox = null;
    if (_transportActive) {
      _writeRaw(_encoder.unsubscribe(inbox.id));
    }
  }

  /// Sends a concurrent request that is never retried after disconnect.
  Future<NatsMessage> request(
    String subject,
    List<int> payload, {
    NatsHeaders? headers,
    NatsTraceOptions? trace,
    Duration timeout = const Duration(seconds: 2),
  }) async {
    if (timeout <= Duration.zero) {
      throw ArgumentError.value(timeout, "timeout", "Must be positive");
    }
    _requireAcceptingWork();
    validatePublishSubject(
      subject,
      maximumBytes: _options.maximumOutboundControlLineBytes,
    );
    final effectiveHeaders = mergeTraceHeaders(headers, trace);
    _preflightPublication(payload.length, effectiveHeaders);
    final payloadSnapshot = checkedByteSnapshot(payload);
    final requestEpoch = _activeEpoch;
    return _requestMux.request(
      subject: subject,
      payload: payloadSnapshot,
      timeout: timeout,
      headers: effectiveHeaders,
      connectionEpoch: requestEpoch,
    );
  }

  /// Sends a request only while [generation] remains active.
  Future<NatsMessage> requestOnGeneration(
    String subject,
    List<int> payload, {
    required int generation,
    NatsHeaders? headers,
    NatsTraceOptions? trace,
    Duration timeout = const Duration(seconds: 2),
  }) async {
    if (timeout <= Duration.zero) {
      throw ArgumentError.value(timeout, "timeout", "Must be positive");
    }
    _requireAcceptingWork();
    if (generation != _generation) {
      throw NatsPublicationGenerationException(
        operation: "request",
        expectedGeneration: generation,
        actualGeneration: _generation,
      );
    }
    validatePublishSubject(
      subject,
      maximumBytes: _options.maximumOutboundControlLineBytes,
    );
    final effectiveHeaders = mergeTraceHeaders(headers, trace);
    _preflightPublication(payload.length, effectiveHeaders);
    final payloadSnapshot = checkedByteSnapshot(payload);
    return _requestMux.request(
      subject: subject,
      payload: payloadSnapshot,
      timeout: timeout,
      headers: effectiveHeaders,
      connectionEpoch: _activeEpoch,
    );
  }

  /// Waits for a server PING and PONG processing barrier.
  Future<void> flush({Duration? timeout}) {
    _requireReady();
    return _ping(timeout ?? _options.flushTimeout);
  }

  Future<void> _unsubscribe(int id, [SubscriptionState? expectedState]) async {
    final active = _subscriptions[id];
    if (active != null &&
        expectedState != null &&
        !identical(active, expectedState)) {
      return;
    }
    final state = active ?? expectedState;
    if (state == null || state.isClosed) {
      return;
    }
    if (identical(active, state)) {
      _subscriptions.remove(id);
    }
    _retiredSubscriptions.remove(state);
    try {
      if (_transportActive && active != null) {
        _writeRaw(_encoder.unsubscribe(id));
      }
    } finally {
      state.close();
    }
  }

  Future<void> _autoUnsubscribe(int id, int maximumDeliveries) async {
    final state = _subscriptions[id];
    if (state == null) {
      return;
    }
    final reached = state.setMaximumDeliveries(maximumDeliveries);
    if (!_transportActive) {
      return;
    }
    if (reached) {
      _writeRaw(_encoder.unsubscribe(id));
      return;
    }
    _writeRaw(
      _encoder.unsubscribe(
        id,
        maximumDeliveries: state.serverGenerationMaximumDeliveries,
      ),
    );
  }

  Future<void> _drainSubscription(
    int id,
    SubscriptionState expectedState,
    Duration timeout,
  ) async {
    final active = _subscriptions[id];
    if (active != null && !identical(active, expectedState)) {
      return;
    }
    final state = active ?? expectedState;
    if (state.isClosed) {
      return state.done;
    }
    final deadline = DateTime.timestamp().add(timeout);
    try {
      if (_transportActive && active != null) {
        _writeRaw(_encoder.unsubscribe(id));
        await _ping(_remainingDrain(deadline));
      }
      if (identical(_subscriptions[id], state)) {
        _subscriptions.remove(id);
      }
      await state.drain().timeout(_remainingDrain(deadline));
      _retiredSubscriptions.remove(state);
    } on TimeoutException {
      const error = NatsDrainTimeoutException();
      _subscriptions.remove(id);
      _retiredSubscriptions.remove(state);
      state.close(error);
      throw error;
    }
  }

  void _subscriptionOverflow(int id, NatsSlowConsumerException error) {
    final state = _subscriptions.remove(id);
    if (state == null) {
      return;
    }
    try {
      if (_transportActive) {
        _writeRaw(_encoder.unsubscribe(id));
      }
    } finally {
      state.close(error);
    }
  }

  void _replaySubscriptions() {
    for (final state in _subscriptions.values) {
      if (!state.replayOnReconnect) {
        continue;
      }
      state.beginConnectionGeneration();
      _writeRaw(
        _encoder.subscribe(
          subject: state.subject,
          subscriptionId: state.id,
          queue: state.queue,
        ),
      );
      if (state.remainingDeliveries case final remaining?) {
        _writeRaw(_encoder.unsubscribe(state.id, maximumDeliveries: remaining));
      }
    }
    final inbox = _requestInbox;
    if (inbox != null) {
      _writeRaw(
        _encoder.subscribe(subject: inbox.subject, subscriptionId: inbox.id),
      );
    }
  }

  void _replayPublications() {
    while (_reconnectBuffer.isNotEmpty) {
      final publication = _reconnectBuffer.first;
      if (publication.requiresHeaders && !_headersSupported) {
        throw const NatsConnectionException(
          "Reconnected server cannot accept retained header publication",
        );
      }
      if (publication.payloadBytes > _maximumPayload) {
        throw NatsMaxPayloadException(
          actual: publication.payloadBytes,
          maximum: _maximumPayload,
        );
      }
      _writeRaw(publication.frame);
      _reconnectBuffer.removeFirst();
      _reconnectBufferBytes -= publication.frame.length;
    }
  }

  void _validateReplayPublications() {
    for (final publication in _reconnectBuffer) {
      if (publication.requiresHeaders && !_headersSupported) {
        throw const NatsConnectionException(
          "Reconnected server cannot accept retained header publication",
        );
      }
      if (publication.payloadBytes > _maximumPayload) {
        throw NatsMaxPayloadException(
          actual: publication.payloadBytes,
          maximum: _maximumPayload,
        );
      }
    }
  }

  Future<void> _ping(Duration timeout, {int? epoch}) async {
    if (timeout <= Duration.zero) {
      throw ArgumentError.value(timeout, "timeout", "Must be positive");
    }
    _requireOpen();
    final selectedEpoch = epoch ?? _activeEpoch;
    final transport = _transport;
    if (transport == null || selectedEpoch != _activeEpoch) {
      throw const NatsConnectionException("No active transport");
    }
    final completer = Completer<void>();
    final waiter = _PingWaiter(selectedEpoch, completer, keepAlive: false);
    _pongWaiters.add(waiter);
    final deadline = DateTime.timestamp().add(timeout);
    try {
      _writeRaw(_encoder.ping(), epoch: selectedEpoch);
      await completer.future.timeout(_remaining(deadline));
    } on Object catch (error, stackTrace) {
      var cause = error;
      final handshakeFailure = _handshakeFailure;
      if (_connecting &&
          error is! NatsException &&
          error is! TimeoutException &&
          handshakeFailure != null &&
          !handshakeFailure.isCompleted) {
        final remaining = deadline.difference(DateTime.timestamp());
        if (remaining > Duration.zero) {
          try {
            cause = await handshakeFailure.future.timeout(remaining);
          } on TimeoutException {
            cause = error;
          }
        }
      }
      final failure = switch (cause) {
        TimeoutException() => NatsTimeoutException("Flush"),
        NatsException() => cause,
        _ => NatsConnectionException(
          "Transport flush failed: $cause",
          cause: cause,
          causeStackTrace: stackTrace,
        ),
      };
      if (!_connecting) {
        _connectionLost(selectedEpoch, failure);
      }
      _pongWaiters.remove(waiter);
      Error.throwWithStackTrace(failure, stackTrace);
    }
  }

  Duration _remaining(DateTime deadline) {
    final remaining = deadline.difference(DateTime.timestamp());
    if (remaining <= Duration.zero) {
      throw NatsTimeoutException("Flush");
    }
    return remaining;
  }

  void _writeRaw(Uint8List bytes, {int? epoch}) {
    _requireOpen();
    final selectedEpoch = epoch ?? _activeEpoch;
    final transport = _transport;
    if (transport == null || selectedEpoch != _activeEpoch) {
      throw const NatsConnectionException("No active transport");
    }
    try {
      transport.add(bytes);
      _statistics.recordWireSent(bytes.length);
    } on Object catch (error, stackTrace) {
      final failure = NatsConnectionException(
        "Transport write failed: $error",
        cause: error,
        causeStackTrace: stackTrace,
      );
      if (!_connecting) {
        _connectionLost(selectedEpoch, failure);
      }
      throw failure;
    }
  }

  void _onData(int epoch, Uint8List bytes) {
    if (_closing || epoch != _activeEpoch) {
      return;
    }
    _statistics.recordWireReceived(bytes.length);
    try {
      for (final operation in _parser!.add(bytes)) {
        _handle(epoch, operation);
      }
    } on NatsProtocolException catch (error) {
      _statistics.recordProtocolError();
      _connectionLost(epoch, error);
    } on NatsException catch (error) {
      _connectionLost(epoch, error);
    } on Object catch (error, stackTrace) {
      _statistics.recordProtocolError();
      _connectionLost(
        epoch,
        NatsProtocolException(
          "Protocol processing failed: $error",
          cause: error,
          causeStackTrace: stackTrace,
        ),
      );
    }
  }

  void _handle(int epoch, ServerOperation operation) {
    switch (operation) {
      case ServerInfoOperation(:final fields):
        final server = _currentServer;
        if (server != null) {
          _applyInfo(fields, server);
        }
        final info = _info;
        if (info != null && !info.isCompleted) {
          info.complete(fields);
        }
      case ServerPingOperation():
        _writeRaw(_encoder.pong(), epoch: epoch);
      case ServerPongOperation():
        if (_pongWaiters.isNotEmpty && _pongWaiters.first.epoch == epoch) {
          final waiter = _pongWaiters.removeFirst();
          if (waiter.keepAlive && _outstandingPings > 0) {
            _outstandingPings -= 1;
          }
          waiter.completer.complete();
        }
      case ServerOkOperation():
        break;
      case ServerErrorOperation(:final message):
        final error = classifyServerError(message);
        final handshakeFailure = _handshakeFailure;
        if (_connecting &&
            handshakeFailure != null &&
            !handshakeFailure.isCompleted) {
          _cancelHandshakeTransportFailure();
          handshakeFailure.complete(error);
        }
        if (!_connecting &&
            (error is NatsPermissionException ||
                error is NatsMaximumSubscriptionsException)) {
          _emit(
            NatsServerError(
              error: error as NatsServerException,
              generation: _generation,
            ),
          );
        } else {
          _connectionLost(epoch, error);
        }
      case ServerMessageOperation():
        _statistics.recordMessage(operation.payload.length);
        final message = NatsMessage(
          subject: operation.subject,
          replySubject: operation.replySubject,
          headers: operation.headers,
          payload: operation.payload,
        );
        final inbox = _requestInbox;
        if (inbox != null && inbox.id == operation.subscriptionId) {
          inbox.onMessage(message);
        } else {
          _subscriptions[operation.subscriptionId]?.enqueue(message);
        }
    }
  }

  void _onTransportError(int epoch, Object error, StackTrace stackTrace) {
    final failure = NatsConnectionException(
      "Transport failed: $error",
      cause: error,
      causeStackTrace: stackTrace,
    );
    if (_connecting && !_closing && epoch == _activeEpoch) {
      _deferHandshakeTransportFailure(epoch, failure);
      return;
    }
    _connectionLost(epoch, failure);
  }

  void _onTransportDone(int epoch) {
    const failure = NatsConnectionException("Transport closed");
    if (_connecting && !_closing && epoch == _activeEpoch) {
      _deferHandshakeTransportFailure(epoch, failure);
      return;
    }
    _connectionLost(epoch, failure);
  }

  void _deferHandshakeTransportFailure(
    int epoch,
    NatsConnectionException failure,
  ) {
    if (_handshakeTransportFailureTimer != null) {
      return;
    }
    _handshakeTransportFailureTimer = Timer(Duration.zero, () {
      _handshakeTransportFailureTimer = null;
      if (_closing || !_connecting || epoch != _activeEpoch) {
        return;
      }
      final handshakeFailure = _handshakeFailure;
      if (handshakeFailure != null && !handshakeFailure.isCompleted) {
        handshakeFailure.complete(failure);
      }
      _connectionLost(epoch, failure);
    });
  }

  void _cancelHandshakeTransportFailure() {
    _handshakeTransportFailureTimer?.cancel();
    _handshakeTransportFailureTimer = null;
  }

  void _connectionLost(int epoch, NatsException error) {
    if (_closing || epoch != _activeEpoch) {
      return;
    }
    final lostGeneration = _generation;
    final wasReady = _ready;
    _ready = false;
    _transportActive = false;
    _publicationWriterActive = false;
    _stopKeepAlive();
    for (final subscription in List<SubscriptionState>.of(
      _subscriptions.values,
    )) {
      if (subscription.replayOnReconnect) {
        subscription.pauseDelivery();
      } else {
        _subscriptions.remove(subscription.id);
        subscription.close(error);
      }
    }
    final info = _info;
    if (info != null && !info.isCompleted) {
      info.completeError(error);
    }
    _failPongs(error, epoch: epoch);
    _requestMux.failAll(error);
    _trackDisposal(epoch);

    if (_connecting) {
      return;
    }
    final willReconnect = !_draining && _options.reconnect.maximumAttempts != 0;
    if (wasReady) {
      _emit(
        NatsDisconnected(
          error: error,
          willReconnect: willReconnect,
          generation: lostGeneration,
        ),
      );
    }
    if (!willReconnect) {
      unawaited(_closeWith(error));
      return;
    }
    _reconnectFuture ??= _runReconnect(error).whenComplete(() {
      _reconnectFuture = null;
    });
  }

  Future<void> _runReconnect(NatsException initialError) async {
    var lastError = initialError;
    NatsAuthenticationFailure? repeatedAuthenticationReason;
    var repeatedAuthenticationFailures = 0;
    var attempt = 1;
    while (!_closing &&
        (_options.reconnect.maximumAttempts == -1 ||
            attempt <= _options.reconnect.maximumAttempts)) {
      final defaultServer = _serverPool[_nextServerIndex % _serverPool.length];
      _nextServerIndex = (_nextServerIndex + 1) % _serverPool.length;
      late final ({NatsServer server, Duration delay}) selection;
      try {
        selection = _selectServer(defaultServer);
      } on NatsException catch (error) {
        await _closeWith(error);
        return;
      }
      final server = selection.server;
      final delay = _reconnectDelay(attempt) + selection.delay;
      _emit(NatsReconnecting(server: server, attempt: attempt, delay: delay));
      if (_closing) {
        return;
      }
      await _waitDelay(delay);
      if (_closing) {
        return;
      }
      _statistics.recordReconnectAttempt();
      try {
        await _establish(server, attempt: attempt, reconnecting: true);
        return;
      } on NatsClosedException {
        return;
      } on NatsException catch (error) {
        lastError = error;
        if (!_options.reconnect.retryAuthenticationFailures &&
            error is NatsAuthenticationException) {
          if (error.reason == repeatedAuthenticationReason) {
            repeatedAuthenticationFailures += 1;
          } else {
            repeatedAuthenticationReason = error.reason;
            repeatedAuthenticationFailures = 1;
          }
          if (repeatedAuthenticationFailures >= 2) {
            await _closeWith(error);
            return;
          }
        } else {
          repeatedAuthenticationReason = null;
          repeatedAuthenticationFailures = 0;
        }
      }
      attempt += 1;
    }
    if (!_closing) {
      await _closeWith(lastError);
    }
  }

  ({NatsServer server, Duration delay}) _selectServer(
    NatsServer defaultServer,
  ) {
    final selector = _options.reconnectToServer;
    if (selector == null) {
      return (server: defaultServer, delay: Duration.zero);
    }
    final pool = List<NatsServer>.unmodifiable([
      defaultServer,
      for (final server in _serverPool)
        if (!identical(server, defaultServer)) server,
    ]);
    try {
      final serverInfo = _serverInfoSnapshot;
      if (serverInfo == null) {
        throw StateError("Server information is unavailable for reconnect");
      }
      final selection = selector(pool, serverInfo);
      if (selection == null) {
        return (server: defaultServer, delay: Duration.zero);
      }
      if (selection.delay > _options.reconnect.maximumDelay) {
        throw ArgumentError.value(
          selection.delay,
          "delay",
          "Must not exceed reconnect maximumDelay",
        );
      }
      final selectedIndex = _serverPool.indexWhere(
        (server) => sameServerEndpoint(server, selection.server),
      );
      if (selectedIndex == -1) {
        throw ArgumentError.value(
          selection.server,
          "server",
          "Must be selected from the provided pool",
        );
      }
      return (server: _serverPool[selectedIndex], delay: selection.delay);
    } on Object catch (error, stackTrace) {
      throw NatsConnectionException(
        "Reconnect server selector failed",
        cause: error,
        causeStackTrace: stackTrace,
      );
    }
  }

  NatsServerInfo? get _serverInfoSnapshot {
    final server = _currentServer;
    if (server == null || _serverInfoFields.isEmpty) {
      return null;
    }
    return NatsServerInfo(
      rawFields: _serverInfoFields,
      serverAuthority: server.uri.authority,
      headersSupported: _headersSupported,
      maximumPayloadBytes: _maximumPayload,
      discoveredServerAuthorities: _implicitServers.map(
        (server) => server.uri.authority,
      ),
      isLameDuck: _lameDuck,
      serverId: _serverId,
      serverName: _serverName,
      serverVersion: _serverVersion,
      host: _serverHost,
      port: _serverPort,
      clientId: _clientId,
      clientIp: _clientIp,
      protocolVersion: _protocolVersion,
      jetStreamAvailable: _jetStreamAvailable,
      tlsAvailable: _serverTlsAvailableInfo,
      tlsRequired: _serverTlsRequiredInfo,
    );
  }

  Duration _reconnectDelay(int attempt) {
    final policy = _options.reconnect;
    if (attempt == 1) {
      return policy.initialDelay;
    }
    final exponent = min(attempt - 2, 30);
    final uncapped = policy.baseDelay.inMicroseconds * (1 << exponent);
    final capped = min(uncapped, policy.maximumDelay.inMicroseconds);
    final factor = 1 + ((_random.nextDouble() * 2 - 1) * policy.jitter);
    return Duration(microseconds: max(0, (capped * factor).round()));
  }

  Future<void> _waitDelay(Duration delay) {
    if (delay == Duration.zero) {
      return Future.value();
    }
    final completer = Completer<void>();
    _delayCompleter = completer;
    _delayTimer = Timer(delay, () {
      if (!completer.isCompleted) {
        completer.complete();
      }
    });
    return completer.future.whenComplete(() {
      if (identical(_delayCompleter, completer)) {
        _delayCompleter = null;
        _delayTimer = null;
      }
    });
  }

  void _startKeepAlive(int epoch) {
    _stopKeepAlive();
    _pingTimer = Timer.periodic(_options.pingInterval, (_) {
      if (_closing || !_ready || epoch != _activeEpoch) {
        return;
      }
      if (_outstandingPings >= _options.maximumOutstandingPings) {
        _connectionLost(
          epoch,
          const NatsConnectionException(
            "Server did not answer client keepalive probes",
          ),
        );
        return;
      }
      final completer = Completer<void>();
      completer.future.ignore();
      _pongWaiters.add(_PingWaiter(epoch, completer, keepAlive: true));
      _outstandingPings += 1;
      try {
        _writeRaw(_encoder.ping(), epoch: epoch);
      } on NatsException {
        // The write path already moved this generation into recovery.
      }
    });
  }

  void _stopKeepAlive() {
    _pingTimer?.cancel();
    _pingTimer = null;
    _outstandingPings = 0;
  }

  void _failPongs(Object error, {int? epoch}) {
    final retained = Queue<_PingWaiter>();
    while (_pongWaiters.isNotEmpty) {
      final waiter = _pongWaiters.removeFirst();
      if (epoch == null || waiter.epoch == epoch) {
        if (!waiter.completer.isCompleted) {
          waiter.completer.completeError(error);
        }
      } else {
        retained.add(waiter);
      }
    }
    _pongWaiters.addAll(retained);
  }

  void _trackDisposal(int epoch) {
    late final Future<void> disposal;
    disposal = _disposeActive(epoch).whenComplete(() {
      _disposals.remove(disposal);
    });
    _disposals.add(disposal);
  }

  Future<void> _disposeActive(int epoch, {Object? error}) async {
    if (epoch != _activeEpoch) {
      return;
    }
    if (error != null) {
      _failPongs(error, epoch: epoch);
    }
    _cancelHandshakeTransportFailure();
    _activeEpoch = 0;
    final reader = _reader;
    final transport = _transport;
    await _cancelAuthenticationAttempt();
    _reader = null;
    _transport = null;
    _parser = null;
    _info = null;
    _handshakeFailure = null;
    _authenticationCancellation = null;
    try {
      await reader?.cancel();
    } on Object {
      // Cleanup continues so transport ownership is always released.
    }
    try {
      await transport?.close();
    } on Object {
      // Terminal cleanup is best effort after the cause is fixed.
    }
  }

  void _requireOpen() {
    if (_closing) {
      throw const NatsClosedException();
    }
  }

  void _requireWriter() {
    _requireOpen();
    if (!_transportActive) {
      throw const NatsConnectionException("Connection is reconnecting");
    }
  }

  void _requireReady() {
    _requireOpen();
    if (!_ready) {
      throw const NatsConnectionException("Connection is reconnecting");
    }
  }

  void _requireAcceptingWork() {
    _requireReady();
    if (_draining) {
      throw const NatsDrainingException();
    }
  }

  void _emit(NatsConnectionEvent event) {
    _events.add(event);
  }

  /// Closes every local resource and the transport. Idempotent.
  Future<void> close() => _closeWith(null);

  /// Gracefully delivers accepted work before terminal close.
  Future<void> drain({Duration timeout = const Duration(seconds: 30)}) {
    if (timeout <= Duration.zero) {
      throw ArgumentError.value(timeout, "timeout", "Must be positive");
    }
    final current = _drainFuture;
    if (current != null) {
      return current;
    }
    if (_closing) {
      return closed;
    }
    _requireReady();
    final completion = Completer<void>();
    final operation = completion.future;
    _drainFuture = operation;
    _draining = true;
    _stopKeepAlive();
    _requestMux.stopMany();
    _emit(NatsDraining(generation: _generation));
    unawaited(
      _performDrain(
        timeout,
      ).then<void>(completion.complete, onError: completion.completeError),
    );
    return operation;
  }

  Future<void> _performDrain(Duration timeout) async {
    final deadline = DateTime.timestamp().add(timeout);
    try {
      final activeSubscriptions = List<SubscriptionState>.of(
        _subscriptions.values,
      );
      final subscriptions = <SubscriptionState>{
        ...activeSubscriptions,
        ..._retiredSubscriptions,
      }.toList(growable: false);
      for (final subscription in activeSubscriptions) {
        if (identical(_subscriptions[subscription.id], subscription)) {
          _writeRaw(_encoder.unsubscribe(subscription.id));
        }
      }
      await _ping(_remainingDrain(deadline));
      await Future.wait(
        subscriptions.map((subscription) => subscription.drain()),
      ).timeout(_remainingDrain(deadline));
      for (final subscription in subscriptions) {
        if (identical(_subscriptions[subscription.id], subscription)) {
          _subscriptions.remove(subscription.id);
        }
        _retiredSubscriptions.remove(subscription);
      }
      _drainingPublications = true;
      await _requestMux.idle.timeout(_remainingDrain(deadline));
      await _closeRequestInbox();
      await _ping(_remainingDrain(deadline));
      await _closeWith(null);
    } on TimeoutException {
      const error = NatsDrainTimeoutException();
      await _closeWith(error);
      throw error;
    } on NatsClosedException {
      await closed;
    } on NatsException catch (error) {
      await _closeWith(error);
      rethrow;
    } on Object catch (error, stackTrace) {
      final failure = NatsConnectionException(
        "Connection drain failed: $error",
        cause: error,
        causeStackTrace: stackTrace,
      );
      await _closeWith(failure);
      Error.throwWithStackTrace(failure, stackTrace);
    }
  }

  Duration _remainingDrain(DateTime deadline) {
    final remaining = deadline.difference(DateTime.timestamp());
    if (remaining <= Duration.zero) {
      throw const NatsDrainTimeoutException();
    }
    return remaining;
  }

  Future<void> _closeWith(NatsException? error) {
    if (_closing) {
      return _closed.future;
    }
    _closing = true;
    _ready = false;
    _transportActive = false;
    _publicationWriterActive = false;
    _terminalError = error;
    _delayTimer?.cancel();
    _stopKeepAlive();
    final delay = _delayCompleter;
    if (delay != null && !delay.isCompleted) {
      delay.complete();
    }
    _failPongs(error ?? const NatsClosedException());
    _requestMux.failAll(error ?? const NatsClosedException());
    for (final subscription in _subscriptions.values) {
      subscription.close(error);
    }
    for (final subscription in _retiredSubscriptions) {
      subscription.close(error);
    }
    _subscriptions.clear();
    _retiredSubscriptions.clear();
    _reconnectBuffer.clear();
    _reconnectBufferBytes = 0;
    return _finishClose();
  }

  Future<void> _finishClose() => _finishCloseFuture ??= _completeClose();

  Future<void> _completeClose() async {
    await _requestMux.close();
    // The establish operation owns its pending dial. It observes closing and
    // closes any transport that arrives later. Waiting here would make close
    // inherit the dial timeout.
    final epoch = _activeEpoch;
    if (epoch != 0) {
      await _disposeActive(epoch);
    }
    if (_disposals.isNotEmpty) {
      await Future.wait(_disposals.toList());
    }
    _emit(NatsClosed(error: _terminalError));
    _events.close();
    if (!_closed.isCompleted) {
      _closed.complete();
    }
  }
}

final class _PingWaiter {
  const _PingWaiter(this.epoch, this.completer, {required this.keepAlive});

  final int epoch;
  final Completer<void> completer;
  final bool keepAlive;
}

final class _BufferedPublication {
  const _BufferedPublication(
    this.frame, {
    required this.payloadBytes,
    required this.requiresHeaders,
  });

  final Uint8List frame;
  final int payloadBytes;
  final bool requiresHeaders;
}

final class _DirectSubscription {
  const _DirectSubscription({
    required this.id,
    required this.subject,
    required this.onMessage,
  });

  final int id;
  final String subject;
  final void Function(NatsMessage) onMessage;
}
