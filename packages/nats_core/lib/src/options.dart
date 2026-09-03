import "dart:convert";
import "dart:typed_data";

import "package:nats_core/src/authentication.dart";
import "package:nats_core/src/byte_snapshot.dart";
import "package:nats_core/src/server_info.dart";
import "package:nats_core/src/subject.dart";

/// TLS handshake position for a native NATS connection.
enum NatsTlsMode {
  /// No TLS.
  none,

  /// Read INFO over TCP, then upgrade before CONNECT.
  upgrade,

  /// Complete TLS before reading INFO.
  first,
}

/// Address of one NATS Server.
final class NatsServer {
  /// Creates a plain TCP server address.
  const NatsServer.nats(this.host, {this.port = 4222})
    : scheme = "nats",
      tlsMode = NatsTlsMode.none,
      path = "/";

  /// Creates an explicit TLS server address.
  const NatsServer.tls(this.host, {this.port = 4222})
    : scheme = "tls",
      tlsMode = NatsTlsMode.upgrade,
      path = "/";

  /// Creates a strict TLS first server address.
  const NatsServer.tlsFirst(this.host, {this.port = 4222})
    : scheme = "tls",
      tlsMode = NatsTlsMode.first,
      path = "/";

  /// Creates a plain WebSocket server address.
  const NatsServer.websocket(this.host, {this.port = 80, this.path = "/"})
    : scheme = "ws",
      tlsMode = NatsTlsMode.none;

  /// Creates a secure WebSocket server address.
  const NatsServer.secureWebSocket(
    this.host, {
    this.port = 443,
    this.path = "/",
  }) : scheme = "wss",
       tlsMode = NatsTlsMode.none;

  /// Parses a supported server [value].
  factory NatsServer.parse(String value) {
    final uri = Uri.tryParse(value);
    if (uri == null || uri.host.isEmpty || uri.userInfo.isNotEmpty) {
      throw ArgumentError("Invalid NATS server URL", "value");
    }
    if (!const {"nats", "tls", "ws", "wss"}.contains(uri.scheme)) {
      throw ArgumentError(
        "Supported schemes are nats, tls, ws, and wss",
        "value",
      );
    }
    final webSocket = uri.scheme == "ws" || uri.scheme == "wss";
    if ((!webSocket && uri.path.isNotEmpty && uri.path != "/") ||
        uri.query.isNotEmpty ||
        uri.fragment.isNotEmpty) {
      throw ArgumentError(
        "NATS server URL contains unsupported URI components",
        "value",
      );
    }
    final port = uri.hasPort
        ? uri.port
        : switch (uri.scheme) {
            "ws" => 80,
            "wss" => 443,
            _ => 4222,
          };
    final path = uri.path.isEmpty ? "/" : uri.path;
    return switch (uri.scheme) {
      "tls" => NatsServer.tls(uri.host, port: port),
      "ws" => NatsServer.websocket(uri.host, port: port, path: path),
      "wss" => NatsServer.secureWebSocket(uri.host, port: port, path: path),
      _ => NatsServer.nats(uri.host, port: port),
    };
  }

  /// URL scheme.
  final String scheme;

  /// TLS behavior for this address.
  final NatsTlsMode tlsMode;

  /// DNS name or IP address.
  final String host;

  /// Server port.
  final int port;

  /// WebSocket request path, or `/` for native transports.
  final String path;

  /// URI without credentials.
  Uri get uri => Uri(
    scheme: scheme,
    host: host,
    port: port,
    path: scheme == "ws" || scheme == "wss" ? path : null,
  );

  @override
  String toString() => uri.toString();
}

/// Native WebSocket handshake customization.
final class NatsWebSocketOptions {
  /// Creates validated immutable handshake headers.
  factory NatsWebSocketOptions({Map<String, String> headers = const {}}) {
    const reservedHeaders = {
      "connection",
      "sec-websocket-key",
      "sec-websocket-protocol",
      "sec-websocket-version",
      "upgrade",
    };
    for (final entry in headers.entries) {
      if (entry.key.isEmpty ||
          entry.key.codeUnits.any((unit) => !_isHttpTokenUnit(unit)) ||
          reservedHeaders.contains(entry.key.toLowerCase()) ||
          entry.value.codeUnits.any(
            (unit) => (unit < 32 && unit != 9) || unit >= 127,
          )) {
        throw ArgumentError("Invalid HTTP header", "headers");
      }
    }
    return NatsWebSocketOptions._(Map.unmodifiable(headers));
  }

  const NatsWebSocketOptions._(this.headers);

  /// Additional native WebSocket upgrade headers.
  ///
  /// Browsers prohibit setting these headers and reject nonempty values.
  final Map<String, String> headers;
}

bool _isHttpTokenUnit(int unit) {
  final alphaNumeric =
      (unit >= 48 && unit <= 57) ||
      (unit >= 65 && unit <= 90) ||
      (unit >= 97 && unit <= 122);
  return alphaNumeric ||
      const {
        33,
        35,
        36,
        37,
        38,
        39,
        42,
        43,
        45,
        46,
        94,
        95,
        96,
        124,
        126,
      }.contains(unit);
}

/// Immutable native TLS credentials and trust options.
final class NatsTlsOptions {
  /// Creates TLS options while snapshotting certificate and key bytes.
  factory NatsTlsOptions({
    List<int>? trustedCertificates,
    List<int>? clientCertificateChain,
    List<int>? clientPrivateKey,
    String? clientPrivateKeyPassword,
    bool allowInvalidCertificates = false,
  }) {
    if ((clientCertificateChain == null) != (clientPrivateKey == null)) {
      throw ArgumentError(
        "Client certificate chain and private key must be supplied together",
      );
    }
    return NatsTlsOptions._(
      _snapshot(trustedCertificates),
      _snapshot(clientCertificateChain),
      _snapshot(clientPrivateKey),
      clientPrivateKeyPassword,
      allowInvalidCertificates,
    );
  }

  const NatsTlsOptions._(
    this.trustedCertificates,
    this.clientCertificateChain,
    this.clientPrivateKey,
    this.clientPrivateKeyPassword,
    this.allowInvalidCertificates,
  );

  /// Additional trusted PEM or PKCS12 certificate bytes.
  final Uint8List? trustedCertificates;

  /// Client certificate chain bytes for mutual TLS.
  final Uint8List? clientCertificateChain;

  /// Client private key bytes for mutual TLS.
  final Uint8List? clientPrivateKey;

  /// Optional client private key password.
  final String? clientPrivateKeyPassword;

  /// Whether an unverifiable server certificate may be accepted.
  final bool allowInvalidCertificates;

  static Uint8List? _snapshot(List<int>? value) =>
      value == null ? null : checkedByteSnapshot(value, name: "TLS material");
}

/// Bounded reconnect and disconnected publication policy.
final class NatsReconnectPolicy {
  /// Creates a validated reconnect policy.
  factory NatsReconnectPolicy({
    int maximumAttempts = 60,
    Duration initialDelay = Duration.zero,
    Duration baseDelay = const Duration(milliseconds: 100),
    Duration maximumDelay = const Duration(seconds: 2),
    double jitter = 0.2,
    int bufferBytes = 8 * 1024 * 1024,
    bool retryAuthenticationFailures = false,
  }) {
    if (maximumAttempts < -1) {
      throw ArgumentError.value(
        maximumAttempts,
        "maximumAttempts",
        "Use minus one for unlimited or a nonnegative value",
      );
    }
    if (initialDelay < Duration.zero ||
        baseDelay <= Duration.zero ||
        maximumDelay < baseDelay) {
      throw ArgumentError("Reconnect delays are invalid");
    }
    if (jitter < 0 || jitter > 1) {
      throw ArgumentError.value(
        jitter,
        "jitter",
        "Must be between zero and one",
      );
    }
    if (bufferBytes < 0) {
      throw ArgumentError.value(
        bufferBytes,
        "bufferBytes",
        "Must not be negative",
      );
    }
    return NatsReconnectPolicy._(
      maximumAttempts,
      initialDelay,
      baseDelay,
      maximumDelay,
      jitter,
      bufferBytes,
      retryAuthenticationFailures,
    );
  }

  const NatsReconnectPolicy._(
    this.maximumAttempts,
    this.initialDelay,
    this.baseDelay,
    this.maximumDelay,
    this.jitter,
    this.bufferBytes,
    this.retryAuthenticationFailures,
  );

  /// Maximum reconnect attempts, or minus one for unlimited.
  final int maximumAttempts;

  /// Delay before the first reconnect attempt.
  final Duration initialDelay;

  /// Exponential delay base after the first attempt.
  final Duration baseDelay;

  /// Maximum reconnect delay before jitter.
  final Duration maximumDelay;

  /// Symmetric delay jitter fraction.
  final double jitter;

  /// Maximum encoded publication bytes retained while disconnected.
  final int bufferBytes;

  /// Whether repeated identical authentication failures may keep reconnecting.
  ///
  /// The default closes after the same typed authentication reason occurs on
  /// two consecutive reconnect attempts. Enable this only when a provider can
  /// rotate credentials and [maximumAttempts] supplies the intended bound, or
  /// when deliberate unlimited credential recovery is required.
  final bool retryAuthenticationFailures;
}

/// One server and optional extra delay selected for a connection attempt.
final class NatsReconnectServerSelection {
  /// Creates a validated server selection.
  ///
  /// [delay] is added to reconnect backoff. The connection rejects a
  /// delay greater than [NatsReconnectPolicy.maximumDelay].
  factory NatsReconnectServerSelection({
    required NatsServer server,
    Duration delay = Duration.zero,
  }) {
    if (delay < Duration.zero) {
      throw ArgumentError.value(delay, "delay", "Must not be negative");
    }
    return NatsReconnectServerSelection._(server, delay);
  }

  const NatsReconnectServerSelection._(this.server, this.delay);

  /// Server from the callback pool snapshot to connect to.
  final NatsServer server;

  /// Extra interruptible delay before dialing [server].
  final Duration delay;
}

/// Selects the server for a reconnect attempt.
///
/// [servers] is an immutable credential free snapshot with the default
/// round robin candidate first. [serverInfo] describes the lost generation.
/// Return null to use the default candidate. The callback must be synchronous.
typedef NatsReconnectServerSelector =
    NatsReconnectServerSelection? Function(
      List<NatsServer> servers,
      NatsServerInfo serverInfo,
    );

/// Immutable Core connection options.
final class NatsOptions {
  /// Creates validated connection options.
  factory NatsOptions({
    required List<NatsServer> servers,
    String? name,
    Duration connectTimeout = const Duration(seconds: 5),
    Duration flushTimeout = const Duration(seconds: 5),
    NatsTlsOptions? tls,
    NatsWebSocketOptions? webSocket,
    NatsAuthentication? authentication,
    NatsReconnectPolicy? reconnect,
    NatsReconnectServerSelector? reconnectToServer,
    bool echo = true,
    int maximumInboundPayloadBytes = 8 * 1024 * 1024,
    int maximumOutboundControlLineBytes = defaultMaximumNatsControlLineBytes,
    int maximumDiscoveredServers = 4096,
    int subscriptionQueueMessages = 1024,
    int subscriptionQueueBytes = 8 * 1024 * 1024,
    Duration pingInterval = const Duration(minutes: 2),
    int maximumOutstandingPings = 2,
  }) {
    final serverSnapshot = validateAndSnapshotServers(servers);
    if (name != null && name.trim().isEmpty) {
      throw ArgumentError.value(name, "name", "Must not be blank");
    }
    if (connectTimeout <= Duration.zero || flushTimeout <= Duration.zero) {
      throw ArgumentError("Connection deadlines must be positive");
    }
    if (maximumInboundPayloadBytes < 1) {
      throw ArgumentError.value(
        maximumInboundPayloadBytes,
        "maximumInboundPayloadBytes",
        "Must be positive",
      );
    }
    if (maximumOutboundControlLineBytes < 64 ||
        maximumOutboundControlLineBytes > 1024 * 1024) {
      throw ArgumentError.value(
        maximumOutboundControlLineBytes,
        "maximumOutboundControlLineBytes",
        "Must be between 64 and 1048576",
      );
    }
    if (maximumDiscoveredServers < 1 || maximumDiscoveredServers > 65536) {
      throw ArgumentError.value(
        maximumDiscoveredServers,
        "maximumDiscoveredServers",
        "Must be between 1 and 65536",
      );
    }
    if (name != null) {
      if (name.length > maximumOutboundControlLineBytes) {
        throw ArgumentError.value(
          name,
          "name",
          "Exceeds the outbound control line byte limit",
        );
      }
      final encodedName = utf8.encode(name);
      if (encodedName.length > maximumOutboundControlLineBytes ||
          utf8.decode(encodedName) != name) {
        throw ArgumentError.value(
          name,
          "name",
          "Must be valid Unicode within the outbound control line byte limit",
        );
      }
    }
    if (subscriptionQueueMessages < 1 || subscriptionQueueBytes < 0) {
      throw ArgumentError("Subscription queue bounds are invalid");
    }
    if (pingInterval <= Duration.zero) {
      throw ArgumentError.value(
        pingInterval,
        "pingInterval",
        "Must be positive",
      );
    }
    if (maximumOutstandingPings < 1 || maximumOutstandingPings > 1000) {
      throw ArgumentError.value(
        maximumOutstandingPings,
        "maximumOutstandingPings",
        "Must be between 1 and 1000",
      );
    }
    return NatsOptions._(
      serverSnapshot,
      name,
      connectTimeout,
      flushTimeout,
      tls ?? NatsTlsOptions(),
      webSocket ?? NatsWebSocketOptions(),
      authentication,
      reconnect ?? NatsReconnectPolicy(),
      reconnectToServer,
      echo,
      maximumInboundPayloadBytes,
      maximumOutboundControlLineBytes,
      maximumDiscoveredServers,
      subscriptionQueueMessages,
      subscriptionQueueBytes,
      pingInterval,
      maximumOutstandingPings,
    );
  }

  const NatsOptions._(
    this.servers,
    this.name,
    this.connectTimeout,
    this.flushTimeout,
    this.tls,
    this.webSocket,
    this.authentication,
    this.reconnect,
    this.reconnectToServer,
    this.echo,
    this.maximumInboundPayloadBytes,
    this.maximumOutboundControlLineBytes,
    this.maximumDiscoveredServers,
    this.subscriptionQueueMessages,
    this.subscriptionQueueBytes,
    this.pingInterval,
    this.maximumOutstandingPings,
  );

  /// Configured server pool.
  final List<NatsServer> servers;

  /// Optional client name visible through server monitoring.
  final String? name;

  /// Complete initial transport and protocol handshake deadline.
  final Duration connectTimeout;

  /// Default PING and PONG barrier deadline.
  final Duration flushTimeout;

  /// Native TLS trust and client identity configuration.
  final NatsTlsOptions tls;

  /// WebSocket handshake options.
  final NatsWebSocketOptions webSocket;

  /// Credentials or provider resolved for every handshake.
  final NatsAuthentication? authentication;

  /// Reconnect and disconnected publication policy.
  final NatsReconnectPolicy reconnect;

  /// Optional server and bounded extra delay selection for reconnect attempts.
  final NatsReconnectServerSelector? reconnectToServer;

  /// Whether this connection receives its own matching publications.
  final bool echo;

  /// Client memory ceiling for one inbound message, including headers.
  ///
  /// The effective publication limit is the lesser of this ceiling and the
  /// server `max_payload`. Raise this only when the server and subscription
  /// queue byte bounds are deliberately sized for larger messages.
  final int maximumInboundPayloadBytes;

  /// Maximum UTF8 bytes in one outbound protocol control line.
  ///
  /// The default matches a normal NATS Server control line limit. Increase it
  /// only with a server configured to accept the same larger bound.
  final int maximumOutboundControlLineBytes;

  /// Maximum distinct servers accepted from one INFO discovery update.
  final int maximumDiscoveredServers;

  /// Maximum messages retained per subscription.
  final int subscriptionQueueMessages;

  /// Maximum retained subject, reply, header, and payload bytes per
  /// subscription.
  final int subscriptionQueueBytes;

  /// Interval between generation scoped client keepalive probes.
  final Duration pingInterval;

  /// Unanswered keepalive probes allowed before disconnect.
  final int maximumOutstandingPings;
}

/// Validates and snapshots one nonempty server pool for package internals.
List<NatsServer> validateAndSnapshotServers(List<NatsServer> servers) {
  if (servers.isEmpty) {
    throw ArgumentError.value(servers, "servers", "At least one is required");
  }
  for (final server in servers) {
    _validateServer(server);
  }
  return List.unmodifiable(servers);
}

/// Compares credential free endpoints for package internals.
bool sameServerEndpoint(NatsServer left, NatsServer right) =>
    left.scheme == right.scheme &&
    left.host.toLowerCase() == right.host.toLowerCase() &&
    left.port == right.port &&
    left.tlsMode == right.tlsMode &&
    left.path == right.path;

void _validateServer(NatsServer server) {
  if (server.host.isEmpty ||
      server.host.codeUnits.any((unit) => unit <= 32 || unit == 127) ||
      server.host.contains(RegExp(r"[/#?@]"))) {
    throw ArgumentError("Server host is invalid", "servers");
  }
  if (server.port < 1 || server.port > 65535) {
    throw ArgumentError("Server port is invalid", "servers");
  }
  if ((server.scheme == "ws" || server.scheme == "wss") &&
      (server.path.isEmpty ||
          !server.path.startsWith("/") ||
          server.path.contains(RegExp(r"[?#\u0000-\u001f\u007f]")))) {
    throw ArgumentError("WebSocket path is invalid", "servers");
  }
}
