import "package:nats_core/src/immutable_json.dart";

/// Immutable negotiated server state for the active connection generation.
///
/// [rawFields] retains unknown INFO fields so callers can adopt future server
/// capabilities without waiting for a client release. The current authority
/// never contains credentials.
final class NatsServerInfo {
  /// Creates a deeply immutable server information snapshot.
  factory NatsServerInfo({
    required Map<String, Object?> rawFields,
    required String serverAuthority,
    required bool headersSupported,
    required int maximumPayloadBytes,
    required Iterable<String> discoveredServerAuthorities,
    required bool isLameDuck,
    String? serverId,
    String? serverName,
    String? serverVersion,
    String? host,
    int? port,
    int? clientId,
    String? clientIp,
    int? protocolVersion,
    bool? jetStreamAvailable,
    bool? tlsAvailable,
    bool? tlsRequired,
  }) => NatsServerInfo._(
    freezeJsonObject(rawFields),
    serverAuthority,
    headersSupported,
    maximumPayloadBytes,
    List.unmodifiable(discoveredServerAuthorities),
    isLameDuck,
    serverId,
    serverName,
    serverVersion,
    host,
    port,
    clientId,
    clientIp,
    protocolVersion,
    jetStreamAvailable,
    tlsAvailable,
    tlsRequired,
  );

  const NatsServerInfo._(
    this.rawFields,
    this.serverAuthority,
    this.headersSupported,
    this.maximumPayloadBytes,
    this.discoveredServerAuthorities,
    this.isLameDuck,
    this.serverId,
    this.serverName,
    this.serverVersion,
    this.host,
    this.port,
    this.clientId,
    this.clientIp,
    this.protocolVersion,
    this.jetStreamAvailable,
    this.tlsAvailable,
    this.tlsRequired,
  );

  /// Latest merged INFO fields, including unknown future fields.
  final Map<String, Object?> rawFields;

  /// Credential free authority of the currently selected server.
  final String serverAuthority;

  /// Whether the server negotiated NATS message headers.
  final bool headersSupported;

  /// Effective server and client payload ceiling in bytes.
  final int maximumPayloadBytes;

  /// Validated authorities learned from INFO updates in discovery order.
  final List<String> discoveredServerAuthorities;

  /// Whether the current server most recently reported lame duck mode.
  final bool isLameDuck;

  /// Stable server process identifier from the initial INFO.
  final String? serverId;

  /// Stable configured server name from the initial INFO.
  final String? serverName;

  /// Stable server version from the initial INFO.
  final String? serverVersion;

  /// Stable server host from the initial INFO.
  final String? host;

  /// Stable server port from the initial INFO.
  final int? port;

  /// Stable client identifier assigned by the server.
  final int? clientId;

  /// Stable client address observed by the server.
  final String? clientIp;

  /// Stable negotiated server protocol version.
  final int? protocolVersion;

  /// Whether JetStream is available on the current server.
  final bool? jetStreamAvailable;

  /// Whether the server offers an explicit TLS upgrade.
  final bool? tlsAvailable;

  /// Whether the server requires TLS.
  final bool? tlsRequired;
}
