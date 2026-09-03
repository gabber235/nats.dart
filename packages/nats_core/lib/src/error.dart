/// Base class for failures produced by `package:nats_core`.
sealed class NatsException implements Exception {
  /// Creates a NATS failure with a stable [message].
  const NatsException(this.message, {this.cause, this.causeStackTrace});

  /// Human readable failure detail.
  final String message;

  /// Underlying boundary failure, when one was translated.
  final Object? cause;

  /// Original boundary stack, when available.
  final StackTrace? causeStackTrace;

  @override
  String toString() => "$runtimeType: $message";
}

/// Identifies errors that publication preflight can raise before a wire write.
///
/// When a publication API surfaces this marker from its preflight boundary,
/// no publication protocol bytes were written.
abstract interface class NatsPublicationPreflightException
    implements Exception {}

/// The peer sent bytes that violate the NATS protocol.
final class NatsProtocolException extends NatsException {
  /// Creates a protocol failure.
  const NatsProtocolException(
    super.message, {
    super.cause,
    super.causeStackTrace,
  });
}

/// A subject or queue group is invalid for its operation.
final class NatsSubjectException extends NatsException {
  /// Creates a subject validation failure.
  const NatsSubjectException(super.message);
}

/// A header block is invalid.
final class NatsHeaderException extends NatsException
    implements NatsPublicationPreflightException {
  /// Creates a header validation failure.
  const NatsHeaderException(super.message);
}

/// A connection could not be established or was lost.
base class NatsConnectionException extends NatsException {
  /// Creates a connection failure.
  const NatsConnectionException(
    super.message, {
    super.cause,
    super.causeStackTrace,
  });
}

/// The current Dart runtime cannot perform one requested operation.
final class NatsUnsupportedRuntimeException extends NatsException {
  /// Creates a stable unsupported runtime failure.
  const NatsUnsupportedRuntimeException({required this.operation, this.option})
    : super("The current runtime does not support $operation");

  /// Operation unavailable on this runtime.
  final String operation;

  /// Incompatible option, when one option caused rejection.
  final String? option;
}

/// TLS setup or negotiation failed.
final class NatsTlsException extends NatsConnectionException {
  /// Creates a TLS failure without rendering its underlying cause.
  const NatsTlsException(
    this.host, {
    required super.cause,
    super.causeStackTrace,
  }) : super("TLS negotiation failed for $host");

  /// Credential free server hostname.
  final String host;
}

/// A server rejected or violated the WebSocket HTTP upgrade.
final class NatsWebSocketUpgradeException extends NatsConnectionException {
  /// Creates an upgrade failure without rendering request headers or URLs.
  const NatsWebSocketUpgradeException({
    required super.cause,
    super.causeStackTrace,
  }) : super("WebSocket upgrade failed");
}

/// A generation fenced publication was rejected before any wire write.
final class NatsPublicationGenerationException extends NatsConnectionException
    implements NatsPublicationPreflightException {
  /// Creates an exact generation mismatch for [operation].
  const NatsPublicationGenerationException({
    required this.operation,
    required this.expectedGeneration,
    required this.actualGeneration,
  }) : super("Connection generation changed before $operation");

  /// Publication operation blocked before its wire write.
  final String operation;

  /// Generation owned by the higher level operation.
  final int expectedGeneration;

  /// Generation active when publication preflight ran.
  final int actualGeneration;
}

/// DNS resolution failed before a native transport could connect.
final class NatsDnsException extends NatsException {
  /// Creates a credential free DNS failure for [host].
  const NatsDnsException(
    this.host, {
    required super.cause,
    super.causeStackTrace,
  }) : super("Could not resolve $host");

  /// Configured hostname without URL credentials.
  final String host;
}

/// One failed resolved address from a native connection attempt.
final class NatsConnectCandidateFailure {
  /// Creates one immutable candidate failure.
  const NatsConnectCandidateFailure(
    this.address,
    this.cause,
    this.causeStackTrace,
  );

  /// Resolved IP address without credentials.
  final String address;

  /// Exact socket or TLS failure.
  final Object cause;

  /// Original candidate failure stack.
  final StackTrace causeStackTrace;
}

/// Every resolved native address failed within one attempt deadline.
final class NatsConnectCandidatesException extends NatsException {
  /// Creates an aggregate failure with immutable candidate causes.
  NatsConnectCandidatesException(
    this.host,
    this.port,
    List<NatsConnectCandidateFailure> failures,
  ) : failures = List.unmodifiable(failures),
      super(
        "Could not connect to $host:$port through ${failures.length} addresses",
      );

  /// Configured hostname without URL credentials.
  final String host;

  /// Configured server port.
  final int port;

  /// Candidate failures in attempted order.
  final List<NatsConnectCandidateFailure> failures;
}

/// Base class for errors reported through the server error protocol.
sealed class NatsServerException extends NatsException {
  const NatsServerException(this.serverMessage, super.message);

  /// Text supplied by the server without surrounding protocol syntax.
  final String serverMessage;
}

/// Operation rejected by subject permissions.
enum NatsPermissionOperation {
  /// A message publication was rejected.
  publish,

  /// A reply subject was rejected.
  publishReply,

  /// A subscription was rejected or revoked.
  subscription,

  /// The server did not identify the operation.
  unknown,
}

/// A publish or subscription violated server permissions.
final class NatsPermissionException extends NatsServerException {
  /// Creates a permission failure with parsed context when available.
  NatsPermissionException({
    required String serverMessage,
    required this.operation,
    this.subject,
    this.queue,
  }) : super(serverMessage, "Server denied a $operation operation");

  /// Rejected operation.
  final NatsPermissionOperation operation;

  /// Rejected subject when present in the server error.
  final String? subject;

  /// Rejected queue group when present in the server error.
  final String? queue;
}

/// The connection reached the server subscription limit.
final class NatsMaximumSubscriptionsException extends NatsServerException {
  /// Creates a maximum subscriptions failure.
  const NatsMaximumSubscriptionsException(String serverMessage)
    : super(serverMessage, "Server maximum subscriptions exceeded");
}

/// Scope of a connection limit reported by the server.
enum NatsConnectionLimitScope {
  /// Limit owned by the server process.
  server,

  /// Limit owned by the authenticated account.
  account,
}

/// The server or account connection limit was reached.
final class NatsConnectionLimitException extends NatsServerException {
  /// Creates a connection limit failure.
  NatsConnectionLimitException({
    required String serverMessage,
    required this.scope,
  }) : super(serverMessage, "$scope connection limit exceeded");

  /// Owner of the reached limit.
  final NatsConnectionLimitScope scope;
}

/// The server closed an unresponsive client connection.
final class NatsStaleConnectionException extends NatsServerException {
  /// Creates a stale connection failure.
  const NatsStaleConnectionException(String serverMessage)
    : super(serverMessage, "Server reported a stale connection");
}

/// A future or uncommon server rejection with preserved protocol text.
final class NatsUnknownServerException extends NatsServerException {
  /// Creates an unclassified server failure.
  NatsUnknownServerException(String serverMessage)
    : super(serverMessage, "Server rejected operation: $serverMessage");
}

/// An operation was attempted after terminal close.
final class NatsClosedException extends NatsException {
  /// Creates a closed connection failure.
  const NatsClosedException() : super("Connection is closed");
}

/// New work was attempted while graceful shutdown was active.
final class NatsDrainingException extends NatsException {
  /// Creates a draining lifecycle failure.
  const NatsDrainingException() : super("Connection is draining");
}

/// Graceful shutdown exceeded its caller supplied deadline.
final class NatsDrainTimeoutException extends NatsException {
  /// Creates a drain deadline failure.
  const NatsDrainTimeoutException() : super("Connection drain timed out");
}

/// No subscription was available to answer a request.
final class NatsNoRespondersException extends NatsException {
  /// Creates a no responders failure.
  const NatsNoRespondersException({this.subject})
    : super("No responders are available");

  /// Subject reported by the server, when available.
  final String? subject;
}

/// A message cannot be answered because it has no reply subject.
final class NatsMissingReplySubjectException extends NatsException
    implements NatsPublicationPreflightException {
  /// Creates a missing reply subject failure.
  const NatsMissingReplySubjectException()
    : super("Message does not contain a reply subject");
}

/// An operation exceeded its deadline.
final class NatsTimeoutException extends NatsException {
  /// Creates a timeout for [operation].
  NatsTimeoutException(String operation) : super("$operation timed out");
}

/// A publication exceeds the effective server and client maximum.
final class NatsMaxPayloadException extends NatsException
    implements NatsPublicationPreflightException {
  /// Creates a payload limit failure.
  NatsMaxPayloadException({required this.actual, required this.maximum})
    : super("Payload has $actual bytes, maximum is $maximum");

  /// Actual payload and header bytes.
  final int actual;

  /// Lesser of the negotiated server limit and client memory ceiling.
  final int maximum;
}

/// A subscription exceeded its configured queue bounds.
final class NatsSlowConsumerException extends NatsException {
  /// Creates a slow consumer failure.
  const NatsSlowConsumerException(super.message);
}

/// Stable authentication failure reason.
enum NatsAuthenticationFailure {
  /// Local credentials or provider material was invalid.
  local,

  /// The server rejected connection authorization.
  authorizationViolation,

  /// The server authentication handshake exceeded its deadline.
  authenticationTimeout,

  /// The authenticated user expired.
  userExpired,

  /// The authenticated user was revoked.
  userRevoked,

  /// The authenticated account expired.
  accountExpired,
}

/// Authentication failed locally, was rejected, timed out, expired, or revoked.
final class NatsAuthenticationException extends NatsException {
  /// Creates an authentication failure.
  const NatsAuthenticationException(
    super.message, {
    this.reason = NatsAuthenticationFailure.local,
    super.cause,
    super.causeStackTrace,
  });

  /// Machine readable failure reason.
  final NatsAuthenticationFailure reason;
}

/// A disconnected publication would exceed the configured byte bound.
final class NatsReconnectBufferException extends NatsException {
  /// Creates a reconnect buffer overflow failure.
  NatsReconnectBufferException({required this.maximumBytes})
    : super("Reconnect publication buffer exceeds $maximumBytes bytes");

  /// Configured encoded byte bound.
  final int maximumBytes;
}
