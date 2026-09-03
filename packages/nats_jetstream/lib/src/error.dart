/// A typed JetStream JSON API rejection.
final class JetStreamApiException implements Exception {
  /// Creates an API rejection with stable server fields.
  const JetStreamApiException({
    required this.subject,
    required this.code,
    required this.errorCode,
    required this.description,
    this.responseType,
    this.safeResponse,
  });

  /// API subject that produced the rejection.
  final String subject;

  /// HTTP style status code.
  final int code;

  /// Stable NATS JetStream error code.
  final int errorCode;

  /// Human readable server description.
  final String description;

  /// Schema response type, when supplied by the server.
  final String? responseType;

  /// Bounded response JSON with credential shaped fields redacted.
  final String? safeResponse;

  @override
  String toString() {
    return "JetStreamApiException($errorCode, $code): $description";
  }
}

/// A JetStream response violated its JSON wire contract.
final class JetStreamProtocolException implements Exception {
  /// Creates a malformed response failure.
  const JetStreamProtocolException(
    this.message, {
    this.subject,
    this.fieldPath,
    this.cause,
  });

  /// Diagnostic without payload contents.
  final String message;

  /// Request subject associated with the malformed response.
  final String? subject;

  /// JSON field path associated with the failure.
  final String? fieldPath;

  /// Safe underlying parsing failure, when available.
  final Object? cause;

  @override
  String toString() => "JetStreamProtocolException: $message";
}

/// An initial watch snapshot lost its single generation coherence.
final class JetStreamInitialSnapshotInterruptedException implements Exception {
  /// Creates a typed interruption with the last accepted stream revision.
  const JetStreamInitialSnapshotInterruptedException({
    required this.operation,
    required this.lastRevision,
  });

  /// Watch operation label.
  final String operation;

  /// Last accepted revision, when any initial value arrived.
  final BigInt? lastRevision;

  @override
  String toString() =>
      "JetStreamInitialSnapshotInterruptedException: $operation";
}
