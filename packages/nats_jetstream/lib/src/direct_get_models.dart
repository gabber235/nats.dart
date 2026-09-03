import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/timestamp.dart";
import "package:nats_jetstream/src/wire_integer.dart";

/// Validated selection and bounds for one direct batch read.
final class JetStreamDirectBatchRequest {
  /// Creates a bounded direct batch request.
  factory JetStreamDirectBatchRequest({
    required int maximumMessages,
    required int maximumBufferedBytes,
    int? maximumServerBytes,
    BigInt? startSequence,
    String? startTime,
    String? nextBySubject,
    List<String>? multipleLastSubjects,
    BigInt? upToSequence,
    String? upToTime,
  }) {
    if (maximumMessages < 1) {
      throw ArgumentError.value(
        maximumMessages,
        "maximumMessages",
        "Must be positive",
      );
    }
    if (maximumBufferedBytes < 1) {
      throw ArgumentError.value(
        maximumBufferedBytes,
        "maximumBufferedBytes",
        "Must be positive",
      );
    }
    if (maximumServerBytes != null && maximumServerBytes < 1) {
      throw ArgumentError.value(
        maximumServerBytes,
        "maximumServerBytes",
        "Must be positive",
      );
    }
    if (startSequence != null) {
      validatePositiveJetStreamUint64(startSequence, "startSequence");
    }
    if (upToSequence != null) {
      validatePositiveJetStreamUint64(upToSequence, "upToSequence");
    }
    _validateTimestamp(startTime, "startTime");
    _validateTimestamp(upToTime, "upToTime");
    if (startSequence != null && startTime != null) {
      throw ArgumentError("startSequence and startTime are mutually exclusive");
    }
    if (upToSequence != null && upToTime != null) {
      throw ArgumentError("upToSequence and upToTime are mutually exclusive");
    }
    if (multipleLastSubjects != null && multipleLastSubjects.isEmpty) {
      throw ArgumentError.value(
        multipleLastSubjects,
        "multipleLastSubjects",
        "Must not be empty",
      );
    }
    if (multipleLastSubjects != null && multipleLastSubjects.length > 1024) {
      throw ArgumentError.value(
        multipleLastSubjects.length,
        "multipleLastSubjects.length",
        "Must not exceed 1024",
      );
    }
    if (multipleLastSubjects != null && nextBySubject != null) {
      throw ArgumentError(
        "multipleLastSubjects and nextBySubject are mutually exclusive",
      );
    }
    if (multipleLastSubjects != null && startTime != null) {
      throw ArgumentError(
        "multipleLastSubjects and startTime are mutually exclusive",
      );
    }
    if (multipleLastSubjects == null &&
        (upToSequence != null || upToTime != null)) {
      throw ArgumentError("An upper boundary requires multipleLastSubjects");
    }
    return JetStreamDirectBatchRequest._(
      maximumMessages,
      maximumBufferedBytes,
      maximumServerBytes,
      startSequence,
      startTime,
      nextBySubject,
      multipleLastSubjects == null
          ? null
          : List.unmodifiable(multipleLastSubjects),
      upToSequence,
      upToTime,
    );
  }

  const JetStreamDirectBatchRequest._(
    this.maximumMessages,
    this.maximumBufferedBytes,
    this.maximumServerBytes,
    this.startSequence,
    this.startTime,
    this.nextBySubject,
    this.multipleLastSubjects,
    this.upToSequence,
    this.upToTime,
  );

  /// Maximum messages requested from the server.
  final int maximumMessages;

  /// Maximum complete wire bytes retained by the client.
  final int maximumBufferedBytes;

  /// Optional response byte limit requested from the server.
  final int? maximumServerBytes;

  /// Inclusive lower stream sequence.
  final BigInt? startSequence;

  /// Inclusive lower RFC 3339 timestamp.
  final String? startTime;

  /// Optional subject filter for ordered batch reads.
  final String? nextBySubject;

  /// Subject patterns whose newest values are requested.
  final List<String>? multipleLastSubjects;

  /// Stable upper stream sequence for multiple subject reads.
  final BigInt? upToSequence;

  /// Stable upper RFC 3339 timestamp for multiple subject reads.
  final String? upToTime;

  /// Exact request JSON.
  Map<String, Object?> toJson() => {
    "batch": maximumMessages,
    "max_bytes": ?maximumServerBytes,
    "seq": ?startSequence,
    "start_time": ?startTime,
    "next_by_subj": ?nextBySubject,
    "multi_last": ?multipleLastSubjects,
    "up_to_seq": ?upToSequence,
    "up_to_time": ?upToTime,
  };
}

/// Complete result of one direct batch read.
final class JetStreamDirectBatchResult {
  /// Creates an immutable batch result.
  JetStreamDirectBatchResult({
    required List<JetStreamStoredMessage> messages,
    required this.pending,
    required this.lastSequence,
    this.upToSequence,
  }) : messages = List.unmodifiable(messages);

  /// Immutable messages returned before the end marker.
  final List<JetStreamStoredMessage> messages;

  /// Matching messages remaining after this page.
  final BigInt pending;

  /// Last stream sequence considered by this page.
  final BigInt lastSequence;

  /// Stable upper cursor for multiple subject pagination.
  final BigInt? upToSequence;
}

/// Direct read status returned by a stream responder.
final class JetStreamDirectGetException implements Exception {
  /// Creates a typed direct read status failure.
  const JetStreamDirectGetException({
    required this.subject,
    required this.statusCode,
    this.description,
  });

  /// API subject that returned the status.
  final String subject;

  /// NATS response status code.
  final int statusCode;

  /// Optional response status description.
  final String? description;

  @override
  String toString() {
    final suffix = description == null ? "" : ": $description";
    return "JetStream direct read failed with $statusCode$suffix";
  }
}

/// Direct batch exceeded its explicit local resource bound.
final class JetStreamDirectBatchLimitException implements Exception {
  /// Creates a typed resource bound failure.
  const JetStreamDirectBatchLimitException(this.message);

  /// Human readable failed bound.
  final String message;

  @override
  String toString() => "JetStream direct batch limit exceeded: $message";
}

/// Direct batch failed before a valid end marker was received.
final class JetStreamDirectBatchException implements Exception {
  /// Creates a failure while snapshotting every partial message.
  JetStreamDirectBatchException({
    required List<JetStreamStoredMessage> partialMessages,
    required this.cause,
  }) : partialMessages = List.unmodifiable(partialMessages);

  /// Messages received before the failure.
  final List<JetStreamStoredMessage> partialMessages;

  /// Exact protocol, transport, timeout, or bound failure.
  final Object cause;

  @override
  String toString() =>
      "JetStream direct batch failed after ${partialMessages.length} messages: "
      "$cause";
}

void _validateTimestamp(String? value, String name) {
  if (value == null) return;
  validateJetStreamRfc3339(value, name);
}
