import "dart:convert";
import "dart:typed_data";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/byte_snapshot.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/publish_options.dart";

/// Server behavior when a fast batch sequence contains a gap.
enum JetStreamFastBatchGapPolicy {
  /// Stop at the first gap and preserve contiguous persistence evidence.
  fail,

  /// Continue ingest while reporting every observed gap.
  ///
  /// The final count remains the server proven contiguous persisted prefix.
  /// It can be lower than the sent count when a gap control message was lost.
  allow,
}

/// Wire commit form for a fast batch.
enum JetStreamFastBatchCommitStrategy {
  /// Commit by storing the final source message as operation 2.
  finalMessage,

  /// Store every source message, then send a nonstorage operation 3 marker.
  endOfBatch,
}

/// Flow control and local memory bounds for one fast ingest batch.
final class JetStreamFastBatchOptions {
  /// Creates validated fast batch options.
  factory JetStreamFastBatchOptions({
    String? batchId,
    int initialFlow = 10,
    int maximumOutstandingAcks = 2,
    int maximumOutstandingBytes = 8 * 1024 * 1024,
    int maximumDiagnostics = 1024,
    Duration controlTimeout = const Duration(seconds: 2),
    JetStreamFastBatchGapPolicy gapPolicy = JetStreamFastBatchGapPolicy.fail,
    JetStreamFastBatchCommitStrategy commitStrategy =
        JetStreamFastBatchCommitStrategy.finalMessage,
  }) {
    if (batchId != null) {
      _validateBatchId(batchId);
    }
    if (initialFlow < 1 || initialFlow > 65535) {
      throw ArgumentError.value(
        initialFlow,
        "initialFlow",
        "Must be between 1 and 65535",
      );
    }
    if (maximumOutstandingAcks < 1 || maximumOutstandingAcks > 3) {
      throw ArgumentError.value(
        maximumOutstandingAcks,
        "maximumOutstandingAcks",
        "Must be between 1 and 3",
      );
    }
    if (maximumOutstandingBytes < 1) {
      throw ArgumentError.value(
        maximumOutstandingBytes,
        "maximumOutstandingBytes",
        "Must be positive",
      );
    }
    if (maximumDiagnostics < 0 || maximumDiagnostics > 1000000) {
      throw ArgumentError.value(
        maximumDiagnostics,
        "maximumDiagnostics",
        "Must be between 0 and 1000000",
      );
    }
    if (controlTimeout <= Duration.zero ||
        controlTimeout >= const Duration(seconds: 10)) {
      throw ArgumentError.value(
        controlTimeout,
        "controlTimeout",
        "Must be positive and shorter than the server idle timeout",
      );
    }
    return JetStreamFastBatchOptions._(
      batchId,
      initialFlow,
      maximumOutstandingAcks,
      maximumOutstandingBytes,
      maximumDiagnostics,
      controlTimeout,
      gapPolicy,
      commitStrategy,
    );
  }

  const JetStreamFastBatchOptions._(
    this.batchId,
    this.initialFlow,
    this.maximumOutstandingAcks,
    this.maximumOutstandingBytes,
    this.maximumDiagnostics,
    this.controlTimeout,
    this.gapPolicy,
    this.commitStrategy,
  );

  /// Caller selected identifier, or `null` for a secure generated value.
  final String? batchId;

  /// Maximum number of messages represented by one server flow ack.
  final int initialFlow;

  /// Number of server flow windows that may remain unacknowledged.
  final int maximumOutstandingAcks;

  /// Local bound for message bytes awaiting persistence evidence.
  final int maximumOutstandingBytes;

  /// Maximum retained gap and rejection diagnostics combined.
  final int maximumDiagnostics;

  /// Maximum silence before a ping, then the deadline for its response.
  final Duration controlTimeout;

  /// Gap handling contract.
  final JetStreamFastBatchGapPolicy gapPolicy;

  /// Whether the final source message or a separate marker commits the batch.
  final JetStreamFastBatchCommitStrategy commitStrategy;
}

/// Immutable message accepted by fast batch ingest.
final class JetStreamFastBatchMessage {
  /// Creates a message while snapshotting [payload].
  factory JetStreamFastBatchMessage(
    String subject,
    List<int> payload, {
    NatsHeaders? headers,
    JetStreamPublishOptions? options,
  }) {
    validatePublishSubject(subject);
    return JetStreamFastBatchMessage._(
      subject,
      checkedJetStreamByteSnapshot(payload),
      headers,
      options,
    );
  }

  const JetStreamFastBatchMessage._(
    this.subject,
    this.payload,
    this.headers,
    this.options,
  );

  /// Stream subject.
  final String subject;

  /// Immutable payload.
  final Uint8List payload;

  /// Optional immutable headers.
  final NatsHeaders? headers;

  /// Per message deduplication and expectation controls.
  final JetStreamPublishOptions? options;
}

/// One gap reported by a non atomic fast batch.
final class JetStreamFastBatchGap {
  /// Creates an exact gap range.
  const JetStreamFastBatchGap({
    required this.expectedSequence,
    required this.receivedSequence,
  });

  /// First missing batch sequence.
  final BigInt expectedSequence;

  /// First sequence observed after the gap.
  final BigInt receivedSequence;
}

/// One per message server rejection reported on the control channel.
final class JetStreamFastBatchMessageError {
  /// Creates a rejection with its exact batch sequence.
  const JetStreamFastBatchMessageError({
    required this.batchSequence,
    required this.error,
  });

  /// Message that triggered the rejection.
  final BigInt batchSequence;

  /// Stable JetStream API rejection.
  final JetStreamApiException error;
}

/// Final evidence returned after the server closes a fast batch.
final class JetStreamFastBatchAck {
  /// Creates an immutable acknowledgement.
  JetStreamFastBatchAck({
    required this.stream,
    required this.sequence,
    required this.batchId,
    required this.messageCount,
    required this.acknowledgedBatchSequence,
    required List<JetStreamFastBatchGap> gaps,
    required List<JetStreamFastBatchMessageError> errors,
    required this.totalGaps,
    required this.totalErrors,
    this.domain,
  }) : gaps = List.unmodifiable(gaps),
       errors = List.unmodifiable(errors) {
    if (totalGaps < gaps.length || totalErrors < errors.length) {
      throw ArgumentError("Diagnostic totals cannot be smaller than details");
    }
  }

  /// Stream that accepted the messages.
  final String stream;

  /// Final assigned stream sequence.
  final BigInt sequence;

  /// Fast batch identifier.
  final String batchId;

  /// Length of the contiguous persisted prefix proven by the final server ack.
  ///
  /// This can be lower than the sent count when a gap control message was lost.
  final BigInt messageCount;

  /// Highest batch sequence covered by flow persistence evidence.
  final BigInt acknowledgedBatchSequence;

  /// Informational gaps observed when gap continuation was enabled.
  final List<JetStreamFastBatchGap> gaps;

  /// Per message rejections observed when gap continuation was enabled.
  final List<JetStreamFastBatchMessageError> errors;

  /// Total gaps observed, including diagnostics not retained.
  final int totalGaps;

  /// Total rejections observed, including diagnostics not retained.
  final int totalErrors;

  /// Whether the configured diagnostic capacity omitted any details.
  bool get diagnosticsTruncated =>
      totalGaps + totalErrors > gaps.length + errors.length;

  /// JetStream domain, when configured.
  final String? domain;
}

/// A fast batch ended without complete persistence evidence.
final class JetStreamFastBatchException implements Exception {
  /// Creates a bounded operation failure.
  const JetStreamFastBatchException({
    required this.message,
    required this.batchId,
    required this.sentSequence,
    required this.acknowledgedSequence,
    required this.ambiguous,
    this.cause,
  });

  /// Safe diagnostic.
  final String message;

  /// Batch whose lifecycle ended.
  final String batchId;

  /// Highest message sequence sent by the client.
  final BigInt sentSequence;

  /// Highest sequence covered by a flow ack.
  final BigInt acknowledgedSequence;

  /// Whether additional sent messages might have persisted.
  final bool ambiguous;

  /// Safe typed cause, when present.
  final Object? cause;

  @override
  String toString() => "JetStreamFastBatchException: $message";
}

void _validateBatchId(String value) {
  if (value.isEmpty ||
      utf8.encode(value).length > 64 ||
      value.contains(".") ||
      value.contains("*") ||
      value.contains(">") ||
      value.codeUnits.any((unit) => unit <= 32 || unit == 127)) {
    throw ArgumentError.value(
      value,
      "batchId",
      "Must be one subject token containing at most 64 characters",
    );
  }
}
