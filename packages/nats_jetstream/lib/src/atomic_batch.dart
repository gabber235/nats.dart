import "dart:typed_data";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/byte_snapshot.dart";
import "package:nats_jetstream/src/publish_options.dart";

/// Immutable message staged in one atomic batch.
final class JetStreamAtomicMessage {
  /// Creates a batch message while snapshotting [payload].
  factory JetStreamAtomicMessage(
    String subject,
    List<int> payload, {
    NatsHeaders? headers,
    JetStreamPublishOptions? options,
  }) {
    validatePublishSubject(subject);
    return JetStreamAtomicMessage._(
      subject,
      checkedJetStreamByteSnapshot(payload),
      headers,
      options,
    );
  }

  const JetStreamAtomicMessage._(
    this.subject,
    this.payload,
    this.headers,
    this.options,
  );

  /// Target stream subject.
  final String subject;

  /// Immutable message bytes.
  final Uint8List payload;

  /// Optional user headers.
  final NatsHeaders? headers;

  /// Optional deduplication and expectation controls.
  final JetStreamPublishOptions? options;
}

/// Final acknowledgement for one committed atomic batch.
final class JetStreamAtomicBatchAck {
  /// Creates an immutable batch acknowledgement.
  const JetStreamAtomicBatchAck({
    required this.stream,
    required this.sequence,
    required this.batchId,
    required this.messageCount,
    required this.duplicate,
    this.domain,
  });

  /// Stream that committed the batch.
  final String stream;

  /// Final assigned stream sequence.
  final BigInt sequence;

  /// Caller supplied or generated batch identifier.
  final String batchId;

  /// Number of messages committed atomically.
  final BigInt messageCount;

  /// Whether deduplication suppressed the commit.
  final bool duplicate;

  /// JetStream domain, when configured.
  final String? domain;
}
