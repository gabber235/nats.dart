import "dart:typed_data";

import "package:nats_core/src/byte_snapshot.dart";
import "package:nats_core/src/headers.dart";

/// Immutable message delivered by NATS.
final class NatsMessage {
  /// Creates a message while snapshotting [payload].
  NatsMessage({
    required this.subject,
    required List<int> payload,
    this.replySubject,
    this.headers,
  }) : payload = checkedByteSnapshot(payload);

  /// Published subject.
  final String subject;

  /// Optional reply subject.
  final String? replySubject;

  /// Immutable message headers.
  final NatsHeaders? headers;

  /// Immutable payload bytes.
  final Uint8List payload;
}
