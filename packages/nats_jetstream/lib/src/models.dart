import "dart:typed_data";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/byte_snapshot.dart";

export "package:nats_jetstream/src/account_models.dart";

/// Immutable message retrieved from one JetStream stream.
final class JetStreamStoredMessage {
  /// Creates a stored message while snapshotting [payload].
  JetStreamStoredMessage({
    required this.subject,
    required this.sequence,
    required List<int> payload,
    required this.timestamp,
    this.headers,
  }) : payload = checkedJetStreamByteSnapshot(payload);

  /// Original message subject.
  final String subject;

  /// Exact stream sequence.
  final BigInt sequence;

  /// Immutable message bytes.
  final Uint8List payload;

  /// Original RFC 3339 timestamp without precision loss.
  final String timestamp;

  /// Original message headers, when present.
  final NatsHeaders? headers;
}

/// Acknowledgement for one stored publication.
final class JetStreamPublishAck {
  /// Creates a publish acknowledgement.
  const JetStreamPublishAck({
    required this.stream,
    required this.sequence,
    required this.duplicate,
    this.domain,
  });

  /// Stream that stored the message.
  final String stream;

  /// Assigned stream sequence.
  final BigInt sequence;

  /// Whether duplicate detection suppressed storage.
  final bool duplicate;

  /// JetStream domain, when configured.
  final String? domain;
}

/// Exact JetStream duration represented as integer nanoseconds.
final class JetStreamNanoseconds {
  /// Creates an exact duration from [nanoseconds].
  const JetStreamNanoseconds(this.nanoseconds);

  /// Creates an exact duration from Dart microseconds.
  factory JetStreamNanoseconds.fromDuration(Duration duration) {
    return JetStreamNanoseconds(
      BigInt.from(duration.inMicroseconds) * BigInt.from(1000),
    );
  }

  /// Exact signed nanosecond count.
  final BigInt nanoseconds;

  /// Converts to a Dart duration when no submicrosecond precision is present.
  Duration toDuration() {
    final remainder = nanoseconds.remainder(BigInt.from(1000));
    if (remainder != BigInt.zero) {
      throw StateError("Nanoseconds are not exactly representable by Duration");
    }
    final microseconds = nanoseconds ~/ BigInt.from(1000);
    if (!microseconds.isValidInt) {
      throw StateError("Nanoseconds are outside the Dart Duration range");
    }
    return Duration(microseconds: microseconds.toInt());
  }
}

/// Acknowledgement for one distributed counter increment.
final class JetStreamCounterAck {
  /// Creates an immutable counter acknowledgement.
  const JetStreamCounterAck({
    required this.stream,
    required this.sequence,
    required this.value,
    this.domain,
  });

  /// Stream that stored the increment.
  final String stream;

  /// Assigned stream sequence.
  final BigInt sequence;

  /// Exact counter value after the increment.
  final BigInt value;

  /// JetStream domain, when configured.
  final String? domain;
}
