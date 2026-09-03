import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/stream_models.dart";
import "package:nats_jetstream/src/wire_integer.dart";

/// One exact page of stream information.
final class JetStreamStreamPage {
  /// Creates an immutable page.
  JetStreamStreamPage({
    required this.total,
    required this.offset,
    required this.limit,
    required List<JetStreamStreamInfo> streams,
  }) : streams = List.unmodifiable(streams);

  /// Total matching streams.
  final BigInt total;

  /// Offset represented by this page.
  final BigInt offset;

  /// Server page limit.
  final BigInt limit;

  /// Immutable stream information in this page.
  final List<JetStreamStreamInfo> streams;
}

/// One exact page of stream names.
final class JetStreamStreamNamePage {
  /// Creates an immutable page.
  JetStreamStreamNamePage({
    required this.total,
    required this.offset,
    required this.limit,
    required List<String> names,
  }) : names = List.unmodifiable(names);

  /// Total matching streams.
  final BigInt total;

  /// Offset represented by this page.
  final BigInt offset;

  /// Server page limit.
  final BigInt limit;

  /// Immutable stream names in this page.
  final List<String> names;
}

/// Optional purge boundary.
final class JetStreamPurgeOptions {
  /// Creates a validated purge boundary.
  factory JetStreamPurgeOptions({
    BigInt? sequence,
    String? filter,
    BigInt? keep,
  }) {
    if (sequence != null) {
      validatePositiveJetStreamUint64(sequence, "sequence");
    }
    if (keep != null) {
      validateJetStreamUint64(keep, "keep");
    }
    if (sequence != null && keep != null) {
      throw ArgumentError("sequence and keep are mutually exclusive");
    }
    if (filter != null) {
      validateSubscriptionSubject(filter);
    }
    return JetStreamPurgeOptions._(sequence, filter, keep);
  }

  const JetStreamPurgeOptions._(this.sequence, this.filter, this.keep);

  /// Purges messages below this stream sequence.
  final BigInt? sequence;

  /// Limits the purge to one subject pattern.
  final String? filter;

  /// Retains this many newest matching messages.
  final BigInt? keep;

  /// Exact request JSON.
  Map<String, Object?> toJson() => {
    "seq": ?sequence,
    "filter": ?filter,
    "keep": ?keep,
  };
}

/// Confirmed stream purge result.
final class JetStreamPurgeResult {
  /// Creates an immutable purge result.
  const JetStreamPurgeResult({required this.purged});

  /// Number of removed messages.
  final BigInt purged;
}
