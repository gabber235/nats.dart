import "dart:async";

import "package:nats_jetstream/src/kv.dart";
import "package:nats_jetstream/src/wire_integer.dart";

/// Initial delivery mode for a KV watch.
enum JetStreamKeyValueWatchStartKind {
  /// Current value for every matching key.
  current,

  /// Every retained matching revision.
  history,

  /// Revisions created after watch establishment.
  updates,

  /// Revisions at or after one exact stream sequence.
  revision,
}

/// Validated initial position for a KV watch.
final class JetStreamKeyValueWatchStart {
  /// Starts with current values.
  const JetStreamKeyValueWatchStart.current()
    : kind = JetStreamKeyValueWatchStartKind.current,
      revision = null;

  /// Starts with all retained history.
  const JetStreamKeyValueWatchStart.history()
    : kind = JetStreamKeyValueWatchStartKind.history,
      revision = null;

  /// Starts after current stream state.
  const JetStreamKeyValueWatchStart.updates()
    : kind = JetStreamKeyValueWatchStartKind.updates,
      revision = null;

  /// Starts at one positive stream [revision].
  factory JetStreamKeyValueWatchStart.revision(BigInt revision) {
    validatePositiveJetStreamUint64(revision, "revision");
    return JetStreamKeyValueWatchStart._(
      JetStreamKeyValueWatchStartKind.revision,
      revision,
    );
  }

  const JetStreamKeyValueWatchStart._(this.kind, this.revision);

  /// Selected watch mode.
  final JetStreamKeyValueWatchStartKind kind;

  /// Exact starting stream sequence for revision mode.
  final BigInt? revision;
}

/// Bounded KV watch options.
final class JetStreamKeyValueWatchOptions {
  /// Creates validated options.
  factory JetStreamKeyValueWatchOptions({
    String? filter,
    Iterable<String> filters = const [],
    JetStreamKeyValueWatchStart start =
        const JetStreamKeyValueWatchStart.current(),
    bool ignoreDeletes = false,
    int maximumPendingMessages = 1024,
    int maximumPendingBytes = 8 * 1024 * 1024,
    Duration heartbeat = const Duration(seconds: 5),
  }) {
    final capturedFilters = List<String>.unmodifiable(filters);
    if (filter != null && capturedFilters.isNotEmpty) {
      throw ArgumentError("filter and filters are mutually exclusive");
    }
    final effectiveFilters = capturedFilters.isNotEmpty
        ? capturedFilters
        : [filter ?? ">"];
    if (effectiveFilters.toSet().length != effectiveFilters.length) {
      throw ArgumentError.value(filters, "filters", "Must be unique");
    }
    for (final value in effectiveFilters) {
      validateJetStreamKeyValueLogicalFilter(value);
    }
    if (maximumPendingMessages < 1 || maximumPendingMessages > 1000000) {
      throw ArgumentError.value(
        maximumPendingMessages,
        "maximumPendingMessages",
        "Must be between 1 and 1000000",
      );
    }
    if (maximumPendingBytes < 1) {
      throw ArgumentError.value(
        maximumPendingBytes,
        "maximumPendingBytes",
        "Must be positive",
      );
    }
    if (heartbeat < const Duration(milliseconds: 100)) {
      throw ArgumentError.value(heartbeat, "heartbeat", "Too short");
    }
    return JetStreamKeyValueWatchOptions._(
      List.unmodifiable(effectiveFilters),
      start,
      ignoreDeletes,
      maximumPendingMessages,
      maximumPendingBytes,
      heartbeat,
    );
  }

  const JetStreamKeyValueWatchOptions._(
    this.filters,
    this.start,
    this.ignoreDeletes,
    this.maximumPendingMessages,
    this.maximumPendingBytes,
    this.heartbeat,
  );

  /// Immutable relative KV key filters. NATS wildcards are allowed.
  final List<String> filters;

  /// Single relative filter, or `null` when multiple filters are configured.
  String? get filter => filters.length == 1 ? filters.single : null;

  /// Initial delivery mode.
  final JetStreamKeyValueWatchStart start;

  /// Whether delete and purge entries are suppressed.
  final bool ignoreDeletes;

  /// Core delivery queue message bound.
  final int maximumPendingMessages;

  /// Core delivery queue byte bound.
  final int maximumPendingBytes;

  /// Server heartbeat interval.
  final Duration heartbeat;
}

/// One typed KV watch event.
sealed class JetStreamKeyValueWatchEvent {
  const JetStreamKeyValueWatchEvent();
}

/// One matching KV revision.
final class JetStreamKeyValueChanged extends JetStreamKeyValueWatchEvent {
  /// Creates a revision event.
  const JetStreamKeyValueChanged(this.entry);

  /// Immutable KV entry.
  final JetStreamKeyValueEntry entry;
}

/// Explicit boundary between initial state and live updates.
final class JetStreamKeyValueInitialStateComplete
    extends JetStreamKeyValueWatchEvent {
  /// Creates the singleton style marker.
  const JetStreamKeyValueInitialStateComplete();
}

/// Owned continuous KV watch lifecycle.
abstract interface class JetStreamKeyValueWatch {
  /// Single subscription event stream.
  Stream<JetStreamKeyValueWatchEvent> get events;

  /// Stops delivery and cleans owned Core and server state.
  Future<void> close();

  /// Completes when cleanup finishes, or with the terminal watch error.
  Future<void> get done;
}
