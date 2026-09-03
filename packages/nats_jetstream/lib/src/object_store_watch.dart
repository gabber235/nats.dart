import "dart:async";

import "package:nats_jetstream/src/object_store.dart";
import "package:nats_jetstream/src/wire_integer.dart";

/// Initial delivery mode for an Object Store watch.
enum JetStreamObjectWatchStartKind {
  /// Current metadata for every matching object.
  current,

  /// Every retained matching metadata revision.
  history,

  /// Metadata revisions created after watch establishment.
  updates,

  /// Metadata revisions at or after one exact stream sequence.
  revision,
}

/// Validated initial position for an Object Store watch.
final class JetStreamObjectWatchStart {
  /// Starts with current metadata.
  const JetStreamObjectWatchStart.current()
    : kind = JetStreamObjectWatchStartKind.current,
      revision = null;

  /// Starts with all retained metadata history.
  const JetStreamObjectWatchStart.history()
    : kind = JetStreamObjectWatchStartKind.history,
      revision = null;

  /// Starts after current stream state.
  const JetStreamObjectWatchStart.updates()
    : kind = JetStreamObjectWatchStartKind.updates,
      revision = null;

  /// Starts at one positive stream [revision].
  factory JetStreamObjectWatchStart.revision(BigInt revision) {
    validatePositiveJetStreamUint64(revision, "revision");
    return JetStreamObjectWatchStart._(
      JetStreamObjectWatchStartKind.revision,
      revision,
    );
  }

  const JetStreamObjectWatchStart._(this.kind, this.revision);

  /// Selected watch mode.
  final JetStreamObjectWatchStartKind kind;

  /// Exact starting stream sequence for revision mode.
  final BigInt? revision;
}

/// Bounded Object Store watch options.
final class JetStreamObjectWatchOptions {
  /// Creates validated options.
  factory JetStreamObjectWatchOptions({
    Set<String>? names,
    JetStreamObjectWatchStart start = const JetStreamObjectWatchStart.current(),
    bool ignoreDeletes = false,
    int maximumPendingMessages = 1024,
    int maximumPendingBytes = 8 * 1024 * 1024,
    Duration heartbeat = const Duration(seconds: 5),
  }) {
    if (names != null && names.isEmpty) {
      throw ArgumentError.value(names, "names", "Must not be empty");
    }
    final capturedNames = names == null ? null : Set<String>.of(names);
    capturedNames?.forEach(validateJetStreamObjectName);
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
    return JetStreamObjectWatchOptions._(
      capturedNames == null ? null : Set.unmodifiable(capturedNames),
      start,
      ignoreDeletes,
      maximumPendingMessages,
      maximumPendingBytes,
      heartbeat,
    );
  }

  const JetStreamObjectWatchOptions._(
    this.names,
    this.start,
    this.ignoreDeletes,
    this.maximumPendingMessages,
    this.maximumPendingBytes,
    this.heartbeat,
  );

  /// Exact object names to watch, or `null` for every object.
  final Set<String>? names;

  /// Initial delivery mode.
  final JetStreamObjectWatchStart start;

  /// Whether deletion markers are suppressed.
  final bool ignoreDeletes;

  /// Core delivery queue message bound.
  final int maximumPendingMessages;

  /// Core delivery queue byte bound.
  final int maximumPendingBytes;

  /// Server heartbeat interval.
  final Duration heartbeat;
}

/// One typed Object Store watch event.
sealed class JetStreamObjectWatchEvent {
  const JetStreamObjectWatchEvent();
}

/// One matching metadata revision.
final class JetStreamObjectChanged extends JetStreamObjectWatchEvent {
  /// Creates an immutable metadata event.
  const JetStreamObjectChanged(this.info, this.encodedBytes);

  /// Verified Object Store metadata.
  final JetStreamObjectInfo info;

  /// Exact encoded metadata payload size.
  final int encodedBytes;
}

/// Explicit boundary between initial metadata and live updates.
final class JetStreamObjectInitialStateComplete
    extends JetStreamObjectWatchEvent {
  /// Creates the singleton style marker.
  const JetStreamObjectInitialStateComplete();
}

/// Owned continuous Object Store watch lifecycle.
abstract interface class JetStreamObjectWatch {
  /// Single subscription event stream.
  Stream<JetStreamObjectWatchEvent> get events;

  /// Stops delivery and cleans owned Core and server state.
  Future<void> close();

  /// Completes when local cleanup and connected deletion finish.
  Future<void> get done;
}
