part of "async_publish.dart";

/// Retry policy for asynchronous publications.
enum JetStreamAsyncPublishRetryPolicy {
  /// Never retry.
  none,

  /// Retry only a definite no responders failure.
  noResponders,

  /// Retry no responders and ambiguous failures with a stable message ID.
  deduplicated,
}

/// Observable state for one admitted publication.
enum JetStreamAsyncPublishState {
  /// Admitted locally but not yet attempted.
  accepted,

  /// One request attempt is active.
  attempting,

  /// Waiting for a retry delay or a connected generation.
  waitingToRetry,

  /// A valid JetStream acknowledgement arrived.
  acknowledged,

  /// The publication terminated with a failure.
  failed,
}

/// Whether a failed publication may have reached stream storage.
enum JetStreamPublishDisposition {
  /// The failure proves no message was stored.
  definitelyNotStored,

  /// The failure happened after storage became possible.
  possiblyStored,
}

/// Immutable publisher resource and retry bounds.
final class JetStreamAsyncPublisherOptions {
  /// Creates validated publisher bounds.
  factory JetStreamAsyncPublisherOptions({
    int maximumPendingMessages = 1024,
    int maximumPendingBytes = 8 * 1024 * 1024,
    Duration acknowledgementTimeout = const Duration(seconds: 5),
    Duration? attemptTimeout,
    JetStreamAsyncPublishRetryPolicy retryPolicy =
        JetStreamAsyncPublishRetryPolicy.noResponders,
    int maximumAttempts = 3,
    Duration retryDelay = const Duration(milliseconds: 250),
  }) {
    if (maximumPendingMessages <= 0) {
      throw ArgumentError.value(
        maximumPendingMessages,
        "maximumPendingMessages",
        "Must be positive",
      );
    }
    if (maximumPendingBytes <= 0) {
      throw ArgumentError.value(
        maximumPendingBytes,
        "maximumPendingBytes",
        "Must be positive",
      );
    }
    if (acknowledgementTimeout <= Duration.zero) {
      throw ArgumentError.value(
        acknowledgementTimeout,
        "acknowledgementTimeout",
        "Must be positive",
      );
    }
    if (maximumAttempts <= 0) {
      throw ArgumentError.value(
        maximumAttempts,
        "maximumAttempts",
        "Must be positive",
      );
    }
    final effectiveAttemptTimeout =
        attemptTimeout ??
        Duration(
          microseconds:
              acknowledgementTimeout.inMicroseconds ~/ maximumAttempts,
        );
    if (effectiveAttemptTimeout <= Duration.zero ||
        effectiveAttemptTimeout > acknowledgementTimeout) {
      throw ArgumentError.value(
        attemptTimeout,
        "attemptTimeout",
        "Must be positive and no greater than acknowledgementTimeout",
      );
    }
    if (retryDelay < Duration.zero) {
      throw ArgumentError.value(
        retryDelay,
        "retryDelay",
        "Must not be negative",
      );
    }
    return JetStreamAsyncPublisherOptions._(
      maximumPendingMessages,
      maximumPendingBytes,
      acknowledgementTimeout,
      effectiveAttemptTimeout,
      retryPolicy,
      maximumAttempts,
      retryDelay,
    );
  }

  const JetStreamAsyncPublisherOptions._(
    this.maximumPendingMessages,
    this.maximumPendingBytes,
    this.acknowledgementTimeout,
    this.attemptTimeout,
    this.retryPolicy,
    this.maximumAttempts,
    this.retryDelay,
  );

  /// Maximum admitted publications.
  final int maximumPendingMessages;

  /// Maximum retained subject, header, and payload bytes.
  final int maximumPendingBytes;

  /// Total lifetime from admission through every attempt.
  final Duration acknowledgementTimeout;

  /// Maximum lifetime of one request attempt.
  final Duration attemptTimeout;

  /// Retry policy.
  final JetStreamAsyncPublishRetryPolicy retryPolicy;

  /// Maximum attempts including the first attempt.
  final int maximumAttempts;

  /// Delay before each retry.
  final Duration retryDelay;
}

/// One locally admitted asynchronous publication.
final class JetStreamPendingPublish {
  const JetStreamPendingPublish._(this._record);

  final _AsyncPublishRecord _record;

  /// Server acknowledgement future.
  Future<JetStreamPublishAck> get acknowledgement => _record.completer.future;

  /// Current state.
  JetStreamAsyncPublishState get state => _record.state;

  /// Publication subject.
  String get subject => _record.subject;

  /// Stable deduplication identifier, when present.
  String? get messageId => _record.messageId;

  /// Attempts started so far.
  int get attempts => _record.attempts;

  /// Bytes retained against publisher capacity.
  int get retainedBytes => _record.retainedBytes;
}

/// A publication exceeded local publisher capacity.
final class JetStreamAsyncPublishCapacityException implements Exception {
  /// Creates a capacity rejection.
  const JetStreamAsyncPublishCapacityException({
    required this.pendingMessages,
    required this.pendingBytes,
    required this.requestedBytes,
    required this.maximumMessages,
    required this.maximumBytes,
  });

  /// Pending publication count.
  final int pendingMessages;

  /// Retained bytes before this rejection.
  final int pendingBytes;

  /// Bytes requested by this publication.
  final int requestedBytes;

  /// Configured publication bound.
  final int maximumMessages;

  /// Configured byte bound.
  final int maximumBytes;

  @override
  String toString() => "JetStream asynchronous publisher capacity exceeded";
}

/// New work was attempted after drain began.
final class JetStreamAsyncPublisherClosedException implements Exception {
  /// Creates a closed publisher rejection.
  const JetStreamAsyncPublisherClosedException();

  @override
  String toString() => "JetStream asynchronous publisher is closed";
}

/// One asynchronous publication terminated without an acknowledgement.
final class JetStreamAsyncPublishException implements Exception {
  /// Creates a classified publication failure.
  const JetStreamAsyncPublishException({
    required this.cause,
    required this.disposition,
    required this.attempts,
    required this.messageId,
  });

  /// Underlying safe failure.
  final Object cause;

  /// Storage ambiguity classification.
  final JetStreamPublishDisposition disposition;

  /// Attempts that started.
  final int attempts;

  /// Stable message identifier, when present.
  final String? messageId;

  @override
  String toString() => "JetStream asynchronous publish failed: $cause";
}

/// Bounded summary of failures observed before publisher drain.
final class JetStreamAsyncPublisherDrainException implements Exception {
  /// Creates a drain failure summary.
  const JetStreamAsyncPublisherDrainException({
    required this.acknowledged,
    required this.definitelyRejected,
    required this.ambiguous,
    required this.firstCause,
  });

  /// Successful publication count.
  final int acknowledged;

  /// Definite rejection count.
  final int definitelyRejected;

  /// Possibly stored failure count.
  final int ambiguous;

  /// First observed failure cause.
  final Object firstCause;

  @override
  String toString() => "JetStream asynchronous publisher drain failed";
}
