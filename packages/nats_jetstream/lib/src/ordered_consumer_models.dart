import "package:nats_jetstream/src/consumer_config.dart";
import "package:nats_jetstream/src/consumer_consume_options.dart";

/// Configuration for one library owned ordered consumer.
final class JetStreamOrderedConsumerOptions {
  /// Creates validated ordered consumer policy.
  factory JetStreamOrderedConsumerOptions({
    JetStreamConsumerStart start = const JetStreamConsumerStart.all(),
    JetStreamConsumerFilters filters = const JetStreamConsumerFilters.all(),
    JetStreamConsumerReplayPolicy replayPolicy =
        JetStreamConsumerReplayPolicy.instant,
    Duration inactiveThreshold = const Duration(seconds: 5),
    Duration recoveryTimeout = const Duration(seconds: 30),
    bool headersOnly = false,
    Map<String, String> metadata = const {},
  }) {
    if (inactiveThreshold < const Duration(seconds: 1)) {
      throw ArgumentError.value(
        inactiveThreshold,
        "inactiveThreshold",
        "Must be at least one second",
      );
    }
    if (recoveryTimeout <= Duration.zero) {
      throw ArgumentError.value(
        recoveryTimeout,
        "recoveryTimeout",
        "Must be positive",
      );
    }
    return JetStreamOrderedConsumerOptions._(
      start,
      filters,
      replayPolicy,
      inactiveThreshold,
      recoveryTimeout,
      headersOnly,
      Map.unmodifiable(metadata),
    );
  }

  const JetStreamOrderedConsumerOptions._(
    this.start,
    this.filters,
    this.replayPolicy,
    this.inactiveThreshold,
    this.recoveryTimeout,
    this.headersOnly,
    this.metadata,
  );

  /// Initial delivery position used until the first message is accepted.
  final JetStreamConsumerStart start;

  /// Stream subject filters preserved across every recreation.
  final JetStreamConsumerFilters filters;

  /// Delivery pacing preserved across every recreation.
  final JetStreamConsumerReplayPolicy replayPolicy;

  /// Server cleanup bound if explicit cleanup becomes impossible.
  final Duration inactiveThreshold;

  /// Total bound for recreating one lost consumer.
  final Duration recoveryTimeout;

  /// Whether message payload bytes are omitted.
  final bool headersOnly;

  /// Immutable server visible metadata.
  final Map<String, String> metadata;
}

/// Cause that required a fresh hidden consumer.
enum JetStreamOrderedConsumerRecreationReason {
  /// The Core connection generation changed.
  connectionGenerationChanged,

  /// Two requested heartbeat intervals elapsed without server traffic.
  heartbeatMissed,

  /// The hidden server consumer was deleted.
  consumerDeleted,

  /// No server responder currently owns the hidden consumer pull subject.
  noResponders,

  /// Consumer leadership changed while a pull was active.
  leadershipChanged,

  /// Consumer delivery sequence skipped or repeated a value.
  consumerSequenceGap,

  /// Stream sequence repeated or moved backwards.
  streamSequenceRegression,
}

/// Sequence anomaly kind observed by ordered delivery.
enum JetStreamOrderedConsumerGapKind {
  /// Consumer delivery sequence skipped or repeated a value.
  consumer,

  /// Stream sequence moved forwards across retained sequence holes.
  streamForward,

  /// Stream sequence repeated or moved backwards.
  streamRegression,
}

/// Typed lifecycle diagnostic for ordered delivery.
sealed class JetStreamOrderedConsumerEvent {
  const JetStreamOrderedConsumerEvent();
}

/// The first hidden consumer became ready.
final class JetStreamOrderedConsumerCreated
    extends JetStreamOrderedConsumerEvent {
  /// Creates immutable creation evidence.
  const JetStreamOrderedConsumerCreated({
    required this.consumer,
    required this.generation,
  });

  /// Hidden consumer name useful for server diagnostics.
  final String consumer;

  /// Core connection generation that created the consumer.
  final int generation;
}

/// One ordered sequence anomaly was detected.
final class JetStreamOrderedConsumerGap extends JetStreamOrderedConsumerEvent {
  /// Creates immutable sequence diagnostics.
  const JetStreamOrderedConsumerGap({
    required this.kind,
    required this.consumer,
    required this.expected,
    required this.received,
    required this.previousStreamSequence,
    required this.receivedStreamSequence,
    required this.causesRecreation,
  });

  /// Sequence space containing the anomaly.
  final JetStreamOrderedConsumerGapKind kind;

  /// Hidden consumer that emitted the message.
  final String consumer;

  /// Next expected value in the selected sequence space.
  final BigInt expected;

  /// Value received in the selected sequence space.
  final BigInt received;

  /// Last stream sequence delivered to the application.
  final BigInt? previousStreamSequence;

  /// Stream sequence carried by the rejected or accepted message.
  final BigInt receivedStreamSequence;

  /// Whether this anomaly discards the message and recreates the consumer.
  final bool causesRecreation;
}

/// A replacement hidden consumer became ready.
final class JetStreamOrderedConsumerRecreated
    extends JetStreamOrderedConsumerEvent {
  /// Creates immutable recreation evidence.
  const JetStreamOrderedConsumerRecreated({
    required this.previousConsumer,
    required this.consumer,
    required this.reason,
    required this.nextStreamSequence,
    required this.generation,
  });

  /// Hidden consumer replaced by this generation.
  final String previousConsumer;

  /// New hidden consumer name.
  final String consumer;

  /// Exact condition that required replacement.
  final JetStreamOrderedConsumerRecreationReason reason;

  /// First stream sequence requested from the replacement.
  final BigInt? nextStreamSequence;

  /// Core connection generation that created the replacement.
  final int generation;
}

/// One bounded ordered delivery operation.
abstract interface class JetStreamOrderedConsumerStream
    implements JetStreamConsumerStream {
  /// Lossy broadcast lifecycle and ordering diagnostics.
  ///
  /// Active listeners receive events in order. A paused listener retains only
  /// the latest event, so this stream is not an audit log.
  Stream<JetStreamOrderedConsumerEvent> get events;
}

/// Ordered consumer recreation exceeded its configured total deadline.
final class JetStreamOrderedConsumerRecoveryException implements Exception {
  /// Creates a failure that preserves the last recovery cause.
  const JetStreamOrderedConsumerRecoveryException({
    required this.timeout,
    required this.cause,
  });

  /// Configured total recreation deadline.
  final Duration timeout;

  /// Last failure observed while creating replacement state.
  final Object cause;

  @override
  String toString() {
    return "JetStreamOrderedConsumerRecoveryException: "
        "recovery exceeded $timeout";
  }
}
