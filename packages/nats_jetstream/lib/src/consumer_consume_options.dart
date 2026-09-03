import "package:nats_jetstream/src/consumer_delivery.dart";
import "package:nats_jetstream/src/consumer_duration.dart";
import "package:nats_jetstream/src/consumer_fetch.dart";
import "package:nats_jetstream/src/continuous_delivery_capacity.dart";

/// Bounded continuous pull delivery options.
final class JetStreamConsumeOptions {
  /// Creates validated continuous pull options.
  factory JetStreamConsumeOptions({
    int batchSize = 100,
    Duration expires = const Duration(seconds: 30),
    Duration idleHeartbeat = const Duration(seconds: 5),
    int maximumPendingMessages = 1024,
    int maximumPendingBytes = 8 * 1024 * 1024,
    JetStreamFetchPriority? priority,
  }) {
    if (batchSize < 1 || batchSize > 1000000) {
      throw ArgumentError.value(
        batchSize,
        "batchSize",
        "Must be between 1 and 1000000",
      );
    }
    if (expires <= Duration.zero ||
        idleHeartbeat < const Duration(milliseconds: 100) ||
        idleHeartbeat.inMicroseconds > expires.inMicroseconds ~/ 2) {
      throw ArgumentError("Continuous pull deadlines are invalid");
    }
    consumerDurationNanoseconds(expires, "expires");
    consumerDurationNanoseconds(idleHeartbeat, "idleHeartbeat");
    if (maximumPendingMessages < 1 || maximumPendingMessages > 1000000) {
      throw ArgumentError.value(
        maximumPendingMessages,
        "maximumPendingMessages",
        "Must be between 1 and 1000000",
      );
    }
    if (maximumPendingBytes < 1 ||
        maximumPendingBytes > maximumContinuousDeliveryPendingBytes) {
      throw ArgumentError("Continuous pull queue bounds must be positive");
    }
    if (batchSize > maximumPendingMessages) {
      throw ArgumentError.value(
        batchSize,
        "batchSize",
        "Must not exceed maximumPendingMessages",
      );
    }
    return JetStreamConsumeOptions._(
      batchSize,
      expires,
      idleHeartbeat,
      maximumPendingMessages,
      maximumPendingBytes,
      priority,
    );
  }

  const JetStreamConsumeOptions._(
    this.batchSize,
    this.expires,
    this.idleHeartbeat,
    this.maximumPendingMessages,
    this.maximumPendingBytes,
    this.priority,
  );

  /// Messages requested per server pull.
  final int batchSize;

  /// Lifetime of each server pull request.
  final Duration expires;

  /// Server heartbeat interval.
  final Duration idleHeartbeat;

  /// Maximum deliveries retained while application demand is paused.
  final int maximumPendingMessages;

  /// Maximum delivery bytes retained while application demand is paused.
  final int maximumPendingBytes;

  /// Optional policy specific priority request.
  final JetStreamFetchPriority? priority;
}

/// One bounded continuous pull operation.
abstract interface class JetStreamConsumerStream {
  /// Single listener delivery stream.
  Stream<JetStreamDelivery> get messages;

  /// Completes after cleanup or with the terminal delivery failure.
  Future<void> get done;

  /// Stops pulls immediately and discards buffered deliveries.
  Future<void> stop();

  /// Stops server delivery and emits every already accepted delivery.
  Future<void> drain({Duration timeout = const Duration(seconds: 30)});
}
