import "package:nats_core/src/message.dart";

/// Behavior of a subscription when its active connection generation is lost.
enum NatsSubscriptionReconnectPolicy {
  /// Recreate server interest before the next generation becomes visible.
  replay,

  /// Terminate immediately because replay would violate operation semantics.
  close,
}

/// Per subscription queue and reconnect behavior.
final class NatsSubscriptionOptions {
  /// Creates validated optional queue overrides.
  factory NatsSubscriptionOptions({
    int? maximumMessages,
    int? maximumBytes,
    int? maximumDeliveries,
    NatsSubscriptionReconnectPolicy reconnect =
        NatsSubscriptionReconnectPolicy.replay,
  }) {
    if (maximumMessages != null && maximumMessages < 1) {
      throw ArgumentError.value(
        maximumMessages,
        "maximumMessages",
        "Must be positive",
      );
    }
    if (maximumBytes != null && maximumBytes < 0) {
      throw ArgumentError.value(
        maximumBytes,
        "maximumBytes",
        "Must not be negative",
      );
    }
    if (maximumDeliveries != null && maximumDeliveries < 1) {
      throw ArgumentError.value(
        maximumDeliveries,
        "maximumDeliveries",
        "Must be positive",
      );
    }
    return NatsSubscriptionOptions._(
      maximumMessages,
      maximumBytes,
      maximumDeliveries,
      reconnect,
    );
  }

  const NatsSubscriptionOptions._(
    this.maximumMessages,
    this.maximumBytes,
    this.maximumDeliveries,
    this.reconnect,
  );

  /// Connection default when absent.
  final int? maximumMessages;

  /// Connection default when absent.
  final int? maximumBytes;

  /// Total deliveries accepted before automatic graceful closure.
  final int? maximumDeliveries;

  /// Generation loss behavior.
  final NatsSubscriptionReconnectPolicy reconnect;
}

/// A bounded active NATS subscription.
abstract interface class NatsSubscription {
  /// Local subscription identifier.
  int get id;

  /// Ordered message stream.
  Stream<NatsMessage> get messages;

  /// Completes after the message stream terminates.
  Future<void> get done;

  /// Stops server delivery and starts closing [messages].
  ///
  /// Await [done] when stream termination must be observed before proceeding.
  Future<void> unsubscribe();

  /// Closes automatically after [maximumDeliveries] total deliveries.
  ///
  /// The total includes messages already accepted by this subscription.
  /// Reconnect replays only the remaining allowance.
  Future<void> autoUnsubscribe(int maximumDeliveries);

  /// Stops server delivery and delivers every already accepted message.
  Future<void> drain({Duration timeout = const Duration(seconds: 30)});
}
