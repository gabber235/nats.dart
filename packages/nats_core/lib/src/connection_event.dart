import "package:nats_core/src/error.dart";
import "package:nats_core/src/options.dart";

/// Ordered observable connection lifecycle event.
sealed class NatsConnectionEvent {
  const NatsConnectionEvent();
}

/// One server connection attempt started.
final class NatsConnecting extends NatsConnectionEvent {
  /// Creates a connecting event.
  const NatsConnecting({required this.server, required this.attempt});

  /// Selected server.
  final NatsServer server;

  /// Attempt number within the current connection cycle.
  final int attempt;
}

/// A complete protocol generation became ready.
final class NatsConnected extends NatsConnectionEvent {
  /// Creates a ready event.
  const NatsConnected({required this.server, required this.generation});

  /// Connected server.
  final NatsServer server;

  /// Monotonically increasing ready generation.
  final int generation;
}

/// The active generation was lost.
final class NatsDisconnected extends NatsConnectionEvent {
  /// Creates a disconnected event.
  const NatsDisconnected({
    required this.error,
    required this.willReconnect,
    required this.generation,
  });

  /// Typed loss cause.
  final NatsException error;

  /// Whether reconnect attempts will follow.
  final bool willReconnect;

  /// Lost generation.
  final int generation;
}

/// A recoverable asynchronous server rejection.
final class NatsServerError extends NatsConnectionEvent {
  /// Creates an asynchronous rejection for the active generation.
  const NatsServerError({required this.error, required this.generation});

  /// Typed server rejection.
  final NatsServerException error;

  /// Generation on which the rejection occurred.
  final int generation;
}

/// A reconnect attempt is waiting for its selected delay.
final class NatsReconnecting extends NatsConnectionEvent {
  /// Creates a reconnecting event.
  const NatsReconnecting({
    required this.server,
    required this.attempt,
    required this.delay,
  });

  /// Next selected server.
  final NatsServer server;

  /// Reconnect attempt number.
  final int attempt;

  /// Delay before transport connection.
  final Duration delay;
}

/// The active server entered lame duck mode.
final class NatsLameDuckMode extends NatsConnectionEvent {
  /// Creates one false to true lame duck transition event.
  const NatsLameDuckMode({
    required this.generation,
    required this.serverAuthority,
  });

  /// Generation that received the INFO transition.
  final int generation;

  /// Credential free authority of the reporting server.
  final String serverAuthority;
}

/// Graceful shutdown stopped accepting new public work.
final class NatsDraining extends NatsConnectionEvent {
  /// Creates a draining transition for the active generation.
  const NatsDraining({required this.generation});

  /// Generation being drained.
  final int generation;
}

/// The connection closed permanently.
final class NatsClosed extends NatsConnectionEvent {
  /// Creates a terminal event.
  const NatsClosed({this.error});

  /// Terminal cause, or `null` after an explicit close.
  final NatsException? error;
}
