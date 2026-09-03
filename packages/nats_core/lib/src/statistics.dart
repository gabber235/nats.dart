/// Immutable diagnostics captured from one connection at one instant.
final class NatsConnectionStatistics {
  /// Creates a complete immutable statistics snapshot.
  const NatsConnectionStatistics({
    required this.wireBytesSent,
    required this.wireBytesReceived,
    required this.messagesPublished,
    required this.messagesReceived,
    required this.payloadBytesPublished,
    required this.payloadBytesReceived,
    required this.reconnectAttempts,
    required this.reconnectSuccesses,
    required this.protocolErrors,
    required this.outstandingPings,
    required this.activeSubscriptions,
    required this.activeRequests,
    required this.reconnectRetainedMessages,
    required this.reconnectRetainedBytes,
    required this.subscriptionQueuedMessages,
    required this.subscriptionQueuedBytes,
    required this.slowConsumers,
    required this.droppedMessages,
    required this.generation,
    required this.serverAuthority,
  });

  /// Bytes successfully accepted by a transport, including protocol framing.
  final int wireBytesSent;

  /// Bytes supplied by transports, including protocol framing.
  final int wireBytesReceived;

  /// Publications accepted from this client.
  final int messagesPublished;

  /// Server messages parsed for subscriptions and request routing.
  final int messagesReceived;

  /// Caller payload bytes accepted for publication.
  final int payloadBytesPublished;

  /// Parsed message payload bytes received from servers.
  final int payloadBytesReceived;

  /// Connection attempts made after a ready generation was lost.
  final int reconnectAttempts;

  /// Reconnect attempts that established a ready generation.
  final int reconnectSuccesses;

  /// Inbound protocol parsing or processing failures.
  final int protocolErrors;

  /// PING operations currently awaiting a matching PONG.
  final int outstandingPings;

  /// Active user subscriptions, excluding the shared request inbox.
  final int activeSubscriptions;

  /// Active single and multiple response requests.
  final int activeRequests;

  /// Publications retained for reconnect replay.
  final int reconnectRetainedMessages;

  /// Encoded publication bytes retained for reconnect replay.
  final int reconnectRetainedBytes;

  /// Messages currently retained by user subscriptions.
  final int subscriptionQueuedMessages;

  /// Bytes currently retained by user subscriptions.
  final int subscriptionQueuedBytes;

  /// Subscription or request reply buffer overflows observed.
  final int slowConsumers;

  /// Messages discarded by bounded buffer overflows.
  final int droppedMessages;

  /// Current or most recently ready connection generation.
  final int generation;

  /// Credential free current server host and port, when selected.
  final String? serverAuthority;
}
