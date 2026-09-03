import "package:nats_core/src/statistics.dart";

/// Mutable counters owned by one connection lifecycle.
final class ConnectionStatisticsOwner {
  int _wireBytesSent = 0;
  int _wireBytesReceived = 0;
  int _messagesPublished = 0;
  int _messagesReceived = 0;
  int _payloadBytesPublished = 0;
  int _payloadBytesReceived = 0;
  int _reconnectAttempts = 0;
  int _reconnectSuccesses = 0;
  int _protocolErrors = 0;
  int _slowConsumers = 0;
  int _droppedMessages = 0;

  /// Records bytes accepted by the active transport.
  void recordWireSent(int bytes) {
    _wireBytesSent += bytes;
  }

  /// Records bytes received from the active transport.
  void recordWireReceived(int bytes) {
    _wireBytesReceived += bytes;
  }

  /// Records one accepted caller publication exactly once.
  void recordPublication(int payloadBytes) {
    _messagesPublished += 1;
    _payloadBytesPublished += payloadBytes;
  }

  /// Records one parsed server message exactly once.
  void recordMessage(int payloadBytes) {
    _messagesReceived += 1;
    _payloadBytesReceived += payloadBytes;
  }

  /// Records an actual reconnect transport attempt.
  void recordReconnectAttempt() {
    _reconnectAttempts += 1;
  }

  /// Records an established reconnect generation.
  void recordReconnectSuccess() {
    _reconnectSuccesses += 1;
  }

  /// Records malformed or internally unprocessable inbound protocol data.
  void recordProtocolError() {
    _protocolErrors += 1;
  }

  /// Records one bounded delivery overflow and its discarded messages.
  void recordSlowConsumer(int dropped) {
    _slowConsumers += 1;
    _droppedMessages += dropped;
  }

  /// Captures counters together with gauges owned by adjacent components.
  NatsConnectionStatistics snapshot({
    required int outstandingPings,
    required int activeSubscriptions,
    required int activeRequests,
    required int reconnectRetainedMessages,
    required int reconnectRetainedBytes,
    required int subscriptionQueuedMessages,
    required int subscriptionQueuedBytes,
    required int generation,
    required String? serverAuthority,
  }) => NatsConnectionStatistics(
    wireBytesSent: _wireBytesSent,
    wireBytesReceived: _wireBytesReceived,
    messagesPublished: _messagesPublished,
    messagesReceived: _messagesReceived,
    payloadBytesPublished: _payloadBytesPublished,
    payloadBytesReceived: _payloadBytesReceived,
    reconnectAttempts: _reconnectAttempts,
    reconnectSuccesses: _reconnectSuccesses,
    protocolErrors: _protocolErrors,
    outstandingPings: outstandingPings,
    activeSubscriptions: activeSubscriptions,
    activeRequests: activeRequests,
    reconnectRetainedMessages: reconnectRetainedMessages,
    reconnectRetainedBytes: reconnectRetainedBytes,
    subscriptionQueuedMessages: subscriptionQueuedMessages,
    subscriptionQueuedBytes: subscriptionQueuedBytes,
    slowConsumers: _slowConsumers,
    droppedMessages: _droppedMessages,
    generation: generation,
    serverAuthority: serverAuthority,
  );
}
