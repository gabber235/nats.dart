part of "package:nats_core/src/nats_connection.dart";

/// Diagnostics that do not mutate connection lifecycle state.
extension NatsConnectionDiagnostics on NatsConnection {
  /// Immutable latest INFO and negotiated capability snapshot.
  NatsServerInfo get serverInfo {
    final snapshot = _serverInfoSnapshot;
    if (snapshot == null) {
      throw const NatsConnectionException("Server information is unavailable");
    }
    return snapshot;
  }

  /// Immutable counters and live gauges captured at access time.
  NatsConnectionStatistics get statistics {
    var queuedMessages = 0;
    var queuedBytes = 0;
    for (final subscription in _subscriptions.values) {
      queuedMessages += subscription.queuedMessages;
      queuedBytes += subscription.queuedBytes;
    }
    for (final subscription in _retiredSubscriptions) {
      queuedMessages += subscription.queuedMessages;
      queuedBytes += subscription.queuedBytes;
    }
    return _statistics.snapshot(
      outstandingPings: _pongWaiters.length,
      activeSubscriptions: _subscriptions.length,
      activeRequests: _requestMux.pending,
      reconnectRetainedMessages: _reconnectBuffer.length,
      reconnectRetainedBytes: _reconnectBufferBytes,
      subscriptionQueuedMessages: queuedMessages,
      subscriptionQueuedBytes: queuedBytes,
      generation: _generation,
      serverAuthority: _currentServer?.uri.authority,
    );
  }

  /// Measures one PING and PONG barrier on the active generation.
  Future<Duration> rtt({Duration? timeout}) async {
    _requireReady();
    final clock = Stopwatch()..start();
    await _ping(timeout ?? _options.flushTimeout);
    clock.stop();
    return clock.elapsed;
  }
}

/// Multiple response requests routed through the shared connection inbox.
extension NatsConnectionMultipleRequests on NatsConnection {
  /// Sends a bounded multiple response request without automatic retry.
  Future<NatsRequestMany> requestMany(
    String subject,
    List<int> payload, {
    NatsHeaders? headers,
    NatsTraceOptions? trace,
    NatsRequestManyOptions? options,
  }) {
    _requireAcceptingWork();
    validatePublishSubject(
      subject,
      maximumBytes: _options.maximumOutboundControlLineBytes,
    );
    final effectiveHeaders = mergeTraceHeaders(headers, trace);
    _preflightPublication(payload.length, effectiveHeaders);
    final payloadSnapshot = checkedByteSnapshot(payload);
    return _requestMux.requestMany(
      subject: subject,
      payload: payloadSnapshot,
      options: options ?? NatsRequestManyOptions(),
      headers: effectiveHeaders,
      connectionEpoch: _activeEpoch,
      onOverflow: _statistics.recordSlowConsumer,
    );
  }

  /// Sends multiple response work only while [generation] remains active.
  Future<NatsRequestMany> requestManyOnGeneration(
    String subject,
    List<int> payload, {
    required int generation,
    NatsHeaders? headers,
    NatsTraceOptions? trace,
    NatsRequestManyOptions? options,
  }) {
    _requireAcceptingWork();
    if (generation != _generation) {
      throw NatsPublicationGenerationException(
        operation: "request",
        expectedGeneration: generation,
        actualGeneration: _generation,
      );
    }
    validatePublishSubject(
      subject,
      maximumBytes: _options.maximumOutboundControlLineBytes,
    );
    final effectiveHeaders = mergeTraceHeaders(headers, trace);
    _preflightPublication(payload.length, effectiveHeaders);
    final payloadSnapshot = checkedByteSnapshot(payload);
    return _requestMux.requestMany(
      subject: subject,
      payload: payloadSnapshot,
      options: options ?? NatsRequestManyOptions(),
      headers: effectiveHeaders,
      connectionEpoch: _activeEpoch,
      onOverflow: _statistics.recordSlowConsumer,
    );
  }
}
