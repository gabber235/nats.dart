part of "async_publish.dart";

const _duplicateMessageInProcessErrorCode = 10158;

enum _AsyncPublisherLifecycle { open, draining, closed }

final class _AsyncPublishRecord {
  _AsyncPublishRecord({
    required this.subject,
    required this.payload,
    required this.headers,
    required this.expectedStream,
    required this.messageId,
    required this.retainedBytes,
    required this.deadlineMicroseconds,
  });

  final String subject;
  final Uint8List payload;
  final NatsHeaders? headers;
  final String? expectedStream;
  final String? messageId;
  final int retainedBytes;
  final int deadlineMicroseconds;
  final completer = Completer<JetStreamPublishAck>();
  var state = JetStreamAsyncPublishState.accepted;
  var attempts = 0;
  var finished = false;
}

/// Bounded owner for concurrent acknowledged publications.
final class JetStreamAsyncPublisher {
  /// Creates a publisher over one reconnecting Core connection.
  factory JetStreamAsyncPublisher(
    NatsConnection connection,
    JetStreamAsyncPublisherOptions options,
  ) {
    return JetStreamAsyncPublisher._(connection, options);
  }

  JetStreamAsyncPublisher._(this.connection, this.options) {
    _clock.start();
    _events = connection.events.listen(_onConnectionEvent);
    _done.future.ignore();
  }

  /// Core connection that owns transport and request correlation.
  final NatsConnection connection;

  /// Immutable capacity and retry bounds.
  final JetStreamAsyncPublisherOptions options;

  final Stopwatch _clock = Stopwatch();
  final Set<_AsyncPublishRecord> _pending = {};
  final Set<Completer<int>> _connectedWaiters = {};
  final Completer<void> _done = Completer<void>();
  late final StreamSubscription<NatsConnectionEvent> _events;
  _AsyncPublisherLifecycle _lifecycle = _AsyncPublisherLifecycle.open;
  var _pendingBytes = 0;
  var _acknowledged = 0;
  var _definitelyRejected = 0;
  var _ambiguous = 0;
  Object? _firstFailure;
  var _finishingDrain = false;

  /// Current admitted publication count.
  int get pendingMessages => _pending.length;

  /// Current retained byte count.
  int get pendingBytes => _pendingBytes;

  /// Completes when drain closes this owner.
  Future<void> get done => _done.future;

  /// Admits one publication synchronously and starts acknowledgement work.
  JetStreamPendingPublish publish(
    String subject,
    List<int> payload, {
    NatsHeaders? headers,
    JetStreamPublishOptions? options,
  }) {
    if (_lifecycle != _AsyncPublisherLifecycle.open) {
      throw const JetStreamAsyncPublisherClosedException();
    }
    if (!connection.isConnected || connection.isDraining) {
      throw const NatsConnectionException("Connection is not ready");
    }
    validatePublishSubject(subject);
    if (headers?.first("Nats-Incr") != null) {
      throw ArgumentError("Nats-Incr is not supported by this publisher");
    }
    final effectiveHeaders = applyJetStreamPublishOptions(headers, options);
    final snapshot = checkedJetStreamByteSnapshot(payload);
    final retainedBytes =
        utf8.encode(subject).length +
        snapshot.length +
        (effectiveHeaders?.encode().length ?? 0);
    if (_pending.length >= this.options.maximumPendingMessages ||
        retainedBytes > this.options.maximumPendingBytes - _pendingBytes) {
      throw JetStreamAsyncPublishCapacityException(
        pendingMessages: _pending.length,
        pendingBytes: _pendingBytes,
        requestedBytes: retainedBytes,
        maximumMessages: this.options.maximumPendingMessages,
        maximumBytes: this.options.maximumPendingBytes,
      );
    }
    final record = _AsyncPublishRecord(
      subject: subject,
      payload: snapshot,
      headers: effectiveHeaders,
      expectedStream: effectiveHeaders?.first("Nats-Expected-Stream"),
      messageId: effectiveHeaders?.first("Nats-Msg-Id"),
      retainedBytes: retainedBytes,
      deadlineMicroseconds:
          _clock.elapsedMicroseconds +
          this.options.acknowledgementTimeout.inMicroseconds,
    );
    record.completer.future.ignore();
    _pending.add(record);
    _pendingBytes += retainedBytes;
    unawaited(_run(record));
    return JetStreamPendingPublish._(record);
  }

  /// Stops admission and waits for every admitted publication.
  Future<void> drain() {
    if (_lifecycle == _AsyncPublisherLifecycle.open) {
      _lifecycle = _AsyncPublisherLifecycle.draining;
      _completeDrainIfReady();
    }
    return _done.future;
  }

  /// Idempotently drains and closes this owner.
  Future<void> close() => drain();

  Future<void> _run(_AsyncPublishRecord record) async {
    Object lastCause = NatsTimeoutException("JetStream publish");
    var disposition = JetStreamPublishDisposition.possiblyStored;
    try {
      while (record.attempts < options.maximumAttempts) {
        if (_remaining(record) <= Duration.zero) break;
        record
          ..state = JetStreamAsyncPublishState.attempting
          ..attempts += 1;
        try {
          final generation = connection.isConnected
              ? connection.generation
              : await _awaitConnected(record);
          final message = await connection.requestOnGeneration(
            record.subject,
            record.payload,
            generation: generation,
            headers: record.headers,
            timeout: _attemptTimeout(record),
          );
          final response = JetStreamWireResponse.decode(
            message.payload,
            record.subject,
          )..throwApiError();
          _succeed(
            record,
            _decodeAck(response, expectedStream: record.expectedStream),
          );
          return;
        } on Object catch (error) {
          lastCause = error;
          disposition = _disposition(error);
          if (!_canRetry(record, error, disposition)) break;
          record.state = JetStreamAsyncPublishState.waitingToRetry;
          await _waitBeforeRetry(record);
        }
      }
    } on Object catch (error) {
      lastCause = error;
      disposition = _disposition(error);
    }
    _fail(
      record,
      JetStreamAsyncPublishException(
        cause: lastCause,
        disposition: disposition,
        attempts: record.attempts,
        messageId: record.messageId,
      ),
    );
  }

  bool _canRetry(
    _AsyncPublishRecord record,
    Object error,
    JetStreamPublishDisposition disposition,
  ) {
    if (record.attempts >= options.maximumAttempts ||
        _remaining(record) <= Duration.zero) {
      return false;
    }
    if (error is NatsNoRespondersException) {
      return options.retryPolicy != JetStreamAsyncPublishRetryPolicy.none;
    }
    return disposition == JetStreamPublishDisposition.possiblyStored &&
        options.retryPolicy == JetStreamAsyncPublishRetryPolicy.deduplicated &&
        record.messageId != null;
  }

  JetStreamPublishDisposition _disposition(Object error) {
    if (error is NatsPublicationPreflightException) {
      return JetStreamPublishDisposition.definitelyNotStored;
    }
    if (error is JetStreamApiException) {
      return error.errorCode == _duplicateMessageInProcessErrorCode
          ? JetStreamPublishDisposition.possiblyStored
          : JetStreamPublishDisposition.definitelyNotStored;
    }
    if (error is NatsNoRespondersException) {
      return JetStreamPublishDisposition.definitelyNotStored;
    }
    return JetStreamPublishDisposition.possiblyStored;
  }

  Future<void> _waitBeforeRetry(_AsyncPublishRecord record) async {
    final remaining = _remaining(record);
    if (remaining <= Duration.zero) return;
    final delay = options.retryDelay < remaining
        ? options.retryDelay
        : remaining;
    if (delay > Duration.zero) await Future<void>.delayed(delay);
    if (!connection.isConnected && _remaining(record) > Duration.zero) {
      await _awaitConnected(record);
    }
  }

  Future<int> _awaitConnected(_AsyncPublishRecord record) async {
    if (connection.isConnected) return connection.generation;
    final remaining = _remaining(record);
    if (remaining <= Duration.zero) {
      throw NatsTimeoutException("JetStream publish reconnect");
    }
    final completer = Completer<int>();
    _connectedWaiters.add(completer);
    if (connection.isConnected && !completer.isCompleted) {
      completer.complete(connection.generation);
    }
    final timer = Timer(remaining, () {
      if (!completer.isCompleted) {
        completer.completeError(
          NatsTimeoutException("JetStream publish reconnect"),
        );
      }
    });
    final closed = connection.closed.then<int>(
      (_) => throw const NatsClosedException(),
    );
    try {
      return await Future.any([completer.future, closed]);
    } finally {
      timer.cancel();
      _connectedWaiters.remove(completer);
    }
  }

  Duration _remaining(_AsyncPublishRecord record) {
    final microseconds =
        record.deadlineMicroseconds - _clock.elapsedMicroseconds;
    return microseconds <= 0
        ? Duration.zero
        : Duration(microseconds: microseconds);
  }

  Duration _attemptTimeout(_AsyncPublishRecord record) {
    final remaining = _remaining(record);
    return options.attemptTimeout < remaining
        ? options.attemptTimeout
        : remaining;
  }

  void _onConnectionEvent(NatsConnectionEvent event) {
    if (event is NatsConnected) {
      for (final waiter in _connectedWaiters.toList(growable: false)) {
        if (!waiter.isCompleted) waiter.complete(event.generation);
      }
    } else if (event is NatsClosed) {
      for (final waiter in _connectedWaiters.toList(growable: false)) {
        if (!waiter.isCompleted) {
          waiter.completeError(event.error ?? const NatsClosedException());
        }
      }
    }
  }

  void _succeed(_AsyncPublishRecord record, JetStreamPublishAck ack) {
    if (!_release(record)) return;
    record.state = JetStreamAsyncPublishState.acknowledged;
    _acknowledged += 1;
    record.completer.complete(ack);
    _completeDrainIfReady();
  }

  void _fail(_AsyncPublishRecord record, JetStreamAsyncPublishException error) {
    if (!_release(record)) return;
    record.state = JetStreamAsyncPublishState.failed;
    _firstFailure ??= error.cause;
    if (error.disposition == JetStreamPublishDisposition.definitelyNotStored) {
      _definitelyRejected += 1;
    } else {
      _ambiguous += 1;
    }
    record.completer.completeError(error);
    _completeDrainIfReady();
  }

  bool _release(_AsyncPublishRecord record) {
    if (record.finished) return false;
    record.finished = true;
    _pending.remove(record);
    _pendingBytes -= record.retainedBytes;
    return true;
  }

  void _completeDrainIfReady() {
    if (_lifecycle != _AsyncPublisherLifecycle.draining ||
        _pending.isNotEmpty ||
        _finishingDrain) {
      return;
    }
    _finishingDrain = true;
    unawaited(_finishDrain());
  }

  Future<void> _finishDrain() async {
    await _events.cancel();
    _lifecycle = _AsyncPublisherLifecycle.closed;
    if (_firstFailure case final failure?) {
      _done.completeError(
        JetStreamAsyncPublisherDrainException(
          acknowledged: _acknowledged,
          definitelyRejected: _definitelyRejected,
          ambiguous: _ambiguous,
          firstCause: failure,
        ),
      );
    } else {
      _done.complete();
    }
  }
}

JetStreamPublishAck _decodeAck(
  JetStreamWireResponse response, {
  required String? expectedStream,
}) {
  final stream = response.requiredString("stream");
  if (expectedStream != null && stream != expectedStream) {
    throw JetStreamProtocolException(
      "Publish acknowledgement has the wrong stream identity",
      subject: response.subject,
      fieldPath: "stream",
    );
  }
  return JetStreamPublishAck(
    stream: stream,
    sequence: response.requiredPositiveUint64("seq"),
    duplicate: response.optionalBoolean("duplicate"),
    domain: response.optionalString("domain"),
  );
}
