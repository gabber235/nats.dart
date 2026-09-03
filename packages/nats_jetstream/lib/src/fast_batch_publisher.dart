import "dart:async";
import "dart:collection";
import "dart:convert";
import "dart:math";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/fast_batch.dart";
import "package:nats_jetstream/src/fast_batch_control.dart";
import "package:nats_jetstream/src/publish_headers.dart";

/// Lifecycle owner for one generation scoped fast ingest batch.
final class JetStreamFastBatchPublisher {
  /// Creates a publisher over one Core connection.
  const JetStreamFastBatchPublisher(this.connection, {this.expectedGeneration});

  /// Core connection used by the operation.
  final NatsConnection connection;

  /// Required generation, or `null` to capture the current generation.
  final int? expectedGeneration;

  /// Publishes an unlimited asynchronous message source with bounded inflight
  /// memory and server coordinated flow control.
  Future<JetStreamFastBatchAck> publish(
    Stream<JetStreamFastBatchMessage> messages, {
    JetStreamFastBatchOptions? options,
  }) async {
    final effective = options ?? JetStreamFastBatchOptions();
    final operation = _FastBatchOperation(
      connection,
      effective,
      effective.batchId ?? _randomToken(22),
      expectedGeneration ?? connection.generation,
    );
    return operation.run(messages);
  }
}

final class _FastBatchOperation {
  _FastBatchOperation(
    this.connection,
    this.options,
    this.batchId,
    this._generation,
  ) : _prefix = "_INBOX.${_randomToken(22)}";

  final NatsConnection connection;
  final JetStreamFastBatchOptions options;
  final String batchId;
  final int _generation;
  final String _prefix;
  final Queue<({BigInt sequence, int bytes})> _pending = Queue();
  final List<JetStreamFastBatchGap> _gaps = [];
  final List<JetStreamFastBatchMessageError> _errors = [];

  NatsSubscription? _subscription;
  _ControlQueue? _control;
  BigInt _sent = BigInt.zero;
  BigInt _acknowledged = BigInt.zero;
  var _flow = 1;
  var _pendingBytes = 0;
  String? _lastSubject;
  var _started = false;
  var _initialReplyReceived = false;
  var _committed = false;
  var _totalGaps = 0;
  var _totalErrors = 0;

  static final BigInt _maximumSequence = (BigInt.one << 64) - BigInt.two;

  Future<JetStreamFastBatchAck> run(
    Stream<JetStreamFastBatchMessage> messages,
  ) async {
    final input = StreamIterator(messages);
    try {
      await _openControlInbox();
      JetStreamFastBatchMessage? retained;
      while (await _moveNextKeepingAlive(input)) {
        if (options.commitStrategy ==
            JetStreamFastBatchCommitStrategy.endOfBatch) {
          await _add(input.current);
          continue;
        }
        final previous = retained;
        retained = input.current;
        if (previous != null) {
          await _add(previous);
        }
      }
      if (options.commitStrategy ==
          JetStreamFastBatchCommitStrategy.finalMessage) {
        if (retained == null) {
          throw ArgumentError.value(messages, "messages", "Must not be empty");
        }
        return await _commitMessage(retained);
      }
      if (!_started) {
        throw ArgumentError.value(messages, "messages", "Must not be empty");
      }
      return await _commitEndOfBatch();
    } on JetStreamFastBatchException {
      rethrow;
    } on Object catch (error, stackTrace) {
      final wrapped = _failure(
        "Fast batch did not complete",
        error,
        ambiguous: _sent > _acknowledged,
      );
      Error.throwWithStackTrace(wrapped, stackTrace);
    } finally {
      await input.cancel();
      await _closeControlInbox();
    }
  }

  Future<void> _openControlInbox() async {
    if (!connection.isConnected) {
      throw const NatsConnectionException("Connection is not ready");
    }
    final subscription = await connection.subscribe(
      "$_prefix.$batchId.>",
      options: NatsSubscriptionOptions(
        maximumMessages: 1024,
        maximumBytes: 1024 * 1024,
        reconnect: NatsSubscriptionReconnectPolicy.close,
      ),
    );
    _subscription = subscription;
    _control = _ControlQueue(subscription.messages);
    await connection.flush(timeout: options.controlTimeout);
    if (!connection.isConnected || connection.generation != _generation) {
      throw NatsPublicationGenerationException(
        operation: "fast batch control inbox",
        expectedGeneration: _generation,
        actualGeneration: connection.generation,
      );
    }
  }

  Future<bool> _moveNextKeepingAlive(
    StreamIterator<JetStreamFastBatchMessage> input,
  ) async {
    final pending = input.moveNext();
    if (!_started) {
      return pending;
    }
    while (true) {
      try {
        return await pending.timeout(options.controlTimeout);
      } on TimeoutException {
        await _pingAndAwaitProgress();
      }
    }
  }

  Future<void> _add(JetStreamFastBatchMessage message) async {
    final next = _sent + BigInt.one;
    if (next > _maximumSequence) {
      throw StateError("Fast batch sequence exhausted the uint64 range");
    }
    var headers = applyJetStreamPublishOptions(
      message.headers,
      message.options,
    );
    headers = setJetStreamManagedHeader(
      headers,
      "Nats-Required-Api-Level",
      "4",
    );
    final retainedBytes = message.payload.length + headers.encode().length;
    _validateMessageCapacity(retainedBytes);
    if (_started) {
      await _awaitCapacity(next, retainedBytes);
    }
    final operation = _started ? 1 : 0;
    await _publish(
      message.subject,
      message.payload,
      sequence: next,
      operation: operation,
      headers: headers,
    );
    _sent = next;
    _lastSubject = message.subject;
    _pending.add((sequence: next, bytes: retainedBytes));
    _pendingBytes += retainedBytes;
    if (!_started) {
      _started = true;
      await _awaitInitialAck();
    }
  }

  Future<void> _awaitCapacity(BigInt next, int bytes) async {
    while (next >
            _acknowledged +
                BigInt.from(_flow * options.maximumOutstandingAcks) ||
        _pendingBytes + bytes > options.maximumOutstandingBytes) {
      await _awaitFlowProgress();
    }
  }

  void _validateMessageCapacity(int retainedBytes) {
    if (retainedBytes <= options.maximumOutstandingBytes) {
      return;
    }
    throw _failure(
      "Fast batch message exceeds maximumOutstandingBytes",
      ArgumentError.value(
        retainedBytes,
        "message",
        "Retained bytes exceed maximumOutstandingBytes",
      ),
      ambiguous: _sent > _acknowledged,
    );
  }

  Future<void> _awaitInitialAck() async {
    while (!_initialReplyReceived) {
      FastBatchControl control;
      try {
        control = await _nextControl().timeout(options.controlTimeout);
      } on TimeoutException catch (error) {
        throw _failure(
          "Fast batch initial acknowledgement timed out",
          error,
          ambiguous: true,
        );
      } on JetStreamApiException catch (error) {
        throw _failure(
          "Fast batch server rejected the initial operation",
          error,
          ambiguous: false,
        );
      }
      await _applyControl(control, allowFinal: false, initialOperation: true);
      if (control is FastBatchFlowAck) {
        _initialReplyReceived = true;
      }
    }
  }

  Future<void> _awaitFlowProgress() async {
    final before = _acknowledged;
    while (_acknowledged <= before) {
      final control = await _nextControlWithPing();
      await _applyControl(control, allowFinal: false);
    }
  }

  Future<FastBatchControl> _nextControlWithPing() async {
    try {
      return await _nextControl().timeout(options.controlTimeout);
    } on TimeoutException {
      await _sendPing();
      try {
        return await _nextControl().timeout(options.controlTimeout);
      } on TimeoutException catch (error) {
        throw _failure(
          "Fast batch control channel stopped responding",
          error,
          ambiguous: _sent > _acknowledged,
        );
      }
    }
  }

  Future<void> _pingAndAwaitProgress() async {
    await _sendPing();
    try {
      while (true) {
        final control = await _nextControl().timeout(options.controlTimeout);
        if (control is FastBatchFlowAck) {
          await _applyControl(control, allowFinal: false);
          return;
        }
        await _applyControl(control, allowFinal: false);
      }
    } on TimeoutException catch (error) {
      throw _failure(
        "Fast batch keepalive was not acknowledged",
        error,
        ambiguous: _sent > _acknowledged,
      );
    }
  }

  Future<void> _sendPing() async {
    final subject = _lastSubject;
    if (subject == null || _committed) {
      return;
    }
    final headers = setJetStreamManagedHeader(
      null,
      "Nats-Required-Api-Level",
      "4",
    );
    await _publish(
      subject,
      const [],
      sequence: _sent,
      operation: 4,
      headers: headers,
    );
  }

  Future<JetStreamFastBatchAck> _commitEndOfBatch() async {
    final subject = _lastSubject!;
    final headers = setJetStreamManagedHeader(
      null,
      "Nats-Required-Api-Level",
      "4",
    );
    await _publish(
      subject,
      const [],
      sequence: _sent + BigInt.one,
      operation: 3,
      headers: headers,
    );
    _committed = true;
    return _awaitFinalAck();
  }

  Future<JetStreamFastBatchAck> _commitMessage(
    JetStreamFastBatchMessage message,
  ) async {
    final next = _sent + BigInt.one;
    var headers = applyJetStreamPublishOptions(
      message.headers,
      message.options,
    );
    headers = setJetStreamManagedHeader(
      headers,
      "Nats-Required-Api-Level",
      "4",
    );
    final retainedBytes = message.payload.length + headers.encode().length;
    _validateMessageCapacity(retainedBytes);
    if (_started) {
      await _awaitCapacity(next, retainedBytes);
    }
    await _publish(
      message.subject,
      message.payload,
      sequence: next,
      operation: 2,
      headers: headers,
    );
    _sent = next;
    _lastSubject = message.subject;
    _pending.add((sequence: next, bytes: retainedBytes));
    _pendingBytes += retainedBytes;
    _started = true;
    _committed = true;
    return _awaitFinalAck();
  }

  Future<JetStreamFastBatchAck> _awaitFinalAck() async {
    while (true) {
      final control = await _nextControl().timeout(
        options.controlTimeout,
        onTimeout: () => throw _failure(
          "Fast batch final acknowledgement timed out",
          null,
          ambiguous: true,
        ),
      );
      if (control is FastBatchFinalAck) {
        if (control.batchId != batchId || control.messageCount > _sent) {
          throw const NatsProtocolException(
            "Fast batch final acknowledgement did not match the operation",
          );
        }
        return JetStreamFastBatchAck(
          stream: control.stream,
          sequence: control.sequence,
          batchId: control.batchId,
          messageCount: control.messageCount,
          acknowledgedBatchSequence: _acknowledged,
          gaps: _gaps,
          errors: _errors,
          totalGaps: _totalGaps,
          totalErrors: _totalErrors,
          domain: control.domain,
        );
      }
      await _applyControl(control, allowFinal: true);
    }
  }

  Future<FastBatchControl> _nextControl() async {
    final queue = _control;
    if (queue == null) {
      throw const NatsConnectionException("Fast batch control inbox closed");
    }
    final message = await queue.next();
    return decodeFastBatchControl(message.payload, message.subject);
  }

  Future<void> _applyControl(
    FastBatchControl control, {
    required bool allowFinal,
    bool initialOperation = false,
  }) async {
    switch (control) {
      case FastBatchFlowAck(:final sequence, :final messages):
        if (sequence > _sent) {
          throw const NatsProtocolException(
            "Fast batch flow ack exceeds the sent sequence",
          );
        }
        if (messages > options.initialFlow) {
          throw const NatsProtocolException(
            "Fast batch flow ack exceeds the requested maximum",
          );
        }
        _flow = messages;
        if (sequence > _acknowledged) {
          _acknowledged = sequence;
          _releasePending();
        }
      case FastBatchGap(:final value):
        if (value.receivedSequence > _sent) {
          throw const NatsProtocolException(
            "Fast batch gap exceeds the sent sequence",
          );
        }
        _totalGaps += 1;
        if (_retainedDiagnosticCount < options.maximumDiagnostics) {
          _gaps.add(value);
        }
        if (options.gapPolicy == JetStreamFastBatchGapPolicy.fail) {
          throw _failure(
            "Fast batch server reported a message gap",
            value,
            ambiguous: _sent > _acknowledged,
          );
        }
      case FastBatchError(:final value):
        if (value.batchSequence > _sent) {
          throw const NatsProtocolException(
            "Fast batch error exceeds the sent sequence",
          );
        }
        _totalErrors += 1;
        if (_retainedDiagnosticCount < options.maximumDiagnostics) {
          _errors.add(value);
        }
        if (initialOperation ||
            options.gapPolicy == JetStreamFastBatchGapPolicy.fail) {
          throw _failure(
            "Fast batch server rejected a message",
            value.error,
            ambiguous: !initialOperation && _sent > _acknowledged,
          );
        }
      case FastBatchFinalAck():
        if (!allowFinal) {
          throw const NatsProtocolException(
            "Fast batch ended before the client committed it",
          );
        }
    }
  }

  int get _retainedDiagnosticCount => _gaps.length + _errors.length;

  void _releasePending() {
    while (_pending.isNotEmpty && _pending.first.sequence <= _acknowledged) {
      _pendingBytes -= _pending.removeFirst().bytes;
    }
  }

  Future<void> _publish(
    String subject,
    List<int> payload, {
    required BigInt sequence,
    required int operation,
    required NatsHeaders headers,
  }) {
    final gap = options.gapPolicy == JetStreamFastBatchGapPolicy.allow
        ? "ok"
        : "fail";
    final reply =
        "$_prefix.$batchId.${options.initialFlow}.$gap.$sequence."
        "$operation.\$FI";
    return connection.publishOnGeneration(
      subject,
      payload,
      generation: _generation,
      replySubject: reply,
      headers: headers,
    );
  }

  JetStreamFastBatchException _failure(
    String message,
    Object? cause, {
    required bool ambiguous,
  }) {
    return JetStreamFastBatchException(
      message: message,
      batchId: batchId,
      sentSequence: _sent,
      acknowledgedSequence: _acknowledged,
      ambiguous: ambiguous,
      cause: cause,
    );
  }

  Future<void> _closeControlInbox() async {
    await _control?.close();
    await _subscription?.unsubscribe();
  }
}

final class _ControlQueue {
  _ControlQueue(Stream<NatsMessage> messages) {
    _subscription = messages.listen(
      _onMessage,
      onError: _onError,
      onDone: _onDone,
      cancelOnError: false,
    );
  }

  final Queue<NatsMessage> _messages = Queue();
  late final StreamSubscription<NatsMessage> _subscription;
  Completer<void>? _changed;
  Object? _error;
  StackTrace? _stackTrace;
  var _bytes = 0;
  var _closed = false;

  Future<NatsMessage> next() async {
    while (_messages.isEmpty) {
      final error = _error;
      if (error != null) {
        Error.throwWithStackTrace(error, _stackTrace ?? StackTrace.current);
      }
      if (_closed) {
        throw const NatsConnectionException("Fast batch control inbox closed");
      }
      final changed = _changed ??= Completer<void>();
      await changed.future;
      if (identical(_changed, changed)) {
        _changed = null;
      }
    }
    final message = _messages.removeFirst();
    _bytes -= _retainedBytes(message);
    return message;
  }

  void _onMessage(NatsMessage message) {
    if (_closed) {
      return;
    }
    final retained = _retainedBytes(message);
    if (_messages.length == 1024 || _bytes + retained > 1024 * 1024) {
      _onError(
        const NatsSlowConsumerException(
          "Fast batch control queue exceeded its local bound",
        ),
        StackTrace.current,
      );
      return;
    }
    _messages.add(message);
    _bytes += retained;
    _signal();
  }

  void _onError(Object error, [StackTrace? stackTrace]) {
    if (_closed || _error != null) {
      return;
    }
    _error = error;
    _stackTrace = stackTrace;
    _messages.clear();
    _bytes = 0;
    _signal();
  }

  void _onDone() {
    _closed = true;
    _signal();
  }

  void _signal() {
    final changed = _changed;
    if (changed != null && !changed.isCompleted) {
      changed.complete();
    }
  }

  int _retainedBytes(NatsMessage message) {
    return message.payload.length +
        utf8.encode(message.subject).length +
        utf8.encode(message.replySubject ?? "").length +
        (message.headers?.encode().length ?? 0);
  }

  Future<void> close() async {
    if (!_closed) {
      _closed = true;
      _signal();
    }
    await _subscription.cancel();
    _messages.clear();
    _bytes = 0;
  }
}

String _randomToken(int length) {
  const alphabet =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  final random = Random.secure();
  return String.fromCharCodes(
    List.generate(
      length,
      (_) => alphabet.codeUnitAt(random.nextInt(alphabet.length)),
    ),
  );
}
