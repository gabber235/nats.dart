import "dart:async";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/consumer_config.dart";
import "package:nats_jetstream/src/consumer_consume_options.dart";
import "package:nats_jetstream/src/consumer_delivery.dart";
import "package:nats_jetstream/src/consumer_duration.dart";
import "package:nats_jetstream/src/consumer_runtime.dart";
import "package:nats_jetstream/src/continuous_delivery_capacity.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/lifecycle_signal.dart";

/// Generation fenced implementation of one continuous pull operation.
final class JetStreamConsumerStreamSession implements JetStreamConsumerStream {
  /// Creates a lazy delivery session.
  JetStreamConsumerStreamSession(
    this.connection,
    this.apiPrefix,
    this.stream,
    this.consumer,
    this.options, {
    this.propagateTerminalErrors = true,
  }) {
    _pinId = options.priority?.pinId;
    _controller = StreamController<JetStreamDelivery>(
      sync: true,
      onListen: _start,
      onPause: _pause,
      onResume: _resume,
      onCancel: _cancelWithoutTerminalError,
    );
    messages = _controller.stream;
    _done.future.ignore();
  }

  /// Core connection used for pull requests and acknowledgements.
  final NatsConnection connection;

  /// Domain aware JetStream API prefix.
  final String apiPrefix;

  /// Owning stream name.
  final String stream;

  /// Existing consumer name.
  final String consumer;

  /// Batching, heartbeat, priority, and queue bounds.
  final JetStreamConsumeOptions options;

  /// Whether [messages] and cancellation propagate terminal failures.
  final bool propagateTerminalErrors;

  final Completer<void> _done = Completer<void>();
  final JetStreamLifecycleSignal _lifecycle = JetStreamLifecycleSignal();
  late final StreamController<JetStreamDelivery> _controller;
  NatsSubscription? _subscription;
  StreamIterator<NatsMessage>? _iterator;
  Object? _terminalError;
  StackTrace? _terminalStackTrace;
  var _started = false;
  var _stopping = false;
  var _draining = false;
  var _paused = false;
  var _inputFinished = false;
  var _streamFinished = false;
  Completer<void>? _pullResume;
  String? _pinId;

  @override
  late final Stream<JetStreamDelivery> messages;

  @override
  Future<void> get done => _done.future;

  @override
  Future<void> stop() async {
    if (_stopping) {
      return done;
    }
    _stopping = true;
    _draining = false;
    _lifecycle.cancel();
    _releaseDeferredPull();
    await _iterator?.cancel();
    await _subscription?.unsubscribe();
    if (!_started) {
      _inputFinished = true;
      _completeIfReady();
    }
    return done;
  }

  @override
  Future<void> drain({Duration timeout = const Duration(seconds: 30)}) async {
    if (_stopping) {
      return done;
    }
    if (timeout <= Duration.zero) {
      throw ArgumentError.value(timeout, "timeout", "Must be positive");
    }
    _draining = true;
    _lifecycle.cancel();
    _releaseDeferredPull();
    final active = _subscription;
    if (active != null) {
      await active.drain(timeout: timeout);
    } else if (!_started) {
      _inputFinished = true;
      _completeIfReady();
    }
    return done.timeout(
      timeout,
      onTimeout: () => throw NatsTimeoutException("JetStream consume drain"),
    );
  }

  Future<void> _cancelWithoutTerminalError() async {
    try {
      await stop();
    } on Object {
      // The stream error and done future own the exact terminal failure.
    }
  }

  void _start() {
    if (_started) {
      return;
    }
    _started = true;
    unawaited(_run());
  }

  void _pause() => _paused = true;

  void _resume() {
    _paused = false;
    _releaseDeferredPull();
    if (_terminalError != null) {
      _finishTerminalStream();
      return;
    }
    _completeIfReady();
  }

  Future<void> _run() async {
    try {
      while (!_stopping) {
        if (!await _lifecycle.waitUntilConnected(connection)) {
          break;
        }
        if (_stopping || _draining) {
          break;
        }
        try {
          await _runGeneration();
        } on NatsConnectionException {
          if (_stopping || _draining) {
            break;
          }
          continue;
        }
      }
    } on Object catch (error, stackTrace) {
      _terminalError = error;
      _terminalStackTrace = stackTrace;
    } finally {
      _inputFinished = true;
      _completeIfReady();
    }
  }

  Future<void> _runGeneration() async {
    final generation = connection.generation;
    final ackPolicy = await readPullConsumerAckPolicy(
      connection,
      apiPrefix,
      stream,
      consumer,
      options.expires,
    );
    _requireGeneration(generation);
    final inbox = connection.newInbox();
    final subscription = await connection.subscribe(
      inbox,
      options: NatsSubscriptionOptions(
        maximumMessages: continuousDeliveryCoreMaximumMessages(
          options.maximumPendingMessages,
        ),
        maximumBytes: continuousDeliveryCoreMaximumBytes(
          options.maximumPendingBytes,
        ),
        reconnect: NatsSubscriptionReconnectPolicy.close,
      ),
    );
    _subscription = subscription;
    final iterator = StreamIterator(subscription.messages);
    _iterator = iterator;
    final connectionEvents = connection.events.listen((event) {
      if ((event is NatsDisconnected && event.generation == generation) ||
          event is NatsClosed) {
        _releaseDeferredPull();
      }
    });
    try {
      await connection.flush(timeout: options.expires);
      _requireGeneration(generation);
      await _renewPull(inbox, generation, subscription);
      var received = 0;
      while (!_stopping) {
        await _waitWhilePaused(generation, subscription);
        if (_stopping) {
          break;
        }
        final available = await iterator.moveNext().timeout(
          options.idleHeartbeat * 2,
          onTimeout: () =>
              throw JetStreamConsumerHeartbeatException(options.idleHeartbeat),
        );
        if (!available) {
          throw const NatsConnectionException(
            "Continuous pull delivery inbox closed",
          );
        }
        final message = iterator.current;
        if (message.headers?.statusCode case final status?) {
          if (status == 100) {
            if (message.replySubject case final reply?) {
              await connection.publishOnGeneration(
                reply,
                const [],
                generation: generation,
              );
            }
            continue;
          }
          if (_normalCompletion(message.headers!)) {
            received = 0;
            if (!_stopping && !_draining) {
              await _renewPull(inbox, generation, subscription);
            }
            continue;
          }
          throw JetStreamConsumerStatusException(
            code: status,
            description: message.headers?.statusDescription ?? "",
            headers: message.headers!,
          );
        }
        final delivery = requireConsumerDeliveryIdentity(
          ackPolicy != JetStreamConsumerAckPolicy.none
              ? JetStreamDelivery.fromMessage(connection, generation, message)
              : JetStreamDelivery.withoutAcknowledgement(message),
          expectedStream: stream,
          expectedConsumer: consumer,
          operation: "Continuous pull",
        );
        final messagePinId = message.headers?.first("Nats-Pin-Id");
        if (messagePinId != null) {
          if (_pinId != null && _pinId != messagePinId) {
            throw JetStreamProtocolException(
              "Pinned client ID changed during continuous delivery",
              subject: "$apiPrefix.CONSUMER.MSG.NEXT.$stream.$consumer",
              fieldPath: "Nats-Pin-Id",
            );
          }
          _pinId = messagePinId;
        }
        _emit(delivery);
        received += 1;
        if (received == options.batchSize && !_stopping && !_draining) {
          received = 0;
          await _renewPull(inbox, generation, subscription);
        }
      }
    } finally {
      await connectionEvents.cancel();
      await iterator.cancel();
      await subscription.unsubscribe();
      if (identical(_iterator, iterator)) {
        _iterator = null;
      }
      if (identical(_subscription, subscription)) {
        _subscription = null;
      }
    }
  }

  Future<void> _publishPull(String inbox, int generation) {
    final priority = options.priority;
    return connection.publishOnGeneration(
      "$apiPrefix.CONSUMER.MSG.NEXT.$stream.$consumer",
      encodeExactJson({
        "batch": options.batchSize,
        "max_bytes": options.maximumPendingBytes,
        "expires": consumerDurationNanoseconds(options.expires, "expires"),
        "idle_heartbeat": consumerDurationNanoseconds(
          options.idleHeartbeat,
          "idleHeartbeat",
        ),
        if (priority != null) ...{
          "group": priority.group,
          "min_pending": ?priority.minimumPending,
          "min_ack_pending": ?priority.minimumAcknowledgementsPending,
          "id": ?_pinId,
          "priority": ?priority.priority,
        },
      }),
      generation: generation,
      replySubject: inbox,
    );
  }

  Future<void> _renewPull(
    String inbox,
    int generation,
    NatsSubscription subscription,
  ) async {
    await _waitWhilePaused(generation, subscription);
    if (_stopping || _draining) {
      return;
    }
    _requireGeneration(generation);
    await _publishPull(inbox, generation);
  }

  Future<void> _waitWhilePaused(
    int generation,
    NatsSubscription subscription,
  ) async {
    while (_paused && !_stopping && !_draining) {
      _requireGeneration(generation);
      final resumed = _pullResume ??= Completer<void>();
      await _waitForPullSignal(resumed, subscription);
    }
  }

  Future<void> _waitForPullSignal(
    Completer<void> resumed,
    NatsSubscription subscription,
  ) {
    final signaled = Completer<void>();
    void signal() {
      if (!signaled.isCompleted) {
        signaled.complete();
      }
    }

    void fail(Object error, StackTrace stackTrace) {
      if (!signaled.isCompleted) {
        signaled.completeError(error, stackTrace);
      }
    }

    resumed.future.then<void>((_) => signal(), onError: fail).ignore();
    subscription.done.then<void>((_) => signal(), onError: fail).ignore();
    connection.closed.then<void>((_) => signal(), onError: fail).ignore();
    return signaled.future;
  }

  void _releaseDeferredPull() {
    final resumed = _pullResume;
    _pullResume = null;
    if (resumed != null && !resumed.isCompleted) {
      resumed.complete();
    }
  }

  void _requireGeneration(int generation) {
    if (!connection.isConnected || connection.generation != generation) {
      throw const NatsConnectionException(
        "Connection generation changed before continuous pull",
      );
    }
  }

  void _emit(JetStreamDelivery delivery) {
    if (_paused && !_draining) {
      throw StateError("Continuous pull advanced while delivery was paused");
    }
    _controller.add(delivery);
    if (_controller.isPaused) {
      _paused = true;
    }
  }

  void _completeIfReady() {
    if (!_inputFinished) {
      return;
    }
    final error = _terminalError;
    if (error != null) {
      _finishTerminalStream();
      if (!_done.isCompleted) {
        _done.completeError(error, _terminalStackTrace);
      }
      return;
    }
    if (_streamFinished) {
      return;
    }
    _finishStream();
    if (!_done.isCompleted) {
      _done.complete();
    }
  }

  void _finishTerminalStream() {
    if (_streamFinished) {
      return;
    }
    if (propagateTerminalErrors) {
      _controller.addError(_terminalError!, _terminalStackTrace);
    }
    _finishStream();
  }

  void _finishStream() {
    if (_streamFinished) {
      return;
    }
    _streamFinished = true;
    _controller.close().ignore();
  }
}

bool _normalCompletion(NatsHeaders headers) => switch (headers.statusCode) {
  404 || 408 => true,
  409
      when (headers.statusDescription ?? "").toLowerCase().contains(
        "batch completed",
      ) =>
    true,
  _ => false,
};
