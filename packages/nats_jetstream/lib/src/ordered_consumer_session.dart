import "dart:async";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/consumer_config.dart";
import "package:nats_jetstream/src/consumer_consume_options.dart";
import "package:nats_jetstream/src/consumer_delivery.dart";
import "package:nats_jetstream/src/diagnostic_events.dart";
import "package:nats_jetstream/src/ordered_consumer_cursor.dart";
import "package:nats_jetstream/src/ordered_consumer_generation.dart";
import "package:nats_jetstream/src/ordered_consumer_models.dart";
import "package:nats_jetstream/src/ordered_consumer_owner.dart";
import "package:nats_jetstream/src/ordered_consumer_recovery.dart";

/// Client managed ordered delivery over replaceable ephemeral pull consumers.
final class JetStreamOrderedConsumerSession
    implements JetStreamOrderedConsumerStream {
  /// Creates one lazy ordered delivery operation.
  JetStreamOrderedConsumerSession({
    required NatsConnection connection,
    required String apiPrefix,
    required Duration timeout,
    required String stream,
    required this.orderedOptions,
    required this.consumeOptions,
  }) : _owner = JetStreamOrderedConsumerOwner(
         connection: connection,
         apiPrefix: apiPrefix,
         timeout: timeout,
         stream: stream,
         options: orderedOptions,
       ),
       _stream = stream {
    _messages = StreamController<JetStreamDelivery>(
      sync: true,
      onListen: _start,
      onPause: _pause,
      onResume: _resume,
      onCancel: _cancelWithoutTerminalError,
    );
    _events = JetStreamDiagnosticEvents<JetStreamOrderedConsumerEvent>();
    messages = _messages.stream;
    events = _events.stream;
    _done.future.ignore();
  }

  final String _stream;
  final JetStreamOrderedConsumerOwner _owner;

  /// Ordered consumer creation and recovery policy.
  final JetStreamOrderedConsumerOptions orderedOptions;

  /// Pull batching, heartbeat, and queue bounds.
  final JetStreamConsumeOptions consumeOptions;

  final JetStreamOrderedConsumerCursor _cursor =
      JetStreamOrderedConsumerCursor();
  final Completer<void> _done = Completer<void>();
  late final StreamController<JetStreamDelivery> _messages;
  late final JetStreamDiagnosticEvents<JetStreamOrderedConsumerEvent> _events;
  JetStreamOrderedConsumerGenerationDelivery? _activeGeneration;
  var _started = false;
  var _stopping = false;
  var _draining = false;
  var _paused = false;

  @override
  late final Stream<JetStreamDelivery> messages;

  @override
  late final Stream<JetStreamOrderedConsumerEvent> events;

  @override
  Future<void> get done => _done.future;

  @override
  Future<void> stop() async {
    if (_stopping) {
      return done;
    }
    _stopping = true;
    _draining = false;
    _owner.cancelWaits();
    await _activeGeneration?.stop();
    if (!_started) {
      await _owner.close();
      _complete();
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
    _owner.cancelWaits();
    final active = _activeGeneration;
    if (active != null) {
      await active.drain(timeout);
    } else if (!_started) {
      await _owner.close();
      _complete();
    }
    return done.timeout(
      timeout,
      onTimeout: () => throw NatsTimeoutException("Ordered consumer drain"),
    );
  }

  Future<void> _cancelWithoutTerminalError() async {
    try {
      await stop();
    } on Object {
      // The public done future owns the exact terminal failure.
    }
  }

  void _start() {
    if (_started) {
      return;
    }
    _started = true;
    unawaited(_run());
  }

  void _pause() {
    _paused = true;
    _activeGeneration?.pause();
  }

  void _resume() {
    _paused = false;
    _activeGeneration?.resume();
  }

  Future<void> _run() async {
    Object? terminalError;
    StackTrace? terminalStackTrace;
    String? previousConsumer;
    JetStreamOrderedConsumerRecreationReason? recreationReason;
    try {
      while (!_stopping && !_draining) {
        final lastStreamSequence = _cursor.lastStreamSequence;
        final start = lastStreamSequence == null
            ? orderedOptions.start
            : JetStreamConsumerStart.sequence(lastStreamSequence + BigInt.one);
        final opened = await _openWithRecovery(start);
        if (_stopping || _draining) {
          await _owner.closeGeneration(opened);
          break;
        }
        _cursor.beginConsumer(start.sequence);
        _emitOpened(previousConsumer, recreationReason, opened, start);
        final generation = JetStreamOrderedConsumerGenerationDelivery(
          connection: _owner.connection,
          stream: _stream,
          opened: opened,
          cursor: _cursor,
          options: consumeOptions,
          onDelivery: _messages.add,
          isDeliveryPaused: () => _messages.isPaused,
          onEvent: _emitEvent,
        );
        _activeGeneration = generation;
        if (_paused) {
          generation.pause();
        }
        final outcome = await generation.run();
        _activeGeneration = null;
        await _owner.closeGeneration(opened);
        previousConsumer = opened.consumer;
        if (outcome.error case final error?) {
          Error.throwWithStackTrace(error, outcome.stackTrace!);
        }
        recreationReason = outcome.reason;
      }
    } on Object catch (error, stackTrace) {
      terminalError = error;
      terminalStackTrace = stackTrace;
    } finally {
      _stopping = true;
      await _activeGeneration?.stop();
      try {
        await _owner.close();
      } on Object catch (error, stackTrace) {
        terminalError ??= error;
        terminalStackTrace ??= stackTrace;
      }
      _complete(terminalError, terminalStackTrace);
    }
  }

  void _emitOpened(
    String? previousConsumer,
    JetStreamOrderedConsumerRecreationReason? reason,
    JetStreamOwnedOrderedConsumer opened,
    JetStreamConsumerStart start,
  ) {
    if (previousConsumer == null) {
      _emitEvent(
        JetStreamOrderedConsumerCreated(
          consumer: opened.consumer,
          generation: opened.generation,
        ),
      );
      return;
    }
    _emitEvent(
      JetStreamOrderedConsumerRecreated(
        previousConsumer: previousConsumer,
        consumer: opened.consumer,
        reason: reason!,
        nextStreamSequence: start.sequence,
        generation: opened.generation,
      ),
    );
  }

  Future<JetStreamOwnedOrderedConsumer> _openWithRecovery(
    JetStreamConsumerStart start,
  ) async {
    final deadline = DateTime.timestamp().add(orderedOptions.recoveryTimeout);
    Object? lastFailure;
    while (!_stopping && !_draining) {
      final remaining = deadline.difference(DateTime.timestamp());
      if (remaining <= Duration.zero) {
        throw JetStreamOrderedConsumerRecoveryException(
          timeout: orderedOptions.recoveryTimeout,
          cause: lastFailure ?? const NatsConnectionException("Not connected"),
        );
      }
      final connected = await _owner.waitUntilConnected(maximumWait: remaining);
      if (!connected) {
        continue;
      }
      await _owner.deleteStaleConsumers();
      try {
        return await _owner.open(start, requestTimeout: remaining);
      } on Object catch (error) {
        if (!isRecoverableOrderedConsumerOpenFailure(error)) {
          rethrow;
        }
        lastFailure = error;
        if (deadline.difference(DateTime.timestamp()) <= Duration.zero) {
          throw JetStreamOrderedConsumerRecoveryException(
            timeout: orderedOptions.recoveryTimeout,
            cause: lastFailure,
          );
        }
      }
      final afterAttempt = deadline.difference(DateTime.timestamp());
      if (afterAttempt > Duration.zero) {
        await Future<void>.delayed(
          afterAttempt < const Duration(milliseconds: 100)
              ? afterAttempt
              : const Duration(milliseconds: 100),
        );
      }
    }
    throw const NatsClosedException();
  }

  void _emitEvent(JetStreamOrderedConsumerEvent event) {
    _events.add(event);
  }

  void _complete([Object? error, StackTrace? stackTrace]) {
    if (!_messages.isClosed) {
      if (error != null) {
        _messages.addError(error, stackTrace);
      }
      _messages.close().ignore();
    }
    _events.close();
    if (_done.isCompleted) {
      return;
    }
    if (error == null) {
      _done.complete();
    } else {
      _done.completeError(error, stackTrace);
    }
  }
}
