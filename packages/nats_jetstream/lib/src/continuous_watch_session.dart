// ignore_for_file: public_member_api_docs

import "dart:async";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/consumer_config.dart";
import "package:nats_jetstream/src/consumer_delivery.dart";
import "package:nats_jetstream/src/continuous_watch.dart";
import "package:nats_jetstream/src/continuous_watch_cursor.dart";
import "package:nats_jetstream/src/continuous_watch_generation.dart";
import "package:nats_jetstream/src/error.dart";

/// Shared generation fenced lifecycle for continuous metadata watches.
final class JetStreamContinuousWatchSession<TValue, TEvent> {
  /// Creates a lazy single listener watch.
  JetStreamContinuousWatchSession({
    required this.connection,
    required this.options,
    required String apiPrefix,
    required Duration timeout,
    required this.decode,
    required this.include,
    required this.changedEvent,
    required this.initialCompleteEvent,
  }) : _owner = JetStreamContinuousWatchGenerationOwner(
         connection,
         apiPrefix,
         timeout,
         options,
       ) {
    _controller = StreamController<TEvent>(
      sync: true,
      onListen: _start,
      onPause: _pause,
      onResume: _resume,
      onCancel: _cancel,
    );
    events = _controller.stream;
    _done.future.ignore();
  }

  final NatsConnection connection;
  final JetStreamContinuousWatchOptions options;
  final TValue Function(JetStreamDelivery delivery) decode;
  final bool Function(TValue value) include;
  final TEvent Function(TValue value) changedEvent;
  final TEvent initialCompleteEvent;

  final JetStreamContinuousWatchGenerationOwner _owner;
  final Completer<void> _done = Completer<void>();
  late final StreamController<TEvent> _controller;

  var _closing = false;
  var _started = false;
  var _paused = false;
  Completer<void>? _resumeSignal;
  Object? _terminalError;
  StackTrace? _terminalStackTrace;
  var _streamFinished = false;

  /// Single subscription event stream.
  late final Stream<TEvent> events;

  /// Completes after cleanup, or with the terminal watch failure.
  Future<void> get done => _done.future;

  /// Stops delivery and cleans every owned resource possible.
  Future<void> close() async {
    if (_closing) {
      return done;
    }
    _closing = true;
    _releasePause();
    try {
      await _owner.close();
    } on Object catch (error, stackTrace) {
      _recordTerminal(error, stackTrace);
    }
    if (!_started) {
      _complete();
    }
    return done;
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
    _releasePause();
    if (_terminalError != null) {
      _finishTerminalStream();
      return;
    }
  }

  Future<void> _cancel() async {
    try {
      await close();
    } on Object {
      // The public done future owns the exact terminal failure.
    }
  }

  void _emit(TEvent event) {
    if (_closing) {
      return;
    }
    if (_paused) {
      throw StateError("${options.label} advanced while delivery was paused");
    }
    _controller.add(event);
  }

  Future<void> _run() async {
    final cursor = JetStreamContinuousWatchCursor();
    BigInt? initialRemaining;
    var initialComplete = false;
    try {
      var start = await _owner.initialStart();
      while (!_closing) {
        if (!await _owner.waitUntilConnected()) {
          break;
        }
        await _owner.deleteStaleConsumers();
        if (_closing) {
          break;
        }
        late final JetStreamContinuousWatchGeneration opened;
        try {
          opened = await _owner.open(start);
        } on NatsConnectionException {
          continue;
        }
        cursor.beginGeneration();
        final iterator = StreamIterator(opened.subscription.messages);
        try {
          if (initialRemaining == null) {
            initialRemaining =
                options.start.kind == JetStreamContinuousWatchStartKind.updates
                ? BigInt.zero
                : opened.initialPending;
            if (initialRemaining == BigInt.zero) {
              await _waitWhilePaused(opened.subscription);
              if (_closing) {
                break;
              }
              initialComplete = true;
              _emit(initialCompleteEvent);
            }
          }
          while (!_closing) {
            await _waitWhilePaused(opened.subscription);
            if (_closing) {
              break;
            }
            final available = await iterator.moveNext().timeout(
              options.heartbeat * 2 + const Duration(seconds: 1),
            );
            if (!available) {
              if (_closing) {
                break;
              }
              throw NatsConnectionException(
                "${options.label} delivery subscription closed",
              );
            }
            final message = iterator.current;
            if (message.headers?.statusCode case final status?) {
              await _handleStatus(message, status, opened.generation);
              continue;
            }
            final delivery = requireConsumerDeliveryIdentity(
              JetStreamDelivery.withoutAcknowledgement(message),
              expectedStream: options.stream,
              expectedConsumer: opened.consumer,
              operation: options.label,
            );
            final metadata = delivery.metadata!;
            switch (cursor.inspect(
              streamSequence: metadata.streamSequence,
              consumerSequence: metadata.consumerSequence,
            )) {
              case JetStreamContinuousWatchCursorDecision.gap:
                throw NatsConnectionException(
                  "${options.label} consumer sequence gap",
                );
              case JetStreamContinuousWatchCursorDecision.duplicate:
                continue;
              case JetStreamContinuousWatchCursorDecision.accept:
                break;
            }
            final value = decode(delivery);
            cursor.commit(metadata.streamSequence);
            if (!initialComplete && initialRemaining! > BigInt.zero) {
              initialRemaining -= BigInt.one;
            }
            if (include(value)) {
              _emit(changedEvent(value));
            }
            if (!initialComplete && initialRemaining == BigInt.zero) {
              await _waitWhilePaused(opened.subscription);
              if (_closing) {
                break;
              }
              initialComplete = true;
              _emit(initialCompleteEvent);
            }
          }
        } on TimeoutException {
          if (!initialComplete) {
            throw JetStreamInitialSnapshotInterruptedException(
              operation: options.label,
              lastRevision: cursor.lastRevision,
            );
          }
          continue;
        } on NatsConnectionException {
          if (_closing) {
            break;
          }
          if (!initialComplete) {
            throw JetStreamInitialSnapshotInterruptedException(
              operation: options.label,
              lastRevision: cursor.lastRevision,
            );
          }
          continue;
        } finally {
          await iterator.cancel();
          await _owner.closeGeneration(opened);
          if (cursor.lastRevision case final lastRevision?) {
            start = JetStreamConsumerStart.sequence(lastRevision + BigInt.one);
          }
        }
      }
    } on Object catch (error, stackTrace) {
      _recordTerminal(error, stackTrace);
    } finally {
      _closing = true;
      try {
        await _owner.close();
      } on Object catch (error, stackTrace) {
        _recordTerminal(error, stackTrace);
      }
      _complete();
    }
  }

  Future<void> _waitWhilePaused(NatsSubscription subscription) async {
    while (_paused && !_closing) {
      final resumed = _resumeSignal ??= Completer<void>();
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
      await signaled.future;
    }
  }

  void _releasePause() {
    final signal = _resumeSignal;
    _resumeSignal = null;
    if (signal != null && !signal.isCompleted) {
      signal.complete();
    }
  }

  Future<void> _handleStatus(
    NatsMessage message,
    int status,
    int generation,
  ) async {
    if (status == 100) {
      if (message.replySubject case final reply?) {
        await connection.publishOnGeneration(
          reply,
          const [],
          generation: generation,
        );
      }
      return;
    }
    throw JetStreamConsumerStatusException(
      code: status,
      description: message.headers?.statusDescription ?? "",
      headers: message.headers!,
    );
  }

  void _finishTerminalStream() {
    if (_streamFinished) {
      return;
    }
    _controller.addError(_terminalError!, _terminalStackTrace);
    _finishStream();
  }

  void _finishStream() {
    if (_streamFinished) {
      return;
    }
    _streamFinished = true;
    _controller.close().ignore();
  }

  void _recordTerminal(Object error, StackTrace stackTrace) {
    _terminalError ??= error;
    _terminalStackTrace ??= stackTrace;
  }

  void _complete() {
    if (_terminalError == null) {
      _finishStream();
    } else if (!_paused) {
      _finishTerminalStream();
    }
    if (_done.isCompleted) {
      return;
    }
    final error = _terminalError;
    if (error == null) {
      _done.complete();
    } else {
      _done.completeError(error, _terminalStackTrace);
    }
  }
}
