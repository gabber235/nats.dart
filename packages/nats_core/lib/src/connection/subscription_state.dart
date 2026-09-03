import "dart:async";
import "dart:collection";

import "package:nats_core/src/connection/message_size.dart";
import "package:nats_core/src/error.dart";
import "package:nats_core/src/message.dart";
import "package:nats_core/src/subscription.dart";

/// Internal bounded delivery state for one subscription.
final class SubscriptionState {
  /// Creates bounded delivery state.
  SubscriptionState({
    required this.id,
    required this.subject,
    required this.queue,
    required this.maximumMessages,
    required this.maximumBytes,
    required this.replayOnReconnect,
    required this.onCancel,
    required this.onOverflow,
    this.maximumDeliveries,
    this.onMaximumDeliveries,
  }) {
    _controller = StreamController<NatsMessage>(
      sync: true,
      onListen: _schedule,
      onPause: () {},
      onResume: _schedule,
      onCancel: onCancel,
    );
    _done.future.ignore();
  }

  /// Local subscription identifier.
  final int id;

  /// Subscription subject replayed after reconnect.
  final String subject;

  /// Optional queue group replayed after reconnect.
  final String? queue;

  /// Maximum retained messages.
  final int maximumMessages;

  /// Maximum retained payload bytes.
  final int maximumBytes;

  /// Whether server interest survives connection generation changes.
  final bool replayOnReconnect;

  /// Called when the only stream listener cancels.
  final Future<void> Function() onCancel;

  /// Called exactly once when queue bounds are exceeded.
  final void Function(NatsSlowConsumerException) onOverflow;

  /// Called when the exact total delivery allowance is accepted.
  final void Function()? onMaximumDeliveries;

  final Queue<NatsMessage> _queue = Queue();
  final Completer<void> _done = Completer<void>();

  late final StreamController<NatsMessage> _controller;
  Future<void>? _drainFuture;
  int _queuedBytes = 0;
  int _receivedMessages = 0;
  int _receivedThisGeneration = 0;
  bool _scheduled = false;
  bool _closed = false;
  bool _deliveryEnabled = true;

  /// Total accepted delivery limit, when configured.
  int? maximumDeliveries;

  /// Ordered delivery stream.
  Stream<NatsMessage> get stream => _controller.stream;

  /// Completes after the message stream terminates.
  Future<void> get done => _done.future;

  /// Messages currently retained for application delivery.
  int get queuedMessages => _queue.length;

  /// Bytes currently retained for application delivery.
  int get queuedBytes => _queuedBytes;

  /// Whether this state has completed local closure.
  bool get isClosed => _closed;

  /// Messages accepted across every connection generation.
  int get receivedMessages => _receivedMessages;

  /// Deliveries still permitted across every future generation.
  int? get remainingDeliveries =>
      maximumDeliveries == null ? null : maximumDeliveries! - _receivedMessages;

  /// Maximum count for UNSUB on the active server generation.
  int? get serverGenerationMaximumDeliveries {
    final remaining = remainingDeliveries;
    return remaining == null ? null : _receivedThisGeneration + remaining;
  }

  /// Resets the server local count before replaying this subscription.
  void beginConnectionGeneration() {
    _receivedThisGeneration = 0;
  }

  /// Sets one exact total delivery allowance.
  bool setMaximumDeliveries(int value) {
    if (value < 1) {
      throw ArgumentError.value(value, "maximumDeliveries", "Must be positive");
    }
    if (value < _receivedMessages) {
      throw ArgumentError.value(
        value,
        "maximumDeliveries",
        "Must not be below $receivedMessages accepted deliveries",
      );
    }
    maximumDeliveries = value;
    final reached = value == _receivedMessages;
    if (reached) {
      onMaximumDeliveries?.call();
    }
    return reached;
  }

  /// Retains [message] or reports terminal overflow.
  void enqueue(NatsMessage message) {
    if (_closed) {
      return;
    }
    final deliveryLimit = maximumDeliveries;
    if (deliveryLimit != null && _receivedMessages >= deliveryLimit) {
      return;
    }
    final nextMessages = _queue.length + 1;
    final nextBytes = _queuedBytes + retainedMessageBytes(message);
    if (nextMessages > maximumMessages || nextBytes > maximumBytes) {
      onOverflow(
        NatsSlowConsumerException(
          "Subscription $id exceeded $maximumMessages messages or "
          "$maximumBytes bytes",
        ),
      );
      return;
    }
    _queue.add(message);
    _queuedBytes = nextBytes;
    _receivedMessages += 1;
    _receivedThisGeneration += 1;
    _schedule();
    if (deliveryLimit != null && _receivedMessages == deliveryLimit) {
      onMaximumDeliveries?.call();
    }
  }

  void _schedule() {
    if (_closed ||
        !_deliveryEnabled ||
        _scheduled ||
        !_controller.hasListener ||
        _controller.isPaused) {
      return;
    }
    _scheduled = true;
    scheduleMicrotask(_deliverOne);
  }

  void _deliverOne() {
    _scheduled = false;
    if (_closed ||
        !_deliveryEnabled ||
        _controller.isPaused ||
        _queue.isEmpty) {
      return;
    }
    final message = _queue.removeFirst();
    _queuedBytes -= retainedMessageBytes(message);
    _controller.add(message);
    if (_queue.isEmpty && _drainFuture != null) {
      _finishDrain();
      return;
    }
    _schedule();
  }

  /// Pauses user delivery while retaining the bounded queue.
  void pauseDelivery() {
    _deliveryEnabled = false;
  }

  /// Resumes ordered user delivery from the retained queue.
  void resumeDelivery() {
    if (_closed) {
      return;
    }
    _deliveryEnabled = true;
    _schedule();
  }

  /// Discards retained messages and terminates the stream.
  Future<void> drain() {
    if (_closed) {
      return done;
    }
    return _drainFuture ??= _drain();
  }

  Future<void> _drain() async {
    if (!_controller.hasListener) {
      _queue.clear();
      _queuedBytes = 0;
      _finishDrain();
      return done;
    }
    if (_queue.isNotEmpty) {
      return done;
    }
    _finishDrain();
    return done;
  }

  void _finishDrain() {
    if (_closed) {
      return;
    }
    _closed = true;
    _queuedBytes = 0;
    if (!_done.isCompleted) {
      _done.complete();
    }
    _controller.close().ignore();
  }

  /// Discards retained messages and schedules terminal stream delivery.
  void close([Object? error]) {
    if (_closed) {
      return;
    }
    _closed = true;
    _queue.clear();
    _queuedBytes = 0;
    scheduleMicrotask(() {
      if (error != null) {
        _controller.addError(error);
      }
      if (!_done.isCompleted) {
        if (error == null) {
          _done.complete();
        } else {
          _done.completeError(error);
        }
      }
      _controller.close().ignore();
    });
  }
}

/// Internal public subscription view over [SubscriptionState].
final class CoreNatsSubscription implements NatsSubscription {
  /// Creates a subscription view.
  const CoreNatsSubscription(
    this.state,
    this.onUnsubscribe,
    this.onAutoUnsubscribe,
    this.onDrain,
  );

  /// Underlying bounded state.
  final SubscriptionState state;

  /// Idempotent server unsubscribe action.
  final Future<void> Function() onUnsubscribe;

  /// Idempotent server automatic unsubscribe action.
  final Future<void> Function(int maximumDeliveries) onAutoUnsubscribe;

  /// Idempotent graceful server unsubscribe action.
  final Future<void> Function(Duration timeout) onDrain;

  @override
  int get id => state.id;

  @override
  Stream<NatsMessage> get messages => state.stream;

  @override
  Future<void> get done => state.done;

  @override
  Future<void> unsubscribe() => onUnsubscribe();

  @override
  Future<void> autoUnsubscribe(int maximumDeliveries) {
    if (maximumDeliveries < 1) {
      throw ArgumentError.value(
        maximumDeliveries,
        "maximumDeliveries",
        "Must be positive",
      );
    }
    return onAutoUnsubscribe(maximumDeliveries);
  }

  @override
  Future<void> drain({Duration timeout = const Duration(seconds: 30)}) {
    if (timeout <= Duration.zero) {
      throw ArgumentError.value(timeout, "timeout", "Must be positive");
    }
    return onDrain(timeout);
  }
}
