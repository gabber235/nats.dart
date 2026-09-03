import "dart:async";
import "dart:collection";

import "package:nats_core/src/connection/message_size.dart";
import "package:nats_core/src/error.dart";
import "package:nats_core/src/message.dart";
import "package:nats_core/src/request_many.dart";

/// Creates one request lifecycle timer.
typedef RequestTimerFactory =
    Timer Function(Duration duration, void Function() callback);

Timer _createTimer(Duration duration, void Function() callback) {
  return Timer(duration, callback);
}

/// One bounded multiple response request routed by the shared mux.
final class RequestManyState implements NatsRequestMany {
  /// Creates and starts the absolute request lifetime.
  RequestManyState({
    required this.subject,
    required this.options,
    required void Function() onTerminal,
    required void Function(int dropped) onOverflow,
    RequestTimerFactory timerFactory = _createTimer,
  }) : _onTerminal = onTerminal,
       _onOverflow = onOverflow,
       _timerFactory = timerFactory {
    _controller = StreamController<NatsMessage>(
      sync: true,
      onListen: _schedule,
      onPause: () {},
      onResume: _schedule,
      onCancel: stop,
    );
    _totalTimer = _timerFactory(options.timeout, _finishNormally);
    _done.future.ignore();
  }

  /// Request subject used for no responders diagnostics.
  final String subject;

  /// Completion and buffering policy.
  final NatsRequestManyOptions options;

  final void Function() _onTerminal;
  final void Function(int dropped) _onOverflow;
  final RequestTimerFactory _timerFactory;
  final Queue<NatsMessage> _queue = Queue();
  final Completer<void> _done = Completer<void>();

  late final StreamController<NatsMessage> _controller;
  late final Timer _totalTimer;
  Timer? _stallTimer;
  Object? _terminalError;
  StackTrace? _terminalStackTrace;
  int _queuedBytes = 0;
  int _acceptedReplies = 0;
  bool _scheduled = false;
  bool _terminal = false;
  bool _controllerClosed = false;

  @override
  Stream<NatsMessage> get messages => _controller.stream;

  @override
  Future<void> get done => _done.future;

  /// Replies retained before transfer to the terminal stream.
  int get retainedReplies => _queue.length;

  /// Bytes retained before transfer to the terminal stream.
  int get retainedBytes => _queuedBytes;

  /// Routes one matching mux reply.
  void add(NatsMessage message) {
    if (_terminal) {
      return;
    }
    if (message.headers?.statusCode == 503 && message.payload.isEmpty) {
      if (_acceptedReplies == 0) {
        finishError(
          NatsNoRespondersException(
            subject: message.headers?.first("Nats-Subject") ?? subject,
          ),
        );
      } else {
        _finishNormally();
      }
      return;
    }
    if (options.completeOnEmptyPayload && message.payload.isEmpty) {
      _finishNormally();
      return;
    }

    final retainedBytes = retainedMessageBytes(message);
    if (_queue.length + 1 > options.maximumBufferedMessages ||
        _queuedBytes + retainedBytes > options.maximumBufferedBytes) {
      _onOverflow(_queue.length + 1);
      finishError(
        const NatsSlowConsumerException(
          "Multiple response request exceeded its reply buffer",
        ),
      );
      return;
    }

    _queue.add(message);
    _queuedBytes += retainedBytes;
    _acceptedReplies += 1;
    _resetStallTimer();
    if (_acceptedReplies == options.maximumReplies) {
      _beginTerminal();
    }
    _schedule();
  }

  /// Terminates with [error] and preserves its originating stack.
  void finishError(
    Object error, {
    StackTrace? stackTrace,
    bool discard = true,
  }) {
    if (_terminal) {
      return;
    }
    _terminalError = error;
    _terminalStackTrace = stackTrace;
    if (discard) {
      _queue.clear();
      _queuedBytes = 0;
    }
    _beginTerminal();
    _schedule();
  }

  @override
  Future<void> stop() async {
    if (_terminal) {
      if (!_controllerClosed) {
        _queue.clear();
        _queuedBytes = 0;
        _closeController();
      }
      return;
    }
    _queue.clear();
    _queuedBytes = 0;
    _beginTerminal();
    _schedule();
  }

  void _finishNormally() {
    if (_terminal) {
      return;
    }
    _beginTerminal();
    _schedule();
  }

  void _beginTerminal() {
    _terminal = true;
    _totalTimer.cancel();
    _stallTimer?.cancel();
    _stallTimer = null;
    _onTerminal();
    scheduleMicrotask(_closeController);
  }

  void _resetStallTimer() {
    final timeout = options.stallTimeout;
    if (_terminal || timeout == null) {
      return;
    }
    _stallTimer?.cancel();
    _stallTimer = _timerFactory(timeout, _finishNormally);
  }

  void _schedule() {
    if (_scheduled || _controllerClosed) {
      return;
    }
    if (_queue.isEmpty) {
      if (_terminal) {
        _closeController();
      }
      return;
    }
    if (!_controller.hasListener || _controller.isPaused) {
      return;
    }
    _scheduled = true;
    scheduleMicrotask(_deliverOne);
  }

  void _deliverOne() {
    _scheduled = false;
    if (_controllerClosed ||
        !_controller.hasListener ||
        _controller.isPaused ||
        _queue.isEmpty) {
      return;
    }
    final message = _queue.removeFirst();
    _queuedBytes -= retainedMessageBytes(message);
    _controller.add(message);
    _schedule();
  }

  void _closeController() {
    if (_controllerClosed) {
      return;
    }
    _controllerClosed = true;
    while (_queue.isNotEmpty) {
      _controller.add(_queue.removeFirst());
    }
    _queuedBytes = 0;
    final error = _terminalError;
    if (error != null) {
      _controller.addError(error, _terminalStackTrace);
      _done.completeError(error, _terminalStackTrace);
    } else {
      _done.complete();
    }
    _controller.close().ignore();
  }
}
