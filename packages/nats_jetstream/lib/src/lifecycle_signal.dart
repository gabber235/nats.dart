// ignore_for_file: public_member_api_docs

import "dart:async";

import "package:nats_core/nats_core.dart";

/// Cancellation owned by one higher level JetStream operation.
final class JetStreamLifecycleSignal {
  final StreamController<void> _cancellations =
      StreamController<void>.broadcast(sync: true);
  var _cancelled = false;

  bool get isCancelled => _cancelled;

  void cancel() {
    if (_cancelled) {
      return;
    }
    _cancelled = true;
    _cancellations.add(null);
    _cancellations.close().ignore();
  }

  /// Waits for readiness, cancellation, permanent close, or an optional bound.
  Future<bool> waitUntilConnected(
    NatsConnection connection, {
    Duration? maximumWait,
  }) async {
    if (isCancelled) {
      return false;
    }
    if (connection.isConnected) {
      return true;
    }

    final ready = Completer<bool>();
    Timer? timer;
    void complete(bool value) {
      if (!ready.isCompleted) {
        ready.complete(value);
      }
    }

    void fail(Object error, StackTrace stackTrace) {
      if (!ready.isCompleted) {
        ready.completeError(error, stackTrace);
      }
    }

    final listener = connection.events.listen((event) {
      switch (event) {
        case NatsConnected():
          complete(true);
        case NatsClosed(:final error):
          fail(error ?? const NatsClosedException(), StackTrace.current);
        default:
          break;
      }
    }, onDone: () => fail(const NatsClosedException(), StackTrace.current));
    final cancellation = _cancellations.stream.listen((_) => complete(false));

    if (isCancelled) {
      complete(false);
    } else if (connection.isConnected) {
      complete(true);
    } else if (maximumWait != null) {
      timer = Timer(maximumWait, () => complete(false));
    }

    try {
      return await ready.future;
    } finally {
      timer?.cancel();
      await cancellation.cancel();
      await listener.cancel();
    }
  }
}
