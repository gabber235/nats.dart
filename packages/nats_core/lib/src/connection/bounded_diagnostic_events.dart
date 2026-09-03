import "dart:async";

/// Broadcast diagnostics with at most one retained event per paused listener.
final class BoundedDiagnosticEvents<T> {
  /// Creates an initially open diagnostic broadcaster.
  BoundedDiagnosticEvents() {
    stream = Stream.multi(_listen, isBroadcast: true);
  }

  final Set<MultiStreamController<T>> _listeners = {};
  final Map<MultiStreamController<T>, T> _pending = {};
  var _closed = false;

  /// Lossy broadcast view. Active listeners receive every event in order.
  late final Stream<T> stream;

  void _listen(MultiStreamController<T> listener) {
    if (_closed) {
      listener.closeSync();
      return;
    }
    _listeners.add(listener);
    listener.onResume = () {
      final event = _pending.remove(listener);
      if (event != null) {
        listener.addSync(event);
      }
    };
    listener.onCancel = () {
      _listeners.remove(listener);
      _pending.remove(listener);
    };
  }

  /// Emits to active listeners and replaces paused listeners' pending event.
  void add(T event) {
    if (_closed) {
      return;
    }
    for (final listener in _listeners.toList(growable: false)) {
      if (listener.isPaused) {
        _pending[listener] = event;
      } else {
        listener.addSync(event);
      }
    }
  }

  /// Preserves each listener's latest event, then terminates every listener.
  void close() {
    if (_closed) {
      return;
    }
    _closed = true;
    for (final listener in _listeners.toList(growable: false)) {
      final event = _pending.remove(listener);
      if (event != null) {
        listener.addSync(event);
      }
      listener.closeSync();
    }
    _listeners.clear();
  }
}
