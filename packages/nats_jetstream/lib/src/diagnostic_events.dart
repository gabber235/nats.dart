// ignore_for_file: public_member_api_docs

import "dart:async";

/// Broadcast diagnostics with at most one retained event per paused listener.
final class JetStreamDiagnosticEvents<T> {
  JetStreamDiagnosticEvents() {
    stream = Stream.multi(_listen, isBroadcast: true);
  }

  final Set<MultiStreamController<T>> _listeners = {};
  final Map<MultiStreamController<T>, T> _pending = {};
  var _closed = false;

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
