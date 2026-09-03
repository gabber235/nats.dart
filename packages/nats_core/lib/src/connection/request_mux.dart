import "dart:async";
import "dart:math";

import "package:nats_core/src/connection/request_many_state.dart";
import "package:nats_core/src/error.dart";
import "package:nats_core/src/headers.dart";
import "package:nats_core/src/message.dart";
import "package:nats_core/src/request_many.dart";

/// Direct publication callback used by the internal request mux.
typedef RequestPublisher =
    Future<void> Function(
      String subject,
      List<int> payload, {
      String? replySubject,
      NatsHeaders? headers,
      int? expectedEpoch,
    });

/// Opens the request inbox without a user delivery queue.
typedef RequestInboxOpener =
    Future<void> Function(String subject, void Function(NatsMessage) onMessage);

/// Closes the request inbox.
typedef RequestInboxCloser = Future<void> Function();

/// Concurrent single response request correlation over one wildcard inbox.
final class RequestMux {
  /// Creates a mux using one direct inbox and publication path.
  RequestMux({
    required RequestInboxOpener openInbox,
    required RequestInboxCloser closeInbox,
    required RequestPublisher publish,
  }) : _openInbox = openInbox,
       _closeInbox = closeInbox,
       _publish = publish,
       _inboxPrefix = "_INBOX.${_randomToken(22)}";

  final RequestInboxOpener _openInbox;
  final RequestInboxCloser _closeInbox;
  final RequestPublisher _publish;
  final String _inboxPrefix;
  final Map<String, _RequestState> _requests = {};
  final Map<String, RequestManyState> _manyRequests = {};

  Future<void>? _subscriptionFuture;
  Completer<void>? _idle;

  /// Number of requests currently awaiting a reply.
  int get pending => _requests.length + _manyRequests.length;

  /// Completes when every accepted request has reached a terminal state.
  Future<void> get idle {
    if (_requests.isEmpty && _manyRequests.isEmpty) {
      return Future.value();
    }
    return (_idle ??= Completer<void>()).future;
  }

  /// Publishes one request and completes with its first matching reply.
  Future<NatsMessage> request({
    required String subject,
    required List<int> payload,
    required Duration timeout,
    required int connectionEpoch,
    NatsHeaders? headers,
  }) async {
    final token = _uniqueToken();
    final completer = Completer<NatsMessage>();
    final state = _RequestState(subject, completer);
    _requests[token] = state;
    state.timer = Timer(timeout, () {
      if (_requests.remove(token) == state && !completer.isCompleted) {
        completer.completeError(NatsTimeoutException("Request"));
        _completeIdleIfNeeded();
      }
    });
    try {
      await _ensureSubscription();
      if (_requests[token] != state) {
        return await completer.future;
      }
      await _publish(
        subject,
        payload,
        replySubject: "$_inboxPrefix.$token",
        headers: headers,
        expectedEpoch: connectionEpoch,
      );
    } on Object catch (error, stackTrace) {
      if (_requests.remove(token) == state) {
        state.timer?.cancel();
        completer.completeError(error, stackTrace);
        _completeIdleIfNeeded();
      }
    }
    return completer.future;
  }

  /// Publishes one bounded multiple response request.
  Future<NatsRequestMany> requestMany({
    required String subject,
    required List<int> payload,
    required NatsRequestManyOptions options,
    required int connectionEpoch,
    required void Function(int dropped) onOverflow,
    NatsHeaders? headers,
  }) async {
    final token = _uniqueToken();
    late final RequestManyState state;
    state = RequestManyState(
      subject: subject,
      options: options,
      onTerminal: () {
        if (_manyRequests.remove(token) == state) {
          _completeIdleIfNeeded();
        }
      },
      onOverflow: onOverflow,
    );
    _manyRequests[token] = state;
    try {
      await _ensureSubscription();
      if (_manyRequests[token] != state) {
        return state;
      }
      await _publish(
        subject,
        payload,
        replySubject: "$_inboxPrefix.$token",
        headers: headers,
        expectedEpoch: connectionEpoch,
      );
      return state;
    } on Object catch (error, stackTrace) {
      if (_manyRequests.remove(token) == state) {
        state.finishError(error, stackTrace: stackTrace);
        _completeIdleIfNeeded();
      }
      Error.throwWithStackTrace(error, stackTrace);
    }
  }

  Future<void> _ensureSubscription() async {
    final current = _subscriptionFuture;
    if (current != null) {
      await current;
      return;
    }
    final created = _openInbox("$_inboxPrefix.*", _routeReply);
    _subscriptionFuture = created;
    try {
      await created;
    } on Object {
      if (identical(_subscriptionFuture, created)) {
        _subscriptionFuture = null;
      }
      rethrow;
    }
  }

  void _routeReply(NatsMessage message) {
    final separator = message.subject.lastIndexOf(".");
    if (separator == -1) {
      return;
    }
    final token = message.subject.substring(separator + 1);
    final state = _requests.remove(token);
    if (state != null) {
      state.timer?.cancel();
      _completeIdleIfNeeded();
      if (message.headers?.statusCode == 503 && message.payload.isEmpty) {
        state.completer.completeError(
          NatsNoRespondersException(
            subject: message.headers?.first("Nats-Subject") ?? state.subject,
          ),
        );
        return;
      }
      state.completer.complete(message);
      return;
    }
    _manyRequests[token]?.add(message);
  }

  String _uniqueToken() {
    while (true) {
      final token = _randomToken(12);
      if (!_requests.containsKey(token) && !_manyRequests.containsKey(token)) {
        return token;
      }
    }
  }

  /// Fails every pending request exactly once.
  void failAll(Object error) {
    for (final request in _requests.values) {
      request.timer?.cancel();
      if (!request.completer.isCompleted) {
        request.completer.completeError(error);
      }
    }
    _requests.clear();
    for (final request in _manyRequests.values.toList()) {
      request.finishError(error);
    }
    _manyRequests.clear();
    _completeIdleIfNeeded();
  }

  /// Completes multiple response requests during graceful drain.
  void stopMany() {
    for (final request in _manyRequests.values.toList()) {
      request.stop().ignore();
    }
    _manyRequests.clear();
    _completeIdleIfNeeded();
  }

  /// Stops reply routing. The connection owns server unsubscribe cleanup.
  Future<void> close() async {
    failAll(const NatsClosedException());
    await _closeInbox();
  }

  void _completeIdleIfNeeded() {
    final idle = _idle;
    if (_requests.isEmpty &&
        _manyRequests.isEmpty &&
        idle != null &&
        !idle.isCompleted) {
      idle.complete();
      _idle = null;
    }
  }
}

final class _RequestState {
  _RequestState(this.subject, this.completer);

  final String subject;
  final Completer<NatsMessage> completer;
  Timer? timer;
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
