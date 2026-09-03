import "dart:async";
import "dart:convert";
import "dart:io";
import "dart:typed_data";

import "nats_wire_frame_parser.dart";

final class ScriptedNatsPublication {
  const ScriptedNatsPublication({
    required this.command,
    required this.subject,
    required this.replySubject,
    required this.headerBytes,
    required this.body,
  });

  final String command;
  final String subject;
  final String? replySubject;
  final Uint8List headerBytes;
  final Uint8List body;
}

final class ScriptedNatsPeer {
  ScriptedNatsPeer._(
    this._listener, {
    required this.headers,
    required this.maximumPayloadBytes,
    required Set<String> deniedSubscriptions,
  }) : deniedSubscriptions = Set.unmodifiable(deniedSubscriptions);

  static Future<ScriptedNatsPeer> start({
    bool headers = true,
    int maximumPayloadBytes = 1024 * 1024,
    Set<String> deniedSubscriptions = const {},
  }) async {
    final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final peer = ScriptedNatsPeer._(
      listener,
      headers: headers,
      maximumPayloadBytes: maximumPayloadBytes,
      deniedSubscriptions: deniedSubscriptions,
    );
    peer._listenerSubscription = listener.listen(peer._accept);
    return peer;
  }

  final ServerSocket _listener;
  final bool headers;
  final int maximumPayloadBytes;
  final Set<String> deniedSubscriptions;
  late final StreamSubscription<Socket> _listenerSubscription;
  final NatsWireFrameParser _parser = NatsWireFrameParser(
    NatsWireDirection.client,
  );
  final Map<int, String> _subscriptions = {};
  final List<String> _pendingSubscriptions = [];
  final List<_SubscriptionWaiter> _subscriptionWaiters = [];
  final List<ScriptedNatsPublication> _pending = [];
  final List<_PublicationWaiter> _waiters = [];
  final List<ScriptedNatsPublication> publications = [];
  final Completer<void> _accepted = Completer<void>();
  Socket? _socket;
  StreamSubscription<List<int>>? _socketSubscription;
  var _respondToPings = true;

  Uri get uri => Uri(
    scheme: "nats",
    host: InternetAddress.loopbackIPv4.address,
    port: _listener.port,
  );

  Future<void> get accepted => _accepted.future;

  Future<ScriptedNatsPublication> nextPublication(String subject) {
    return nextPublicationMatching(subject);
  }

  Future<ScriptedNatsPublication> nextPublicationMatching(String pattern) {
    final index = _pending.indexWhere(
      (value) => _matches(pattern, value.subject),
    );
    if (index >= 0) return Future.value(_pending.removeAt(index));
    final completer = Completer<ScriptedNatsPublication>();
    _waiters.add(_PublicationWaiter(pattern, completer));
    return completer.future;
  }

  Future<String> nextSubscription(String pattern) {
    final index = _pendingSubscriptions.indexWhere(
      (subject) => _matches(pattern, subject),
    );
    if (index >= 0) {
      return Future.value(_pendingSubscriptions.removeAt(index));
    }
    final completer = Completer<String>();
    _subscriptionWaiters.add(_SubscriptionWaiter(pattern, completer));
    return completer.future;
  }

  Future<void> sendMessage(
    String subject,
    List<int> payload, {
    String? replySubject,
  }) async {
    final socket = _socket;
    if (socket == null) throw StateError("No client is connected");
    final sid = _subscriptionFor(subject);
    if (sid == null) {
      throw StateError("No subscription matches $subject");
    }
    final reply = replySubject == null ? "" : " $replySubject";
    socket
      ..add(ascii.encode("MSG $subject $sid$reply ${payload.length}\r\n"))
      ..add(payload)
      ..add(const [13, 10]);
    await socket.flush();
  }

  Future<void> sendMessageFlood(
    String subject,
    List<int> payload, {
    required int count,
    String Function(int index)? replySubject,
  }) async {
    final socket = _socket;
    if (socket == null) throw StateError("No client is connected");
    final sid = _subscriptionFor(subject);
    if (sid == null) {
      throw StateError("No subscription matches $subject");
    }
    final wire = BytesBuilder(copy: false);
    for (var index = 0; index < count; index++) {
      final reply = replySubject?.call(index);
      final replyToken = reply == null ? "" : " $reply";
      wire
        ..add(
          ascii.encode("MSG $subject $sid$replyToken ${payload.length}\r\n"),
        )
        ..add(payload)
        ..add(const [13, 10]);
    }
    socket.add(wire.takeBytes());
    await socket.flush();
  }

  Future<void> sendStatus(
    String subject,
    int status, {
    String description = "",
  }) async {
    final socket = _socket;
    if (socket == null) throw StateError("No client is connected");
    final sid = _subscriptionFor(subject);
    if (sid == null) {
      throw StateError("No subscription matches $subject");
    }
    final suffix = description.isEmpty ? "" : " $description";
    final headers = ascii.encode("NATS/1.0 $status$suffix\r\n\r\n");
    socket
      ..add(
        ascii.encode(
          "HMSG $subject $sid ${headers.length} ${headers.length}\r\n",
        ),
      )
      ..add(headers)
      ..add(const [13, 10]);
    await socket.flush();
  }

  Future<void> sendHeaderMessage(
    String subject,
    List<int> payload,
    List<int> headerBytes,
  ) async {
    final socket = _socket;
    if (socket == null) throw StateError("No client is connected");
    final sid = _subscriptionFor(subject);
    if (sid == null) {
      throw StateError("No subscription matches $subject");
    }
    socket
      ..add(
        ascii.encode(
          "HMSG $subject $sid ${headerBytes.length} "
          "${headerBytes.length + payload.length}\r\n",
        ),
      )
      ..add(headerBytes)
      ..add(payload)
      ..add(const [13, 10]);
    await socket.flush();
  }

  Future<void> disconnectClient() async {
    final subscription = _socketSubscription;
    final socket = _socket;
    _socketSubscription = null;
    _socket = null;
    _subscriptions.clear();
    await subscription?.cancel();
    socket?.destroy();
  }

  void suspendPingResponses() {
    _respondToPings = false;
  }

  Future<void> close() async {
    for (final waiter in _waiters) {
      if (!waiter.completer.isCompleted) {
        waiter.completer.completeError(StateError("Scripted peer closed"));
      }
    }
    for (final waiter in _subscriptionWaiters) {
      if (!waiter.completer.isCompleted) {
        waiter.completer.completeError(StateError("Scripted peer closed"));
      }
    }
    await _socketSubscription?.cancel();
    await _socket?.close();
    await _listenerSubscription.cancel();
    await _listener.close();
  }

  void _accept(Socket socket) {
    if (_socket != null) {
      socket.destroy();
      return;
    }
    _socket = socket;
    final information = jsonEncode({
      "server_id": "SCRIPTED",
      "server_name": "SCRIPTED",
      "version": "2.14.6",
      "proto": 1,
      "go": "go1.25",
      "host": InternetAddress.loopbackIPv4.address,
      "port": _listener.port,
      "max_payload": maximumPayloadBytes,
      "headers": headers,
      "jetstream": true,
    });
    socket.add(ascii.encode("INFO $information\r\n"));
    _socketSubscription = socket.listen(_receive);
    if (!_accepted.isCompleted) _accepted.complete();
  }

  void _receive(List<int> bytes) {
    for (final frame in _parser.add(bytes)) {
      switch (frame.command) {
        case "PING":
          if (_respondToPings) {
            _socket?.add(ascii.encode("PONG\r\n"));
          }
        case "SUB":
          final tokens = _controlLine(frame).split(RegExp(r"[ \t]+"));
          final subject = tokens[1];
          _subscriptions[int.parse(tokens.last)] = subject;
          if (deniedSubscriptions.any(
            (pattern) => _matches(pattern, subject),
          )) {
            _socket?.add(
              ascii.encode(
                "-ERR 'Permissions Violation for Subscription to "
                "\"$subject\"'\r\n",
              ),
            );
          }
          final waiterIndex = _subscriptionWaiters.indexWhere(
            (value) => _matches(value.pattern, subject),
          );
          if (waiterIndex >= 0) {
            _subscriptionWaiters
                .removeAt(waiterIndex)
                .completer
                .complete(subject);
          } else {
            _pendingSubscriptions.add(subject);
          }
        case "UNSUB":
          final tokens = _controlLine(frame).split(RegExp(r"[ \t]+"));
          _subscriptions.remove(int.parse(tokens[1]));
        case "PUB" || "HPUB":
          final publication = ScriptedNatsPublication(
            command: frame.command,
            subject: frame.subject!,
            replySubject: frame.replySubject,
            headerBytes: Uint8List.fromList(frame.headerBytes),
            body: Uint8List.fromList(frame.body),
          );
          publications.add(publication);
          final waiterIndex = _waiters.indexWhere(
            (value) => _matches(value.pattern, publication.subject),
          );
          if (waiterIndex >= 0) {
            _waiters.removeAt(waiterIndex).completer.complete(publication);
          } else {
            _pending.add(publication);
          }
      }
    }
  }

  int? _subscriptionFor(String subject) {
    for (final entry in _subscriptions.entries) {
      if (_matches(entry.value, subject)) return entry.key;
    }
    return null;
  }
}

final class _PublicationWaiter {
  const _PublicationWaiter(this.pattern, this.completer);

  final String pattern;
  final Completer<ScriptedNatsPublication> completer;
}

final class _SubscriptionWaiter {
  const _SubscriptionWaiter(this.pattern, this.completer);

  final String pattern;
  final Completer<String> completer;
}

String _controlLine(NatsWireFrame frame) {
  final end = frame.bytes.indexOf(13);
  return ascii.decode(Uint8List.sublistView(frame.bytes, 0, end));
}

bool _matches(String subscription, String subject) {
  final pattern = subscription.split(".");
  final value = subject.split(".");
  for (var index = 0; index < pattern.length; index += 1) {
    final token = pattern[index];
    if (token == ">") return true;
    if (index >= value.length || (token != "*" && token != value[index])) {
      return false;
    }
  }
  return pattern.length == value.length;
}
