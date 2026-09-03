@TestOn("vm")
library;

import "dart:async";
import "dart:collection";
import "dart:convert";
import "dart:io";
import "dart:typed_data";

import "package:nats_core/nats_core.dart";
import "package:test/test.dart";

const _deadline = Duration(seconds: 2);

void main() {
  test(
    "automatic unsubscribe emits initial and dynamic absolute limits",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);

      final initial = await harness.connection.subscribe(
        "initial",
        options: NatsSubscriptionOptions(maximumDeliveries: 3),
      );
      expect(
        await harness.peer.nextCommand("SUB initial "),
        "SUB initial ${initial.id}",
      );
      expect(
        await harness.peer.nextCommand("UNSUB ${initial.id} "),
        "UNSUB ${initial.id} 3",
      );

      final dynamic = await harness.connection.subscribe("dynamic");
      expect(
        await harness.peer.nextCommand("SUB dynamic "),
        "SUB dynamic ${dynamic.id}",
      );
      final received = <int>[];
      final listener = dynamic.messages.listen(
        (message) => received.add(message.payload.single),
      );
      addTearDown(listener.cancel);
      await harness.peer.sendMessages("dynamic", dynamic.id, 2);
      await _waitUntil(() => received.length == 2);

      await dynamic.autoUnsubscribe(5);
      expect(
        await harness.peer.nextCommand("UNSUB ${dynamic.id} "),
        "UNSUB ${dynamic.id} 5",
      );
      await expectLater(dynamic.autoUnsubscribe(1), throwsArgumentError);
    },
  );

  test(
    "drain still owns queued deliveries after the limit is reached",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final subscription = await harness.connection.subscribe(
        "events",
        options: NatsSubscriptionOptions(maximumDeliveries: 2),
      );
      await harness.peer.nextCommand("SUB events ");
      await harness.peer.nextCommand("UNSUB ${subscription.id} ");
      final received = <int>[];
      final listener = subscription.messages.listen(
        (message) => received.add(message.payload.single),
      )..pause();
      addTearDown(listener.cancel);

      await harness.peer.sendMessages("events", subscription.id, 4);
      await _waitUntil(
        () => harness.connection.statistics.messagesReceived == 4,
      );
      expect(harness.connection.statistics.activeSubscriptions, 0);
      expect(harness.connection.statistics.subscriptionQueuedMessages, 2);
      var drainCompleted = false;
      final draining = subscription.drain(timeout: _deadline).then((_) {
        drainCompleted = true;
      });
      await Future<void>.delayed(Duration.zero);
      expect(drainCompleted, isFalse);

      listener.resume();
      await draining.timeout(_deadline);
      await subscription.done.timeout(_deadline);
      expect(received, [0, 1]);
      expect(harness.connection.statistics.activeSubscriptions, 0);
      expect(harness.connection.statistics.subscriptionQueuedMessages, 0);
    },
  );

  test("listener cancellation closes an exhausted retained queue", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final subscription = await harness.connection.subscribe(
      "events",
      options: NatsSubscriptionOptions(maximumDeliveries: 2),
    );
    await harness.peer.nextCommand("SUB events ");
    await harness.peer.nextCommand("UNSUB ${subscription.id} ");
    final received = <NatsMessage>[];
    final listener = subscription.messages.listen(received.add)..pause();

    await harness.peer.sendMessages("events", subscription.id, 4);
    await _waitUntil(() => harness.connection.statistics.messagesReceived == 4);
    expect(harness.connection.statistics.subscriptionQueuedMessages, 2);
    await listener.cancel().timeout(_deadline);
    await subscription.done.timeout(_deadline);

    expect(received, isEmpty);
    expect(harness.connection.statistics.activeSubscriptions, 0);
    expect(harness.connection.statistics.subscriptionQueuedMessages, 0);
  });

  test("connection close settles an exhausted retained queue", () async {
    final harness = await _Harness.open();
    addTearDown(harness.peer.close);
    final subscription = await harness.connection.subscribe(
      "events",
      options: NatsSubscriptionOptions(maximumDeliveries: 2),
    );
    await harness.peer.nextCommand("SUB events ");
    await harness.peer.nextCommand("UNSUB ${subscription.id} ");
    final listener = subscription.messages.listen((_) {})..pause();
    addTearDown(listener.cancel);

    await harness.peer.sendMessages("events", subscription.id, 4);
    await _waitUntil(() => harness.connection.statistics.messagesReceived == 4);
    expect(harness.connection.statistics.subscriptionQueuedMessages, 2);

    await harness.connection.close().timeout(_deadline);
    await subscription.done.timeout(_deadline);
    expect(harness.connection.statistics.activeSubscriptions, 0);
    expect(harness.connection.statistics.subscriptionQueuedMessages, 0);
    expect(harness.connection.statistics.subscriptionQueuedBytes, 0);
  });
}

Future<void> _waitUntil(bool Function() condition) async {
  final deadline = DateTime.timestamp().add(_deadline);
  while (!condition()) {
    if (DateTime.timestamp().isAfter(deadline)) {
      throw TimeoutException("Scripted condition did not become true");
    }
    await Future<void>.delayed(Duration.zero);
  }
}

final class _Harness {
  const _Harness(this.peer, this.connection);

  static Future<_Harness> open() async {
    final peer = await _ScriptedPeer.start();
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [
          NatsServer.nats(
            InternetAddress.loopbackIPv4.address,
            port: peer.port,
          ),
        ],
        reconnect: NatsReconnectPolicy(maximumAttempts: 0),
        pingInterval: const Duration(minutes: 10),
      ),
    );
    return _Harness(peer, connection);
  }

  final _ScriptedPeer peer;
  final NatsConnection connection;

  Future<void> close() async {
    await connection.close();
    await peer.close();
  }
}

final class _ScriptedPeer {
  _ScriptedPeer._(this._listener);

  static Future<_ScriptedPeer> start() async {
    final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final peer = _ScriptedPeer._(listener);
    peer._acceptor = listener.listen(peer._accept);
    return peer;
  }

  final ServerSocket _listener;
  final Queue<String> _commands = Queue();
  final List<_CommandWaiter> _waiters = [];
  StreamSubscription<Socket>? _acceptor;
  StreamSubscription<List<int>>? _reader;
  Socket? _socket;
  String _incoming = "";

  int get port => _listener.port;

  void _accept(Socket socket) {
    _socket = socket;
    _reader = socket.listen(_onData);
    socket.add(
      ascii.encode(
        "INFO {\"server_id\":\"scripted\",\"headers\":true,"
        "\"max_payload\":16777216}\r\n",
      ),
    );
  }

  void _onData(List<int> bytes) {
    _incoming += ascii.decode(bytes);
    while (true) {
      final end = _incoming.indexOf("\r\n");
      if (end < 0) {
        return;
      }
      final command = _incoming.substring(0, end);
      _incoming = _incoming.substring(end + 2);
      if (command == "PING") {
        _socket?.add(ascii.encode("PONG\r\n"));
      }
      if (command.startsWith("SUB ") || command.startsWith("UNSUB ")) {
        _record(command);
      }
    }
  }

  void _record(String command) {
    final waiterIndex = _waiters.indexWhere(
      (waiter) => command.startsWith(waiter.prefix),
    );
    if (waiterIndex < 0) {
      _commands.add(command);
      return;
    }
    _waiters.removeAt(waiterIndex).completer.complete(command);
  }

  Future<String> nextCommand(String prefix) {
    final commandIndex = _commands.toList().indexWhere(
      (command) => command.startsWith(prefix),
    );
    if (commandIndex >= 0) {
      final commands = _commands.toList();
      final command = commands.removeAt(commandIndex);
      _commands
        ..clear()
        ..addAll(commands);
      return Future.value(command);
    }
    final completer = Completer<String>();
    _waiters.add(_CommandWaiter(prefix, completer));
    return completer.future.timeout(_deadline);
  }

  Future<void> sendMessages(String subject, int id, int count) async {
    final wire = BytesBuilder(copy: false);
    for (var index = 0; index < count; index++) {
      wire
        ..add(ascii.encode("MSG $subject $id 1\r\n"))
        ..add([index])
        ..add(const [13, 10]);
    }
    _socket!.add(wire.takeBytes());
    await _socket!.flush();
  }

  Future<void> close() async {
    await _reader?.cancel();
    await _socket?.close();
    await _acceptor?.cancel();
    await _listener.close();
  }
}

final class _CommandWaiter {
  const _CommandWaiter(this.prefix, this.completer);

  final String prefix;
  final Completer<String> completer;
}
