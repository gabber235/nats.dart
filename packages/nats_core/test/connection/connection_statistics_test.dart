@TestOn("vm")
library;

import "dart:async";
import "dart:convert";
import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:test/test.dart";

void main() {
  test(
    "scripted peer exposes immutable traffic, queue, and RTT state",
    () async {
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
        ),
      );
      final initial = connection.statistics;
      expect(
        () => connection.requestManyOnGeneration(
          "wrong.generation",
          const [],
          generation: connection.generation + 1,
        ),
        throwsA(isA<NatsConnectionException>()),
      );
      final subscription = await connection.subscribe("events");
      final received = Completer<NatsMessage>();
      final listener = subscription.messages.listen(received.complete)..pause();

      await connection.publish("outbound", const [1, 2, 3]);
      await peer.sendMessage("events", subscription.id, const [4, 5]);
      await connection.flush();

      final queued = connection.statistics;
      expect(initial.generation, 1);
      expect(initial.messagesPublished, 0);
      expect(initial.serverAuthority, "127.0.0.1:${peer.port}");
      expect(queued.messagesPublished, 1);
      expect(queued.payloadBytesPublished, 3);
      expect(queued.messagesReceived, 1);
      expect(queued.payloadBytesReceived, 2);
      expect(queued.activeSubscriptions, 1);
      expect(queued.subscriptionQueuedMessages, 1);
      expect(queued.subscriptionQueuedBytes, greaterThanOrEqualTo(2));
      expect(queued.wireBytesSent, greaterThan(0));
      expect(queued.wireBytesReceived, greaterThan(0));
      expect(initial.messagesPublished, 0);

      peer.pausePongs();
      final measured = connection.rtt();
      await peer.nextPing;
      expect(connection.statistics.outstandingPings, 1);
      await peer.sendPong();
      expect(await measured, isNot(Duration.zero));
      expect(connection.statistics.outstandingPings, 0);

      listener.resume();
      expect((await received.future).payload, [4, 5]);
      await listener.cancel();
      await connection.close();
      await peer.close();
    },
  );

  test("scripted malformed protocol increments the terminal counter", () async {
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
      ),
    );

    await peer.sendRaw(ascii.encode("UNKNOWN\r\n"));
    await connection.closed;

    expect(connection.statistics.protocolErrors, 1);
    await peer.close();
  });
}

final class _ScriptedPeer {
  _ScriptedPeer._(this._listener);

  final ServerSocket _listener;
  final Completer<Socket> _accepted = Completer<Socket>();
  Completer<void>? _nextPing;
  StreamSubscription<List<int>>? _reader;
  Socket? _socket;
  String _incoming = "";
  bool _automaticPongs = true;

  int get port => _listener.port;

  Future<void> get nextPing => (_nextPing ??= Completer<void>()).future;

  static Future<_ScriptedPeer> start() async {
    final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final server = _ScriptedPeer._(listener);
    listener.first.then(server._accept).ignore();
    return server;
  }

  void _accept(Socket socket) {
    _socket = socket;
    _reader = socket.listen(_onData);
    socket.add(
      ascii.encode(
        "INFO {\"server_id\":\"scripted\",\"headers\":true,"
        "\"max_payload\":16777216}\r\n",
      ),
    );
    if (!_accepted.isCompleted) {
      _accepted.complete(socket);
    }
  }

  void _onData(List<int> bytes) {
    _incoming += latin1.decode(bytes);
    while (true) {
      final ping = _incoming.indexOf("PING\r\n");
      if (ping == -1) {
        return;
      }
      _incoming = _incoming.substring(ping + 6);
      if (_automaticPongs) {
        _socket?.add(ascii.encode("PONG\r\n"));
      } else {
        final next = _nextPing ??= Completer<void>();
        if (!next.isCompleted) {
          next.complete();
        }
      }
    }
  }

  void pausePongs() {
    _automaticPongs = false;
    _nextPing = Completer<void>();
  }

  Future<void> sendPong() async {
    await _accepted.future;
    _socket!.add(ascii.encode("PONG\r\n"));
    await _socket!.flush();
    _automaticPongs = true;
  }

  Future<void> sendMessage(
    String subject,
    int subscriptionId,
    List<int> payload,
  ) async {
    await _accepted.future;
    _socket!.add(<int>[
      ...ascii.encode("MSG $subject $subscriptionId ${payload.length}\r\n"),
      ...payload,
      13,
      10,
    ]);
    await _socket!.flush();
  }

  Future<void> sendRaw(List<int> bytes) async {
    await _accepted.future;
    _socket!.add(bytes);
    await _socket!.flush();
  }

  Future<void> close() async {
    await _reader?.cancel();
    await _socket?.close();
    await _listener.close();
  }
}
