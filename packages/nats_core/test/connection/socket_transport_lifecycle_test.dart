@TestOn("vm")
library;

import "dart:async";
import "dart:io";
import "dart:typed_data";

import "package:nats_core/src/options.dart";
import "package:nats_core/src/transport/connector_io.dart";
import "package:test/test.dart";

const _deadline = Duration(seconds: 2);

void main() {
  test("TCP close settles before the incoming stream is listened", () async {
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final accepted = server.first;
    final transport = await connectTransport(
      NatsServer.nats(InternetAddress.loopbackIPv4.address, port: server.port),
      NatsTlsOptions(),
      NatsWebSocketOptions(),
      _deadline,
    ).timeout(_deadline);
    final peer = await accepted.timeout(_deadline);
    final peerDone = peer.drain<void>();
    var incomingDrained = false;
    try {
      await transport.close().timeout(_deadline);
      await peerDone.timeout(_deadline);
      await transport.incoming.drain<void>().timeout(_deadline);
      incomingDrained = true;
    } finally {
      if (!incomingDrained) {
        await transport.incoming.drain<void>().timeout(_deadline);
      }
      peer.destroy();
      await transport.close().timeout(_deadline);
      await server.close().timeout(_deadline);
    }
  });

  test(
    "WebSocket close settles before the incoming stream is listened",
    () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final accepted = Completer<WebSocket>();
      final serverSubscription = server
          .transform(WebSocketTransformer())
          .listen(accepted.complete);
      final transport = await connectTransport(
        NatsServer.websocket(
          InternetAddress.loopbackIPv4.address,
          port: server.port,
        ),
        NatsTlsOptions(),
        NatsWebSocketOptions(),
        _deadline,
      ).timeout(_deadline);
      final peer = await accepted.future.timeout(_deadline);
      final peerDone = peer.drain<void>();
      var incomingDrained = false;
      try {
        await transport.close().timeout(_deadline);
        await peerDone.timeout(_deadline);
        await transport.incoming.drain<void>().timeout(_deadline);
        incomingDrained = true;
      } finally {
        if (!incomingDrained) {
          await transport.incoming.drain<void>().timeout(_deadline);
        }
        await peer.close();
        await transport.close().timeout(_deadline);
        await serverSubscription.cancel();
        await server.close(force: true).timeout(_deadline);
      }
    },
  );

  test("peer reset does not emit an unowned socket sink error", () async {
    final uncaught = <Object>[];
    final finished = Completer<void>();

    runZonedGuarded(() {
      unawaited(() async {
        ServerSocket? server;
        try {
          server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
          final peerFuture = server.first;
          final transport = await connectTransport(
            NatsServer.nats(
              InternetAddress.loopbackIPv4.address,
              port: server.port,
            ),
            NatsTlsOptions(),
            NatsWebSocketOptions(),
            const Duration(seconds: 2),
          );
          final peer = await peerFuture;
          final incomingDone = transport.incoming.drain<void>().catchError(
            (_) {},
          );

          transport.add(Uint8List(16 * 1024 * 1024));
          peer.destroy();
          try {
            await transport.flush();
          } on Object {
            // The incoming stream owns the transport failure.
          }
          await incomingDone.timeout(const Duration(seconds: 2));
          await transport.close();
          await Future<void>.delayed(Duration.zero);
        } finally {
          await server?.close();
          if (!finished.isCompleted) finished.complete();
        }
      }());
    }, (error, _) => uncaught.add(error));

    await finished.future.timeout(const Duration(seconds: 5));
    expect(uncaught, isEmpty);
  });
}
