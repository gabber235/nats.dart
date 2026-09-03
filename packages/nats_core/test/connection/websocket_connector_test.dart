@TestOn("vm")
library;

import "dart:async";
import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_core/src/transport/connector_io.dart";
import "package:test/test.dart";

void main() {
  test("nonupgrade HTTP response is a typed WebSocket failure", () async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    final listener = server.listen((request) async {
      request.response
        ..statusCode = HttpStatus.ok
        ..persistentConnection = false
        ..contentLength = 0;
      await request.response.close();
    });
    addTearDown(() async {
      await listener.cancel();
      await server.close(force: true);
    });

    await expectLater(
      connectTransport(
        NatsServer.websocket(
          InternetAddress.loopbackIPv4.address,
          port: server.port,
        ),
        NatsTlsOptions(),
        NatsWebSocketOptions(),
        const Duration(seconds: 10),
      ),
      throwsA(isA<NatsWebSocketUpgradeException>()),
    );
  });

  test("WebSocket connect timeout remains typed", () async {
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final accepted = Completer<Socket>();
    final listener = server.listen(accepted.complete);
    addTearDown(() async {
      if (accepted.isCompleted) (await accepted.future).destroy();
      await listener.cancel();
      await server.close();
    });

    await expectLater(
      connectTransport(
        NatsServer.websocket(
          InternetAddress.loopbackIPv4.address,
          port: server.port,
        ),
        NatsTlsOptions(),
        NatsWebSocketOptions(),
        const Duration(milliseconds: 25),
      ),
      throwsA(isA<NatsTimeoutException>()),
    );
  });
}
