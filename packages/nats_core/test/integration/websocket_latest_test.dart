@TestOn("vm")
library;

import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:test/test.dart";

void main() {
  final plainUrl = Platform.environment["NATS_WS_URL"];
  final secureUrl = Platform.environment["NATS_WSS_URL"];
  final certificatePath = Platform.environment["NATS_WS_CA"];
  final skipReason =
      plainUrl == null || secureUrl == null || certificatePath == null
      ? "WebSocket integration environment is not configured"
      : false;

  test(
    "NATS 2.14 WebSocket and secure WebSocket preserve Core semantics",
    () async {
      await _exercise(
        NatsServer.parse(plainUrl!),
        NatsTlsOptions(),
        NatsWebSocketOptions(headers: const {"X-Nats-Test": "present"}),
      );

      final certificate = await File(certificatePath!).readAsBytes();
      await _exercise(
        NatsServer.parse(secureUrl!),
        NatsTlsOptions(trustedCertificates: certificate),
        NatsWebSocketOptions(),
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}

Future<void> _exercise(
  NatsServer server,
  NatsTlsOptions tls,
  NatsWebSocketOptions webSocket,
) async {
  final connection = await NatsConnection.connect(
    NatsOptions(servers: [server], tls: tls, webSocket: webSocket),
  );
  addTearDown(connection.close);
  final suffix = DateTime.now().microsecondsSinceEpoch;
  final subject = "websocket.$suffix.events";
  final subscription = await connection.subscribe(subject);
  final received = subscription.messages.first;

  await connection.publish(subject, const [0, 255, 13, 10]);
  await connection.flush();
  expect((await received.timeout(const Duration(seconds: 2))).payload, [
    0,
    255,
    13,
    10,
  ]);

  final serviceSubject = "websocket.$suffix.service";
  final responder = await connection.subscribe(serviceSubject);
  final responderListener = responder.messages.listen((request) async {
    await connection.publish(request.replySubject!, const [42]);
  });
  final reply = await connection.request(serviceSubject, const [1]);
  expect(reply.payload, [42]);

  await subscription.unsubscribe();
  await responder.unsubscribe();
  await responderListener.cancel();
  await connection.drain();
  await connection.closed;
}
