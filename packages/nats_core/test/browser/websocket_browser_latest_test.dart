@TestOn("browser")
library;

import "dart:async";
import "dart:convert";
import "dart:js_interop";

import "package:nats_core/nats_core.dart";
import "package:test/test.dart";
import "package:web/web.dart" as browser;

void main() {
  test(
    "Chrome rejects unsupported transport options before connection",
    () async {
      final cases = <(NatsOptions, String)>[
        (
          NatsOptions(servers: const [NatsServer.nats("native.invalid")]),
          "servers",
        ),
        (
          NatsOptions(
            servers: const [NatsServer.websocket("browser.invalid")],
            webSocket: NatsWebSocketOptions(headers: const {"X-Test": "one"}),
          ),
          "webSocket.headers",
        ),
        (
          NatsOptions(
            servers: const [NatsServer.websocket("browser.invalid")],
            tls: NatsTlsOptions(trustedCertificates: const [1]),
          ),
          "tls.trustedCertificates",
        ),
        (
          NatsOptions(
            servers: const [NatsServer.websocket("browser.invalid")],
            tls: NatsTlsOptions(
              clientCertificateChain: const [1],
              clientPrivateKey: const [2],
            ),
          ),
          "tls.clientCertificateChain",
        ),
        (
          NatsOptions(
            servers: const [NatsServer.websocket("browser.invalid")],
            tls: NatsTlsOptions(allowInvalidCertificates: true),
          ),
          "tls.allowInvalidCertificates",
        ),
      ];
      for (final (options, option) in cases) {
        await expectLater(
          NatsConnection.connect(options),
          throwsA(
            isA<NatsUnsupportedRuntimeException>().having(
              (error) => error.option,
              "option",
              option,
            ),
          ),
        );
      }
      await expectLater(
        loadNatsCredentialsFile("credentials.creds"),
        throwsA(
          isA<NatsUnsupportedRuntimeException>().having(
            (error) => error.option,
            "option",
            "credentialsFile",
          ),
        ),
      );
    },
  );

  test(
    "Chrome exchanges binary Core messages through WebSocket",
    () => _exercise("ws://nats-ws:8080/nats"),
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "Chrome exchanges binary Core messages through secure WebSocket",
    () => _exercise("wss://nats-wss:8443/nats"),
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "Chrome accepts an exact allowed origin and rejects another origin",
    _exerciseOriginPolicy,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "Chrome rejects a WebSocket with an untrusted mismatched certificate",
    () async {
      const url = "wss://nats-browser:8444/nats";
      final socket = browser.WebSocket(url);
      final browserResult = await Future.any([
        socket.onOpen.first.then((_) => "opened"),
        socket.onError.first.then((_) => "rejected"),
      ]).timeout(const Duration(seconds: 5));
      socket.close();
      expect(browserResult, "rejected");

      await expectLater(
        NatsConnection.connect(
          NatsOptions(
            servers: [NatsServer.parse(url)],
            connectTimeout: const Duration(seconds: 3),
            reconnect: NatsReconnectPolicy(maximumAttempts: 0),
          ),
        ),
        throwsA(isA<NatsConnectionException>()),
      );
    },
    timeout: const Timeout(Duration(seconds: 10)),
  );

  test(
    "Chrome and Go exchange headers before discovered reconnect",
    _exerciseGoInteropAndReconnect,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}

Future<void> _exercise(String url) async {
  final connection = await NatsConnection.connect(
    NatsOptions(servers: [NatsServer.parse(url)]),
  );
  addTearDown(connection.close);

  final suffix = DateTime.now().microsecondsSinceEpoch;
  final subject = "browser.$suffix.events";
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

  final serviceSubject = "browser.$suffix.service";
  final responder = await connection.subscribe(serviceSubject);
  final responderListener = responder.messages.listen((request) async {
    await connection.publish(request.replySubject!, const [42]);
  });
  final reply = await connection.request(serviceSubject, const [1]);
  expect(reply.payload, [42]);

  await responderListener.cancel();
  await subscription.unsubscribe();
  await responder.unsubscribe();
  await connection.drain();
}

Future<void> _exerciseOriginPolicy() async {
  const originUrl = "ws://nats-browser:8082/nats";
  final bootstrap = await _connect(originUrl);
  await _control(
    "/origin?value=${Uri.encodeQueryComponent(browser.window.origin)}",
  );
  await bootstrap.close();

  final allowed = await _connect(originUrl);
  await allowed.flush();
  await _control(
    "/origin?value=${Uri.encodeQueryComponent("https://denied.example")}",
  );
  await allowed.close();

  await expectLater(
    _connect(originUrl),
    throwsA(isA<NatsConnectionException>()),
  );
}

Future<void> _exerciseGoInteropAndReconnect() async {
  final connection = await NatsConnection.connect(
    NatsOptions(
      servers: [NatsServer.parse("ws://nats-browser:8080/nats")],
      reconnect: NatsReconnectPolicy(
        maximumAttempts: 20,
        baseDelay: const Duration(milliseconds: 20),
        maximumDelay: const Duration(milliseconds: 50),
        jitter: 0,
      ),
    ),
  );
  addTearDown(connection.close);
  await _awaitDiscoveredServer(connection, 8081);

  final ready = await connection.request("browser.go.ready", const []);
  expect(utf8.decode(ready.payload), "READY");

  final goResponse = await connection.request(
    "browser.go.request",
    const [0, 255, 13, 10],
    headers: NatsHeaders(
      entries: const [
        MapEntry("X-From-Dart", "dart-one"),
        MapEntry("x-from-dart", "dart-two"),
      ],
    ),
  );
  expect(goResponse.payload, [...utf8.encode("go:"), 0, 255, 13, 10]);
  expect(goResponse.headers?.values("traceparent"), ["first", "second"]);

  final fromGo = await connection.subscribe("browser.dart.receive");
  final deliveries = StreamController<NatsMessage>.broadcast(sync: true);
  final deliveryListener = fromGo.messages.listen(deliveries.add);
  addTearDown(deliveryListener.cancel);
  addTearDown(deliveries.close);
  final delivery = deliveries.stream.first;
  final receipt = await connection.request("browser.go.publish", const [
    10,
    13,
    255,
    0,
  ]);
  expect(utf8.decode(receipt.payload), "OK");
  final message = await delivery.timeout(const Duration(seconds: 3));
  expect(message.payload, [10, 13, 255, 0]);
  expect(message.headers?.values("x-interop"), ["go-one", "go-two"]);

  final reconnected = Completer<NatsConnected>();
  final events = connection.events.listen((event) {
    if (event case NatsConnected(generation: 2)) {
      reconnected.complete(event);
    }
  });
  addTearDown(events.cancel);
  await _control("/stop-a");
  final connected = await reconnected.future.timeout(
    const Duration(seconds: 10),
  );
  expect(connected.server.scheme, "ws");
  expect(connected.server.host, "nats-browser");
  expect(connected.server.port, 8081);
  expect(connected.server.path, "/nats");

  final replayed = deliveries.stream.first;
  await connection.publish("browser.dart.receive", const [42]);
  await connection.flush();
  expect((await replayed.timeout(const Duration(seconds: 3))).payload, [42]);

  await events.cancel();
  await deliveryListener.cancel();
  await deliveries.close();
  await fromGo.unsubscribe();
  await connection.drain();
}

Future<NatsConnection> _connect(String url) => NatsConnection.connect(
  NatsOptions(
    servers: [NatsServer.parse(url)],
    connectTimeout: const Duration(seconds: 3),
    reconnect: NatsReconnectPolicy(maximumAttempts: 0),
  ),
);

Future<void> _control(String path) async {
  final response = await browser.window
      .fetch("http://nats-browser:8090$path".toJS)
      .toDart
      .timeout(const Duration(seconds: 5));
  if (!response.ok) {
    throw StateError("Browser fixture control failed with ${response.status}");
  }
}

Future<void> _awaitDiscoveredServer(NatsConnection connection, int port) async {
  final deadline = DateTime.timestamp().add(const Duration(seconds: 5));
  while (DateTime.timestamp().isBefore(deadline)) {
    if (connection.getServers().any((server) => server.port == port)) {
      return;
    }
    await Future<void>.delayed(const Duration(milliseconds: 20));
  }
  throw StateError(
    "WebSocket discovery did not include port $port: "
    "${connection.getServers()}",
  );
}
