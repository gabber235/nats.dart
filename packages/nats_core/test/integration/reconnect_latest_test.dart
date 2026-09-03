@TestOn("vm")
library;

import "dart:async";
import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:test/test.dart";

import "../support/nats_server_process.dart";

const _publicNkey = "UD466L6EBCM3YY5HEGHJANNTN4LSKTSUXTH7RILHCKEQMQHTBNLHJJXT";

void main() {
  final serverBinary = Platform.environment["NATS_SERVER_BINARY"];
  final skipReason = serverBinary == null
      ? "NATS_SERVER_BINARY is not configured"
      : false;

  test(
    "automatic unsubscribe replays only its remaining allowance",
    () async {
      final environment = await _createEnvironment();
      NatsServerProcess? server;
      NatsConnection? connection;
      StreamIterator<NatsMessage>? messages;
      addTearDown(() async {
        await messages?.cancel();
        await connection?.close();
        await server?.stop();
        await environment.directory.delete(recursive: true);
      });

      server = await _startServer(serverBinary!, environment);
      connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(server.clientUri.toString())],
          authentication: NatsAuthentication.userPassword(
            "alice",
            "wonderland",
          ),
          reconnect: NatsReconnectPolicy(
            maximumAttempts: -1,
            baseDelay: const Duration(milliseconds: 20),
            maximumDelay: const Duration(milliseconds: 50),
            jitter: 0,
          ),
        ),
      );
      final subject =
          "integration.${DateTime.now().microsecondsSinceEpoch}.automatic";
      final subscription = await connection.subscribe(
        subject,
        options: NatsSubscriptionOptions(maximumDeliveries: 3),
      );
      messages = StreamIterator(subscription.messages);
      await connection.publish(subject, const [1]);
      await connection.flush();
      expect(await messages.moveNext(), isTrue);
      expect(messages.current.payload, [1]);

      final disconnected = connection.events.firstWhere(
        (event) => event is NatsDisconnected,
      );
      final reconnected = connection.events.firstWhere(
        (event) => event is NatsConnected && event.generation == 2,
      );
      await server.stop();
      server = null;
      await disconnected.timeout(const Duration(seconds: 5));
      server = await _startServer(serverBinary, environment);
      await reconnected.timeout(const Duration(seconds: 10));

      await connection.publish(subject, const [2]);
      await connection.publish(subject, const [3]);
      await connection.publish(subject, const [4]);
      await connection.flush();
      expect(await messages.moveNext(), isTrue);
      expect(messages.current.payload, [2]);
      expect(await messages.moveNext(), isTrue);
      expect(messages.current.payload, [3]);
      expect(await messages.moveNext(), isFalse);
      await subscription.done;
      expect(connection.statistics.activeSubscriptions, 0);
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "restart replays interest before retained publications "
    "without retrying requests",
    () async {
      final environment = await _createEnvironment();
      NatsServerProcess? server;
      NatsConnection? connection;
      StreamSubscription<NatsConnectionEvent>? eventListener;
      StreamSubscription<NatsMessage>? deliveryListener;
      StreamSubscription<NatsMessage>? blackholeListener;
      final deliveries = StreamController<NatsMessage>.broadcast(sync: true);
      final blackholeDeliveries = StreamController<NatsMessage>.broadcast(
        sync: true,
      );
      addTearDown(() async {
        await eventListener?.cancel();
        await deliveryListener?.cancel();
        await blackholeListener?.cancel();
        await deliveries.close();
        await blackholeDeliveries.close();
        await connection?.close();
        await server?.stop();
        await environment.directory.delete(recursive: true);
      });

      server = await _startServer(serverBinary!, environment);
      final authenticationNonces = <String?>[];
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final clientName = "nats-dart-reconnect-$suffix";
      connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(server.clientUri.toString())],
          name: clientName,
          authentication: NatsAuthentication.provider((context) async {
            authenticationNonces.add(context.nonce);
            return NatsConnectCredentials(
              username: "alice",
              password: "wonderland",
            );
          }),
          reconnect: NatsReconnectPolicy(
            maximumAttempts: -1,
            baseDelay: const Duration(milliseconds: 20),
            maximumDelay: const Duration(milliseconds: 50),
            jitter: 0,
            bufferBytes: 512,
          ),
        ),
      );
      final connectionEvents = connection.events;
      final deliveryOrder = <String>[];
      eventListener = connectionEvents.listen((event) {
        if (event case NatsConnected(generation: 2)) {
          deliveryOrder.add("connected");
        }
      });

      final subject = "integration.$suffix.reconnect";
      final subscription = await connection.subscribe(subject);
      deliveryListener = subscription.messages.listen((message) {
        deliveryOrder.add("message");
        deliveries.add(message);
      });
      final initialDelivery = deliveries.stream.first;
      await connection.publish(subject, const [9]);
      await connection.flush();
      expect((await initialDelivery).payload, [9]);

      final cancelledSubject = "integration.$suffix.cancelled";
      final cancelled = await connection.subscribe(cancelledSubject);
      final blackholeSubject = "integration.$suffix.blackhole";
      final blackhole = await connection.subscribe(blackholeSubject);
      blackholeListener = blackhole.messages.listen(blackholeDeliveries.add);
      final requestReachedServer = blackholeDeliveries.stream.first;
      final pendingRequest = connection.request(blackholeSubject, const [
        7,
      ], timeout: const Duration(seconds: 20));
      final pendingRequestFailure = expectLater(
        pendingRequest,
        throwsA(isA<NatsConnectionException>()),
      );
      expect((await requestReachedServer).payload, [7]);

      final disconnected = connectionEvents.firstWhere(
        (event) => event is NatsDisconnected,
      );
      final reconnected = connectionEvents.firstWhere(
        (event) => event is NatsConnected && event.generation == 2,
      );
      await server.stop();
      server = null;
      await disconnected.timeout(const Duration(seconds: 5));
      await pendingRequestFailure;

      await cancelled.unsubscribe();
      final firstPayload = [0, 255, 13, 10, 1];
      await connection.publish(
        subject,
        firstPayload,
        headers: NatsHeaders(
          entries: const [
            MapEntry("Traceparent", "first"),
            MapEntry("traceparent", "second"),
          ],
        ),
      );
      firstPayload[0] = 99;
      await connection.publish(subject, const [2, 3, 4]);
      await expectLater(
        connection.publish(subject, List.filled(1024, 1)),
        throwsA(isA<NatsReconnectBufferException>()),
      );
      final retainedStatistics = connection.statistics;
      expect(retainedStatistics.reconnectRetainedMessages, 2);
      expect(retainedStatistics.reconnectRetainedBytes, greaterThan(0));

      deliveryOrder.clear();
      final retainedDeliveries = deliveries.stream.take(2).toList();
      server = await _startServer(serverBinary, environment);
      await reconnected.timeout(const Duration(seconds: 10));
      final retained = await retainedDeliveries.timeout(
        const Duration(seconds: 5),
      );

      expect(deliveryOrder, ["connected", "message", "message"]);
      expect(retained[0].payload, [0, 255, 13, 10, 1]);
      expect(retained[0].headers?.values("traceparent"), ["first", "second"]);
      expect(retained[1].payload, [2, 3, 4]);
      expect(connection.generation, 2);
      expect(connection.statistics.reconnectAttempts, greaterThanOrEqualTo(1));
      expect(connection.statistics.reconnectSuccesses, 1);
      expect(connection.statistics.reconnectRetainedMessages, 0);
      expect(connection.statistics.reconnectRetainedBytes, 0);
      expect(authenticationNonces, hasLength(2));
      expect(
        authenticationNonces.every((nonce) => nonce?.isNotEmpty ?? false),
        isTrue,
      );
      expect(authenticationNonces.toSet(), hasLength(2));

      await connection.flush();
      final serverSubscriptions = await server.subscriptionsFor(clientName);
      expect(serverSubscriptions, contains(subject));
      expect(serverSubscriptions, isNot(contains(cancelledSubject)));

      await blackhole.unsubscribe();
      await subscription.unsubscribe();
      await connection.close();
      connection = null;
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "connected event exposes a publication ready generation",
    () async {
      final environment = await _createEnvironment();
      NatsServerProcess? server;
      NatsConnection? connection;
      StreamSubscription<NatsConnectionEvent>? eventListener;
      StreamSubscription<NatsMessage>? deliveryListener;
      StreamSubscription<NatsMessage>? responderListener;
      addTearDown(() async {
        await eventListener?.cancel();
        await deliveryListener?.cancel();
        await responderListener?.cancel();
        await connection?.close();
        await server?.stop();
        await environment.directory.delete(recursive: true);
      });

      server = await _startServer(serverBinary!, environment);
      connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(server.clientUri.toString())],
          authentication: NatsAuthentication.userPassword(
            "alice",
            "wonderland",
          ),
          reconnect: NatsReconnectPolicy(
            maximumAttempts: -1,
            baseDelay: const Duration(milliseconds: 20),
            maximumDelay: const Duration(milliseconds: 50),
            jitter: 0,
            bufferBytes: 0,
          ),
        ),
      );
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final publicationSubject = "integration.$suffix.connected.publish";
      final requestSubject = "integration.$suffix.connected.request";
      final deliveries = <List<int>>[];
      final delivery = Completer<void>();
      final publicationSubscription = await connection.subscribe(
        publicationSubject,
      );
      deliveryListener = publicationSubscription.messages.listen((message) {
        deliveries.add(message.payload);
        if (!delivery.isCompleted) delivery.complete();
      });
      final responder = await connection.subscribe(requestSubject);
      responderListener = responder.messages.listen((message) {
        final replySubject = message.replySubject;
        if (replySubject != null) {
          unawaited(connection!.publish(replySubject, const [2]));
        }
      });

      final connectedActions = Completer<void>();
      eventListener = connection.events.listen((event) {
        if (event case NatsConnected(generation: 2)) {
          expect(connection!.isConnected, isTrue);
          final publication = connection.publish(publicationSubject, const [1]);
          final request = connection.request(requestSubject, const [
            7,
          ], timeout: const Duration(seconds: 5));
          final flush = connection.flush();
          unawaited(
            Future.wait([
              publication,
              request.then((message) => expect(message.payload, [2])),
              flush,
              delivery.future,
            ]).then<void>(
              (_) => connectedActions.complete(),
              onError: (Object error, StackTrace stackTrace) {
                connectedActions.completeError(error, stackTrace);
              },
            ),
          );
        }
      });

      final disconnected = connection.events.firstWhere(
        (event) => event is NatsDisconnected,
      );
      await server.stop();
      server = null;
      await disconnected.timeout(const Duration(seconds: 5));
      server = await _startServer(serverBinary, environment);
      await connectedActions.future.timeout(const Duration(seconds: 10));
      await connection.flush();

      expect(deliveries, [
        [1],
      ]);
      await responder.unsubscribe();
      await publicationSubscription.unsubscribe();
      await connection.close();
      connection = null;
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "close cancels a pending reconnect delay",
    () async {
      final environment = await _createEnvironment();
      NatsServerProcess? server;
      NatsConnection? connection;
      addTearDown(() async {
        await connection?.close();
        await server?.stop();
        await environment.directory.delete(recursive: true);
      });

      server = await _startServer(serverBinary!, environment);
      connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(server.clientUri.toString())],
          authentication: NatsAuthentication.userPassword(
            "alice",
            "wonderland",
          ),
          reconnect: NatsReconnectPolicy(
            maximumAttempts: -1,
            initialDelay: const Duration(seconds: 30),
            baseDelay: const Duration(seconds: 30),
            maximumDelay: const Duration(seconds: 30),
            jitter: 0,
          ),
        ),
      );
      final reconnecting = connection.events.firstWhere(
        (event) => event is NatsReconnecting,
      );

      await server.stop();
      server = null;
      await reconnecting.timeout(const Duration(seconds: 5));
      await connection.close().timeout(const Duration(seconds: 2));
      await connection.closed.timeout(const Duration(seconds: 2));
      expect(connection.isConnected, isFalse);
      connection = null;
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "finite reconnect attempts end in one terminal close",
    () async {
      final environment = await _createEnvironment();
      NatsServerProcess? server;
      NatsConnection? connection;
      StreamSubscription<NatsConnectionEvent>? listener;
      addTearDown(() async {
        await listener?.cancel();
        await connection?.close();
        await server?.stop();
        await environment.directory.delete(recursive: true);
      });

      server = await _startServer(serverBinary!, environment);
      connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(server.clientUri.toString())],
          authentication: NatsAuthentication.userPassword(
            "alice",
            "wonderland",
          ),
          reconnect: NatsReconnectPolicy(
            maximumAttempts: 2,
            baseDelay: const Duration(milliseconds: 20),
            maximumDelay: const Duration(milliseconds: 20),
            jitter: 0,
          ),
        ),
      );
      final events = <NatsConnectionEvent>[];
      final eventsDone = Completer<void>();
      listener = connection.events.listen(
        events.add,
        onDone: eventsDone.complete,
      );

      await server.stop();
      server = null;
      await connection.closed.timeout(const Duration(seconds: 5));
      await eventsDone.future.timeout(const Duration(seconds: 2));

      expect(events.whereType<NatsReconnecting>(), hasLength(2));
      expect(events.whereType<NatsClosed>(), hasLength(1));
      expect(events.whereType<NatsClosed>().single.error, isNotNull);
      await expectLater(
        connection.publish("closed.subject", const []),
        throwsA(isA<NatsClosedException>()),
      );
      connection = null;
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "repeated static authentication rejection closes unlimited reconnect",
    () async {
      final environment = await _createEnvironment();
      NatsServerProcess? server;
      NatsConnection? connection;
      addTearDown(() async {
        await connection?.close();
        await server?.stop();
        await environment.directory.delete(recursive: true);
      });

      server = await _startServer(serverBinary!, environment);
      connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(server.clientUri.toString())],
          authentication: NatsAuthentication.userPassword(
            "alice",
            "wonderland",
          ),
          reconnect: NatsReconnectPolicy(
            maximumAttempts: -1,
            baseDelay: const Duration(milliseconds: 20),
            maximumDelay: const Duration(milliseconds: 20),
            jitter: 0,
          ),
        ),
      );
      final closed = connection.events
          .where((event) => event is NatsClosed)
          .cast<NatsClosed>()
          .first;

      await server.stop();
      server = null;
      await environment.config.writeAsString("""
authorization {
  users: [{user: "alice", password: "rotated"}]
}
""");
      server = await _startServer(serverBinary, environment);

      final event = await closed.timeout(const Duration(seconds: 10));
      expect(
        event.error,
        isA<NatsAuthenticationException>().having(
          (error) => error.reason,
          "reason",
          NatsAuthenticationFailure.authorizationViolation,
        ),
      );
      expect(connection.statistics.reconnectAttempts, greaterThanOrEqualTo(2));
      await connection.closed.timeout(const Duration(seconds: 2));
      connection = null;
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "rotating provider can recover after repeated authentication rejection",
    () async {
      final environment = await _createEnvironment();
      NatsServerProcess? server;
      NatsConnection? connection;
      addTearDown(() async {
        await connection?.close();
        await server?.stop();
        await environment.directory.delete(recursive: true);
      });

      server = await _startServer(serverBinary!, environment);
      var providerInvocations = 0;
      connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(server.clientUri.toString())],
          authentication: NatsAuthentication.provider((_) async {
            providerInvocations += 1;
            final password =
                providerInvocations == 1 || providerInvocations >= 4
                ? "wonderland"
                : "stale";
            return NatsConnectCredentials(
              username: "alice",
              password: password,
            );
          }),
          reconnect: NatsReconnectPolicy(
            maximumAttempts: -1,
            baseDelay: const Duration(milliseconds: 20),
            maximumDelay: const Duration(milliseconds: 20),
            jitter: 0,
            retryAuthenticationFailures: true,
          ),
        ),
      );
      final reconnected = connection.events
          .where((event) => event is NatsConnected)
          .cast<NatsConnected>()
          .firstWhere((event) => event.generation == 2);

      await server.stop();
      server = await _startServer(serverBinary, environment);
      await reconnected.timeout(const Duration(seconds: 10));

      expect(providerInvocations, 4);
      expect(connection.generation, 2);
      expect(connection.statistics.reconnectAttempts, greaterThanOrEqualTo(3));
      await connection.close();
      connection = null;
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}

Future<({Directory directory, File config, int clientPort, int monitorPort})>
_createEnvironment() async {
  final directory = await Directory.systemTemp.createTemp(
    "nats-dart-reconnect-",
  );
  final config = File("${directory.path}/nats.conf");
  await config.writeAsString("""
authorization {
  users: [
    {user: "alice", password: "wonderland"},
    {nkey: "$_publicNkey"}
  ]
}
""");
  return (
    directory: directory,
    config: config,
    clientPort: await reserveLoopbackPort(),
    monitorPort: await reserveLoopbackPort(),
  );
}

Future<NatsServerProcess> _startServer(
  String binary,
  ({Directory directory, File config, int clientPort, int monitorPort})
  environment,
) {
  return NatsServerProcess.start(
    binary: binary,
    clientPort: environment.clientPort,
    monitorPort: environment.monitorPort,
    configPath: environment.config.path,
    storeDirectory: "${environment.directory.path}/jetstream",
  );
}
