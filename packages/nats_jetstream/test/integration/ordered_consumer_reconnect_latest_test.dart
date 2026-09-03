@TestOn("vm")
library;

import "dart:async";
import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

import "../../../nats_core/test/support/nats_server_process.dart";

void main() {
  final serverBinary = Platform.environment["NATS_SERVER_BINARY"];
  final skipReason = serverBinary == null
      ? "NATS_SERVER_BINARY is not configured"
      : false;

  test(
    "ordered delivery recreates from the next sequence after reconnect",
    () async {
      final environment = await _createEnvironment();
      NatsServerProcess? server;
      NatsConnection? connection;
      JetStreamOrderedConsumerStream? ordered;
      StreamIterator<JetStreamDelivery>? messages;
      addTearDown(() async {
        await ordered?.stop();
        await messages?.cancel();
        await connection?.close();
        await server?.stop();
        await environment.directory.delete(recursive: true);
      });

      server = await _startServer(serverBinary!, environment);
      connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(server.clientUri.toString())],
          name: "nats-dart-ordered-reconnect",
          reconnect: NatsReconnectPolicy(
            maximumAttempts: -1,
            baseDelay: const Duration(milliseconds: 20),
            maximumDelay: const Duration(milliseconds: 50),
            jitter: 0,
          ),
        ),
      );
      final jetStream = JetStreamContext(
        connection,
        timeout: const Duration(seconds: 20),
      );
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: "ORDERED_RECONNECT",
          subjects: const ["ordered.reconnect"],
        ),
      );
      await jetStream.publish("ordered.reconnect", const [1]);

      ordered = jetStream.consumers
          .ordered("ORDERED_RECONNECT")
          .consume(
            options: JetStreamConsumeOptions(
              batchSize: 1,
              expires: const Duration(seconds: 2),
              idleHeartbeat: const Duration(milliseconds: 250),
              maximumPendingMessages: 2,
              maximumPendingBytes: 1024,
            ),
          );
      final recreations = <JetStreamOrderedConsumerRecreated>[];
      final eventSubscription = ordered.events.listen((event) {
        if (event is JetStreamOrderedConsumerRecreated) {
          recreations.add(event);
        }
      });
      addTearDown(eventSubscription.cancel);
      messages = StreamIterator(ordered.messages);
      expect(await messages.moveNext(), isTrue);
      expect(messages.current.message.payload, [1]);

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
      await jetStream.publish("ordered.reconnect", const [2]);
      expect(await messages.moveNext(), isTrue);
      expect(messages.current.message.payload, [2]);
      await _awaitCondition(
        () => recreations.isNotEmpty,
        const Duration(seconds: 5),
      );
      expect(recreations, hasLength(1));
      expect(
        recreations.single.reason,
        JetStreamOrderedConsumerRecreationReason.connectionGenerationChanged,
      );
      expect(recreations.single.nextStreamSequence, BigInt.two);

      await ordered.stop().timeout(const Duration(seconds: 5));
      await messages.cancel();
      await ordered.done;
      ordered = null;
      messages = null;
      expect(
        (await jetStream.consumers.listPage("ORDERED_RECONNECT")).total,
        BigInt.zero,
      );
      await jetStream.deleteStream("ORDERED_RECONNECT");
      await connection.close();
      connection = null;
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "paused ordered delivery terminates after permanent connection close",
    () async {
      final environment = await _createEnvironment();
      NatsServerProcess? server;
      NatsConnection? connection;
      JetStreamOrderedConsumerStream? ordered;
      StreamIterator<JetStreamDelivery>? messages;
      addTearDown(() async {
        try {
          await ordered?.stop();
        } on NatsException {
          // The asserted terminal connection failure remains primary.
        }
        try {
          await messages?.cancel();
        } on NatsException {
          // The asserted terminal connection failure remains primary.
        }
        await connection?.close();
        await server?.stop();
        await environment.directory.delete(recursive: true);
      });

      server = await _startServer(serverBinary!, environment);
      connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(server.clientUri.toString())],
          name: "nats-dart-ordered-close-paused",
        ),
      );
      final jetStream = JetStreamContext(
        connection,
        timeout: const Duration(seconds: 20),
      );
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: "ORDERED_CLOSE",
          subjects: const ["ordered.close"],
        ),
      );
      await jetStream.publish("ordered.close", const [1]);

      ordered = jetStream.consumers
          .ordered(
            "ORDERED_CLOSE",
            options: JetStreamOrderedConsumerOptions(
              recoveryTimeout: const Duration(seconds: 2),
            ),
          )
          .consume(
            options: JetStreamConsumeOptions(
              batchSize: 1,
              expires: const Duration(seconds: 2),
              idleHeartbeat: const Duration(milliseconds: 250),
              maximumPendingMessages: 2,
              maximumPendingBytes: 1024,
            ),
          );
      messages = StreamIterator(ordered.messages);
      expect(await messages.moveNext(), isTrue);
      expect(messages.current.message.payload, [1]);

      final consumers = await jetStream.consumers.listPage("ORDERED_CLOSE");
      expect(consumers.total, BigInt.one);
      final paused = await jetStream.consumers.info(
        "ORDERED_CLOSE",
        consumers.consumers.single.name,
      );
      expect(paused.waiting, BigInt.zero);

      await connection.close();
      connection = null;
      await expectLater(
        ordered.done.timeout(const Duration(seconds: 5)),
        throwsA(isA<NatsException>()),
      );
      await expectLater(messages.moveNext(), throwsA(isA<NatsException>()));
      ordered = null;
      messages = null;
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 20)),
  );
}

Future<({Directory directory, File config, int clientPort, int monitorPort})>
_createEnvironment() async {
  final directory = await Directory.systemTemp.createTemp(
    "nats-dart-ordered-reconnect-",
  );
  final config = File("${directory.path}/nats.conf");
  await config.writeAsString("# Isolated ordered consumer restart test\n");
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

Future<void> _awaitCondition(
  bool Function() condition,
  Duration timeout,
) async {
  final deadline = DateTime.timestamp().add(timeout);
  while (!condition()) {
    if (DateTime.timestamp().isAfter(deadline)) {
      throw TimeoutException("Ordered consumer condition timed out", timeout);
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}
