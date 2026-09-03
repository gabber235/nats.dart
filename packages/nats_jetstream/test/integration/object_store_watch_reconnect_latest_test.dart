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
    "Object Store watch resumes at the next metadata revision after restart",
    () async {
      final environment = await _createEnvironment();
      NatsServerProcess? server;
      NatsConnection? connection;
      JetStreamObjectWatch? watch;
      StreamIterator<JetStreamObjectWatchEvent>? events;
      addTearDown(() async {
        await watch?.close();
        await events?.cancel();
        await connection?.close();
        await server?.stop();
        await environment.directory.delete(recursive: true);
      });

      server = await _startServer(serverBinary!, environment);
      connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(server.clientUri.toString())],
          name: "nats-dart-object-watch-reconnect",
          reconnect: NatsReconnectPolicy(
            maximumAttempts: -1,
            baseDelay: const Duration(milliseconds: 20),
            maximumDelay: const Duration(milliseconds: 50),
            jitter: 0,
          ),
        ),
      );
      final jetStream = JetStreamContext(connection);
      final bucket = await jetStream.objectStore.createStore(
        JetStreamObjectStoreConfig(name: "OBJECT_RECONNECT"),
      );
      watch = bucket.watch(
        options: JetStreamObjectWatchOptions(
          start: const JetStreamObjectWatchStart.updates(),
          heartbeat: const Duration(milliseconds: 250),
        ),
      );
      events = StreamIterator(watch.events);
      expect(await events.moveNext(), isTrue);
      expect(events.current, isA<JetStreamObjectInitialStateComplete>());

      final before = await bucket.putBytes("state", const [1]);
      expect(await events.moveNext(), isTrue);
      final beforeEvent = events.current as JetStreamObjectChanged;
      expect(beforeEvent.info.revision, before.revision);
      expect(beforeEvent.info.name, "state");

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
      final after = await bucket.putBytes("state", const [2]);
      expect(after.revision, greaterThan(before.revision));
      expect(await events.moveNext(), isTrue);
      final afterEvent = events.current as JetStreamObjectChanged;
      expect(afterEvent.info.revision, after.revision);
      expect(afterEvent.info.name, "state");
      expect(
        (await jetStream.consumers.listPage("OBJ_OBJECT_RECONNECT")).total,
        BigInt.one,
      );

      await watch.close();
      await events.cancel();
      await watch.done;
      watch = null;
      events = null;
      expect(
        (await jetStream.consumers.listPage("OBJ_OBJECT_RECONNECT")).total,
        BigInt.zero,
      );
      await jetStream.objectStore.deleteStore("OBJECT_RECONNECT");
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
    "nats-dart-object-watch-reconnect-",
  );
  final config = File("${directory.path}/nats.conf");
  await config.writeAsString("# Isolated Object Store restart test\n");
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
