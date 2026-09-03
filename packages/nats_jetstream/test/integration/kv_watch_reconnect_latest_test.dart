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
    "KV watch resumes from the next stream revision after restart",
    () async {
      final environment = await _createEnvironment();
      NatsServerProcess? server;
      NatsConnection? connection;
      JetStreamKeyValueWatch? watch;
      StreamIterator<JetStreamKeyValueWatchEvent>? events;
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
          name: "nats-dart-kv-watch-reconnect",
          reconnect: NatsReconnectPolicy(
            maximumAttempts: -1,
            baseDelay: Duration(milliseconds: 20),
            maximumDelay: Duration(milliseconds: 50),
            jitter: 0,
          ),
        ),
      );
      final jetStream = JetStreamContext(connection);
      final bucket = await jetStream.keyValue.createBucket(
        JetStreamKeyValueBucketConfig(name: "RECONNECT"),
      );
      watch = bucket.watch(
        options: JetStreamKeyValueWatchOptions(
          start: const JetStreamKeyValueWatchStart.updates(),
          heartbeat: const Duration(milliseconds: 250),
        ),
      );
      events = StreamIterator(watch.events);
      expect(await events.moveNext(), isTrue);
      expect(events.current, isA<JetStreamKeyValueInitialStateComplete>());

      final beforeRevision = await bucket.put("state", const [1]);
      expect(await events.moveNext(), isTrue);
      final before = events.current as JetStreamKeyValueChanged;
      expect(before.entry.revision, beforeRevision);
      expect(before.entry.value, [1]);

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
      final afterRevision = await bucket.put("state", const [2]);
      expect(afterRevision, beforeRevision + BigInt.one);
      expect(await events.moveNext(), isTrue);
      final after = events.current as JetStreamKeyValueChanged;
      expect(after.entry.revision, afterRevision);
      expect(after.entry.value, [2]);

      await watch.close();
      await events.cancel();
      await watch.done;
      watch = null;
      events = null;
      expect(
        (await jetStream.consumers.listPage("KV_RECONNECT")).total,
        BigInt.zero,
      );
      await jetStream.keyValue.deleteBucket("RECONNECT");
      await connection.close();
      connection = null;
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "KV watch close during outage deletes its consumer after reconnect",
    () async {
      final environment = await _createEnvironment();
      NatsServerProcess? server;
      NatsConnection? connection;
      JetStreamKeyValueWatch? watch;
      StreamIterator<JetStreamKeyValueWatchEvent>? events;
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
          name: "nats-dart-kv-watch-close-reconnect",
          reconnect: NatsReconnectPolicy(
            maximumAttempts: -1,
            baseDelay: const Duration(milliseconds: 20),
            maximumDelay: const Duration(milliseconds: 50),
            jitter: 0,
          ),
        ),
      );
      final jetStream = JetStreamContext(connection);
      final bucket = await jetStream.keyValue.createBucket(
        JetStreamKeyValueBucketConfig(name: "CLOSE_RECONNECT"),
      );
      watch = bucket.watch(
        options: JetStreamKeyValueWatchOptions(
          start: const JetStreamKeyValueWatchStart.updates(),
          heartbeat: const Duration(milliseconds: 250),
        ),
      );
      events = StreamIterator(watch.events);
      expect(await events.moveNext(), isTrue);
      expect(events.current, isA<JetStreamKeyValueInitialStateComplete>());
      expect(
        (await jetStream.consumers.listPage("KV_CLOSE_RECONNECT")).total,
        BigInt.one,
      );

      final disconnected = connection.events.firstWhere(
        (event) => event is NatsDisconnected,
      );
      final reconnected = connection.events.firstWhere(
        (event) => event is NatsConnected && event.generation == 2,
      );
      await server.stop();
      server = null;
      await disconnected.timeout(const Duration(seconds: 5));

      final closed = watch.close();
      server = await _startServer(serverBinary, environment);
      await reconnected.timeout(const Duration(seconds: 10));
      await closed.timeout(const Duration(seconds: 5));
      await events.cancel();
      await watch.done;
      watch = null;
      events = null;
      expect(
        (await jetStream.consumers.listPage("KV_CLOSE_RECONNECT")).total,
        BigInt.zero,
      );

      await jetStream.keyValue.deleteBucket("CLOSE_RECONNECT");
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
    "nats-dart-kv-watch-reconnect-",
  );
  final config = File("${directory.path}/nats.conf");
  await config.writeAsString("# Isolated KV watch restart test\n");
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
