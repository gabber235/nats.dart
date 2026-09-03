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
    "finite pull fails after two missed server heartbeat intervals",
    () async {
      final directory = await Directory.systemTemp.createTemp(
        "nats-dart-fetch-heartbeat-",
      );
      final config = File("${directory.path}/nats.conf");
      await config.writeAsString("# Isolated finite pull heartbeat test\n");
      final server = await NatsServerProcess.start(
        binary: serverBinary!,
        clientPort: await reserveLoopbackPort(),
        monitorPort: await reserveLoopbackPort(),
        configPath: config.path,
        storeDirectory: "${directory.path}/jetstream",
      );
      var paused = false;
      NatsConnection? connection;
      addTearDown(() async {
        if (paused) {
          server.resume();
        }
        await connection?.close();
        await server.stop();
        await directory.delete(recursive: true);
      });

      connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(server.clientUri.toString())],
          name: "nats-dart-fetch-heartbeat",
          pingInterval: const Duration(seconds: 30),
        ),
      );
      final jetStream = JetStreamContext(connection);
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: "HEARTBEAT",
          subjects: const ["heartbeat.events"],
          storage: JetStreamStorage.memory,
        ),
      );
      await jetStream.consumers.createPull(
        "HEARTBEAT",
        JetStreamPullConsumerConfig(name: "WORKER"),
      );
      final consumer = await jetStream.consumers.pull("HEARTBEAT", "WORKER");
      final fetch = consumer.fetch(
        JetStreamFetchRequest(
          maximumMessages: 1,
          expires: const Duration(seconds: 3),
          idleHeartbeat: const Duration(milliseconds: 100),
        ),
      );
      await Stream<void>.periodic(const Duration(milliseconds: 10))
          .asyncMap((_) => jetStream.consumers.info("HEARTBEAT", "WORKER"))
          .firstWhere((info) => info.waiting == BigInt.one)
          .timeout(const Duration(seconds: 3));

      final elapsed = Stopwatch()..start();
      server.pause();
      paused = true;
      late final JetStreamFetchException failure;
      try {
        await fetch;
        fail("Finite pull completed while the server was suspended");
      } on JetStreamFetchException catch (error) {
        failure = error;
      }
      elapsed.stop();

      expect(failure.cause, isA<JetStreamConsumerHeartbeatException>());
      expect(failure.partialMessages, isEmpty);
      expect(failure.ambiguous, isTrue);
      expect(elapsed.elapsed, lessThan(const Duration(seconds: 2)));
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 20)),
  );
}
