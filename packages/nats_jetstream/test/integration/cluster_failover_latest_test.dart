@TestOn("vm")
library;

import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

import "../support/nats_cluster_environment.dart";

void main() {
  final serverBinary = Platform.environment["NATS_SERVER_BINARY"];
  final skipReason = serverBinary == null
      ? "NATS_SERVER_BINARY is not configured"
      : false;

  test(
    "deduplicated asynchronous publish survives stream leader failure",
    () async {
      final cluster = await NatsClusterEnvironment.create();
      final running = cluster.running;
      NatsConnection? managementConnection;
      NatsConnection? publishConnection;
      JetStreamAsyncPublisher? publisher;
      String? pausedServer;
      addTearDown(() async {
        printOnFailure(cluster.logs);
        if (pausedServer case final name?) {
          running[name]?.resume();
          pausedServer = null;
        }
        await publishConnection?.close();
        try {
          await publisher?.close().timeout(const Duration(seconds: 5));
        } on Object {
          // A failed assertion may leave the publisher incomplete.
        }
        await managementConnection?.close();
        await cluster.close();
      });

      await cluster.start(serverBinary!);
      managementConnection = await NatsConnection.connect(
        NatsOptions(
          servers: cluster.servers
              .map((server) => NatsServer.parse(server.clientUri.toString()))
              .toList(growable: false),
          name: "nats-dart-cluster-management",
          reconnect: NatsReconnectPolicy(bufferBytes: 0),
        ),
      );
      var management = JetStreamContext(
        managementConnection,
        timeout: const Duration(seconds: 10),
      );
      await eventually(
        () => management.accountInfo(),
        (info) => info.apiLevel >= 4,
        "JetStream meta leader did not become ready",
      );

      const stream = "CHAOS";
      const subject = "chaos.events";
      await management.createStream(
        JetStreamStreamConfig(
          name: stream,
          subjects: const [subject],
          replicas: 3,
          duplicateWindow: JetStreamNanoseconds.fromDuration(
            const Duration(minutes: 2),
          ),
        ),
      );
      final initial = await eventually(
        () => management.streamInfo(stream),
        streamReplicasCurrent,
        "Stream replicas did not become current",
      );
      final leaderName = initial.cluster!.leader!;
      final leader = cluster.byName(leaderName);

      await managementConnection.close();
      managementConnection = null;
      final publisherOptions = NatsOptions(
        servers: [NatsServer.parse(leader.clientUri.toString())],
        name: "nats-dart-cluster-publisher",
        reconnect: NatsReconnectPolicy(
          maximumAttempts: -1,
          baseDelay: const Duration(milliseconds: 20),
          maximumDelay: const Duration(milliseconds: 200),
          jitter: 0,
          bufferBytes: 0,
        ),
      );
      final connectedPublisher = await eventually(
        () => NatsConnection.connect(publisherOptions),
        (_) => true,
        "Could not connect publisher to elected stream leader $leaderName",
        timeout: const Duration(seconds: 5),
      );
      publishConnection = connectedPublisher;
      final firstGeneration = connectedPublisher.generation;
      final jetStream = JetStreamContext(
        connectedPublisher,
        timeout: const Duration(seconds: 10),
      );
      publisher = jetStream.createAsyncPublisher(
        options: JetStreamAsyncPublisherOptions(
          maximumPendingMessages: 2000,
          maximumPendingBytes: 16 * 1024 * 1024,
          acknowledgementTimeout: const Duration(seconds: 30),
          attemptTimeout: const Duration(seconds: 2),
          retryPolicy: JetStreamAsyncPublishRetryPolicy.deduplicated,
          maximumAttempts: 100,
          retryDelay: const Duration(milliseconds: 100),
        ),
      );
      running[leaderName]!.pause();
      pausedServer = leaderName;
      final payload = List<int>.filled(4096, 7);
      final pending = List.generate(
        2000,
        (index) => publisher!.publish(
          subject,
          payload,
          options: JetStreamPublishOptions(
            messageId: "cluster-message-$index",
            expectedStream: stream,
          ),
        ),
      );
      await eventually(
        () async => pending.where((item) => item.attempts > 0).length,
        (attempted) => attempted > 0 && publisher!.pendingMessages > 0,
        "No publication attempt became active",
      );

      await running[leaderName]!.crash();
      running.remove(leaderName);
      pausedServer = null;
      await eventually(
        () async => publishConnection!.generation,
        (generation) => generation > firstGeneration,
        "Client did not reconnect after leader failure",
      );

      final acknowledgements = await Future.wait(
        pending.map((item) => item.acknowledgement),
      ).timeout(const Duration(seconds: 30));
      await publisher.drain().timeout(const Duration(seconds: 30));
      expect(publisher.pendingMessages, 0);
      expect(publisher.pendingBytes, 0);
      expect(pending.any((item) => item.attempts > 1), isTrue);
      expect(
        acknowledgements.map((ack) => ack.sequence).toSet(),
        hasLength(pending.length),
      );

      management = jetStream;
      final afterFailure = await eventually(
        () => management.streamInfo(stream),
        (info) =>
            info.state.messages == BigInt.from(pending.length) &&
            info.cluster?.leader != null,
        "Acknowledged publications were not all stored",
      );
      expect(afterFailure.state.messages, BigInt.from(2000));

      running[leaderName] = await leader.start(serverBinary);
      final healed = await eventually(
        () => management.streamInfo(stream),
        streamReplicasCurrent,
        "Restarted stream replica did not catch up",
        timeout: const Duration(seconds: 30),
      );
      expect(healed.state.messages, BigInt.from(2000));
    },
    skip: skipReason,
    timeout: const Timeout(Duration(minutes: 2)),
  );
}
