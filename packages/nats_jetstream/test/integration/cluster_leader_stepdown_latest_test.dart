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
    "preferred stream leader stepdown preserves replicated state",
    () async {
      final cluster = await NatsClusterEnvironment.create();
      NatsConnection? connection;
      addTearDown(() async {
        printOnFailure(cluster.logs);
        await connection?.close();
        await cluster.close();
      });
      await cluster.start(serverBinary!);
      connection = await NatsConnection.connect(
        NatsOptions(
          servers: cluster.servers
              .map((server) => NatsServer.parse(server.clientUri.toString()))
              .toList(growable: false),
          name: "nats-dart-cluster-leader-stepdown",
          reconnect: NatsReconnectPolicy(bufferBytes: 0),
        ),
      );
      final jetStream = JetStreamContext(
        connection,
        timeout: const Duration(seconds: 10),
      );
      await eventually(
        jetStream.accountInfo,
        (info) => info.apiLevel >= 4,
        "JetStream meta leader did not become ready",
      );

      const stream = "STEPDOWN";
      const subject = "stepdown.events";
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: stream,
          subjects: const [subject],
          replicas: 3,
        ),
      );
      for (var sequence = 1; sequence <= 50; sequence += 1) {
        await jetStream.publish(subject, [sequence]);
      }
      final before = await eventually(
        () => jetStream.streamInfo(stream),
        streamReplicasCurrent,
        "Stream replicas did not become current",
      );
      final previousLeader = before.cluster!.leader!;
      final preferred = before.cluster!.replicas.first.name;

      final receipt = await jetStream.requestStreamLeaderStepDown(
        stream,
        selection: JetStreamPreferredLeader(preferred),
      );
      expect(receipt.previousLeader, previousLeader);
      expect(receipt.selection, isA<JetStreamPreferredLeader>());
      final changed = await jetStream.awaitStreamLeaderChange(
        receipt,
        maximumWait: const Duration(seconds: 15),
        pollInterval: const Duration(milliseconds: 20),
      );
      expect(changed.cluster?.leader, preferred);
      final after = await eventually(
        () => jetStream.streamInfo(stream),
        (info) =>
            info.cluster?.leader == preferred && streamReplicasCurrent(info),
        "Preferred leader replicas did not become current",
        timeout: const Duration(seconds: 30),
      );
      expect(after.state.messages, before.state.messages);
      expect(after.state.bytes, before.state.bytes);
      expect(after.state.firstSequence, before.state.firstSequence);
      expect(after.state.lastSequence, before.state.lastSequence);

      final finalAck = await jetStream.publish(subject, const [51]);
      expect(finalAck.sequence, BigInt.from(51));
      final stored = await jetStream.getMessage(stream, BigInt.from(51));
      expect(stored.payload, [51]);
      await jetStream.deleteStream(stream);
    },
    skip: skipReason,
    timeout: const Timeout(Duration(minutes: 1)),
  );
}
