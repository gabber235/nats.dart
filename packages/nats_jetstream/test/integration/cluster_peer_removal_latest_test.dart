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
    "peer removal distinguishes replacement from partial reduction",
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
          name: "nats-dart-cluster-peer-removal",
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

      const replacementStream = "REPLACEMENT";
      const replacementSubject = "replacement.events";
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: replacementStream,
          subjects: const [replacementSubject],
          replicas: 2,
        ),
      );
      for (var sequence = 1; sequence <= 10; sequence += 1) {
        await jetStream.publish(replacementSubject, [sequence]);
      }
      final beforeReplacement = await eventually(
        () => jetStream.streamInfo(replacementStream),
        _twoReplicasCurrent,
        "R2 stream did not become current",
      );
      final removedFollower = beforeReplacement.cluster!.replicas.single.name;
      final spare = {
        "A",
        "B",
        "C",
      }.difference(_members(beforeReplacement)).single;
      final replacement = await jetStream.removeStreamPeer(
        replacementStream,
        removedFollower,
        maximumWait: const Duration(seconds: 20),
        pollInterval: const Duration(milliseconds: 20),
      );

      expect(
        replacement.classification,
        JetStreamPeerRemovalClassification.replaced,
      );
      expect(replacement.serverReportedSuccess, isTrue);
      expect(_members(replacement.lastInfo), contains(spare));
      expect(_members(replacement.lastInfo), isNot(contains(removedFollower)));
      expect(
        replacement.lastInfo.state.messages,
        beforeReplacement.state.messages,
      );
      expect(replacement.lastInfo.state.bytes, beforeReplacement.state.bytes);
      expect(
        (await jetStream.publish(replacementSubject, const [11])).sequence,
        BigInt.from(11),
      );

      const reducedStream = "REDUCED";
      const reducedSubject = "reduced.events";
      await jetStream.createStream(
        JetStreamStreamConfig(
          name: reducedStream,
          subjects: const [reducedSubject],
          replicas: 3,
        ),
      );
      await jetStream.publish(reducedSubject, const [1]);
      final beforeReduction = await eventually(
        () => jetStream.streamInfo(reducedStream),
        streamReplicasCurrent,
        "R3 stream did not become current",
      );
      final reducedFollower = beforeReduction.cluster!.replicas.first.name;
      final reduction = await jetStream.removeStreamPeer(
        reducedStream,
        reducedFollower,
        maximumWait: const Duration(seconds: 20),
        pollInterval: const Duration(milliseconds: 20),
      );

      expect(
        reduction.classification,
        JetStreamPeerRemovalClassification.removedWithoutReplacement,
      );
      expect(reduction.serverReportedSuccess, isFalse);
      expect(reduction.mutationError?.errorCode, 10075);
      expect(_members(reduction.lastInfo), hasLength(2));
      expect(_members(reduction.lastInfo), isNot(contains(reducedFollower)));
      expect(reduction.lastInfo.state.messages, BigInt.one);
      expect(
        (await jetStream.publish(reducedSubject, const [2])).sequence,
        BigInt.two,
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(minutes: 1)),
  );
}

bool _twoReplicasCurrent(JetStreamStreamInfo info) {
  final cluster = info.cluster;
  return cluster?.leader != null &&
      cluster!.replicas.length == 1 &&
      cluster.replicas.single.current &&
      !cluster.replicas.single.offline &&
      cluster.replicas.single.lag == BigInt.zero;
}

Set<String> _members(JetStreamStreamInfo info) => {
  info.cluster!.leader!,
  ...info.cluster!.replicas.map((peer) => peer.name),
};
