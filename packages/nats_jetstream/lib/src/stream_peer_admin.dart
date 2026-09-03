import "dart:async";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/identifiers.dart";
import "package:nats_jetstream/src/stream_cluster_admin.dart";
import "package:nats_jetstream/src/stream_models.dart";
import "package:nats_jetstream/src/stream_peer_models.dart";
import "package:nats_jetstream/src/stream_wire.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// Sends one raw stream peer removal request.
typedef JetStreamPeerRemovalRequest =
    Future<JetStreamWireResponse> Function(
      String subject,
      Map<String, Object?> body,
      Duration timeout,
    );

/// Safe orchestration for one stream peer removal and topology reconciliation.
final class JetStreamStreamPeerAdmin {
  /// Creates peer administration over one Core connection.
  factory JetStreamStreamPeerAdmin(
    NatsConnection connection,
    String apiPrefix,
    Duration requestTimeout,
  ) {
    return JetStreamStreamPeerAdmin.withRequests(
      apiPrefix: apiPrefix,
      requestTimeout: requestTimeout,
      readInfo: (stream, timeout) async {
        final subject = "$apiPrefix.STREAM.INFO.$stream";
        final response = await _request(connection, subject, const {}, timeout);
        response.throwApiError();
        return requireStreamIdentity(
          response,
          decodeStreamInfo(response),
          stream,
        );
      },
      requestRemoval: (subject, body, timeout) =>
          _request(connection, subject, body, timeout),
    );
  }

  /// Creates peer administration with explicit request boundaries for testing.
  const JetStreamStreamPeerAdmin.withRequests({
    required this.apiPrefix,
    required this.requestTimeout,
    required this.readInfo,
    required this.requestRemoval,
  });

  /// Management API prefix.
  final String apiPrefix;

  /// Maximum duration of one management request.
  final Duration requestTimeout;

  /// Stream topology read boundary.
  final JetStreamStreamInfoRequest readInfo;

  /// Peer mutation boundary.
  final JetStreamPeerRemovalRequest requestRemoval;

  /// Removes one named server from a stream assignment and reconciles topology.
  ///
  /// The mutation is sent exactly once. A `10075` response or Core transport
  /// failure can follow a partial server mutation, so this method preserves the
  /// cause and performs only safe information reads until [maximumWait].
  Future<JetStreamPeerRemovalResult> removePeer(
    String stream,
    String server, {
    JetStreamLeaderRemoval leaderRemoval = JetStreamLeaderRemoval.refuse,
    Duration maximumWait = const Duration(seconds: 30),
    Duration pollInterval = const Duration(milliseconds: 100),
  }) async {
    validateJetStreamStreamName(stream);
    _validateServerName(server);
    _validatePositive(maximumWait, "maximumWait");
    _validatePositive(pollInterval, "pollInterval");

    final before = await readInfo(stream, requestTimeout);
    final cluster = before.cluster;
    if (cluster == null) {
      throw StateError("Stream $stream is not clustered");
    }
    final members = _memberNames(before);
    if (!members.contains(server)) {
      throw ArgumentError.value(
        server,
        "server",
        "Must name a member of stream $stream",
      );
    }
    if (cluster.leader == server &&
        leaderRemoval == JetStreamLeaderRemoval.refuse) {
      throw StateError(
        "Server $server is the current leader of stream $stream",
      );
    }

    final subject = "$apiPrefix.STREAM.PEER.REMOVE.$stream";
    final mutation = await _mutateOnce(subject, server);
    return _reconcile(
      stream: stream,
      server: server,
      expectedReplicas: before.config.replicas,
      before: before,
      mutation: mutation,
      maximumWait: maximumWait,
      pollInterval: pollInterval,
    );
  }

  Future<JetStreamPeerRemovalMutationResult> _mutateOnce(
    String subject,
    String server,
  ) async {
    try {
      final response = await requestRemoval(subject, {
        "peer": server,
      }, requestTimeout);
      try {
        response.throwApiError();
      } on JetStreamApiException catch (error) {
        if (error.errorCode != 10075) rethrow;
        return JetStreamPeerRemovalRemapFailed(error);
      }
      if (!response.optionalBoolean("success")) {
        throw JetStreamProtocolException(
          "Peer removal response did not confirm success",
          subject: subject,
          fieldPath: "success",
        );
      }
      return const JetStreamPeerRemovalAccepted();
    } on NatsException catch (error, stackTrace) {
      return JetStreamPeerRemovalAmbiguous(error, stackTrace);
    } on TimeoutException catch (error, stackTrace) {
      return JetStreamPeerRemovalAmbiguous(error, stackTrace);
    }
  }

  Future<JetStreamPeerRemovalResult> _reconcile({
    required String stream,
    required String server,
    required int expectedReplicas,
    required JetStreamStreamInfo before,
    required JetStreamPeerRemovalMutationResult mutation,
    required Duration maximumWait,
    required Duration pollInterval,
  }) async {
    final clock = Stopwatch()..start();
    var lastInfo = before;
    Object? lastCause;
    StackTrace? lastCauseStackTrace;

    while (clock.elapsed < maximumWait) {
      final remaining = maximumWait - clock.elapsed;
      try {
        lastInfo = await readInfo(stream, _shorter(requestTimeout, remaining));
        lastCause = null;
        lastCauseStackTrace = null;
        final classification = _classify(lastInfo, server, expectedReplicas);
        final reductionIsConclusive =
            classification ==
                JetStreamPeerRemovalClassification.removedWithoutReplacement &&
            mutation is JetStreamPeerRemovalRemapFailed;
        if (classification == JetStreamPeerRemovalClassification.replaced ||
            reductionIsConclusive) {
          return _result(
            stream: stream,
            server: server,
            classification: classification,
            mutation: mutation,
            before: before,
            lastInfo: lastInfo,
            maximumWait: maximumWait,
          );
        }
      } on Exception catch (error, stackTrace) {
        lastCause = error;
        lastCauseStackTrace = stackTrace;
      }

      final afterRead = maximumWait - clock.elapsed;
      if (afterRead <= Duration.zero) break;
      await Future<Object?>.delayed(_shorter(pollInterval, afterRead));
    }

    return JetStreamPeerRemovalResult(
      stream: stream,
      removedServer: server,
      classification: _classify(lastInfo, server, expectedReplicas),
      mutation: mutation,
      before: before,
      lastInfo: lastInfo,
      maximumWait: maximumWait,
      reconciliationCause: lastCause,
      reconciliationCauseStackTrace: lastCauseStackTrace,
    );
  }
}

Future<JetStreamWireResponse> _request(
  NatsConnection connection,
  String subject,
  Map<String, Object?> body,
  Duration timeout,
) async {
  final message = await connection.request(
    subject,
    encodeExactJson(body),
    timeout: timeout,
  );
  return JetStreamWireResponse.decode(message.payload, subject);
}

JetStreamPeerRemovalClassification _classify(
  JetStreamStreamInfo info,
  String server,
  int expectedReplicas,
) {
  final members = _memberNames(info);
  if (members.contains(server)) {
    return JetStreamPeerRemovalClassification.unknown;
  }
  final cluster = info.cluster;
  final replacementIsCurrent =
      cluster?.leader != null &&
      cluster!.leader!.isNotEmpty &&
      cluster.replicas.every((peer) => peer.current && !peer.offline);
  if (members.length == expectedReplicas && replacementIsCurrent) {
    return JetStreamPeerRemovalClassification.replaced;
  }
  if (members.length < expectedReplicas) {
    return JetStreamPeerRemovalClassification.removedWithoutReplacement;
  }
  return JetStreamPeerRemovalClassification.unknown;
}

Set<String> _memberNames(JetStreamStreamInfo info) => {
  if (info.cluster?.leader case final String leader when leader.isNotEmpty)
    leader,
  ...?info.cluster?.replicas.map((peer) => peer.name),
};

JetStreamPeerRemovalResult _result({
  required String stream,
  required String server,
  required JetStreamPeerRemovalClassification classification,
  required JetStreamPeerRemovalMutationResult mutation,
  required JetStreamStreamInfo before,
  required JetStreamStreamInfo lastInfo,
  required Duration maximumWait,
}) => JetStreamPeerRemovalResult(
  stream: stream,
  removedServer: server,
  classification: classification,
  mutation: mutation,
  before: before,
  lastInfo: lastInfo,
  maximumWait: maximumWait,
  reconciliationCause: null,
  reconciliationCauseStackTrace: null,
);

Duration _shorter(Duration first, Duration second) =>
    first <= second ? first : second;

void _validatePositive(Duration value, String name) {
  if (value <= Duration.zero) {
    throw ArgumentError.value(value, name, "Must be positive");
  }
}

void _validateServerName(String value) {
  if (value.isEmpty || value.trim() != value) {
    throw ArgumentError.value(value, "server", "Must be nonempty and trimmed");
  }
}
