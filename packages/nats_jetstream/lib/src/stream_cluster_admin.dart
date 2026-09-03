import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/identifiers.dart";
import "package:nats_jetstream/src/stream_cluster_models.dart";
import "package:nats_jetstream/src/stream_models.dart";
import "package:nats_jetstream/src/stream_wire.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// Reads one stream topology with a per request deadline.
typedef JetStreamStreamInfoRequest =
    Future<JetStreamStreamInfo> Function(String stream, Duration timeout);

/// Sends one raw leader stepdown request.
typedef JetStreamLeaderStepDownRequest =
    Future<JetStreamWireResponse> Function(
      String subject,
      Map<String, Object?> body,
      Duration timeout,
    );

/// Safe orchestration for stream leader stepdown and later convergence.
final class JetStreamStreamClusterAdmin {
  /// Creates cluster administration over one Core connection.
  factory JetStreamStreamClusterAdmin(
    NatsConnection connection,
    String apiPrefix,
    Duration requestTimeout,
  ) {
    return JetStreamStreamClusterAdmin.withRequests(
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
      requestStepDown: (subject, body, timeout) =>
          _request(connection, subject, body, timeout),
    );
  }

  /// Creates administration with explicit request boundaries for testing.
  const JetStreamStreamClusterAdmin.withRequests({
    required this.apiPrefix,
    required this.requestTimeout,
    required this.readInfo,
    required this.requestStepDown,
  });

  /// Management API prefix.
  final String apiPrefix;

  /// Maximum duration of one management request.
  final Duration requestTimeout;

  /// Stream topology read boundary.
  final JetStreamStreamInfoRequest readInfo;

  /// Leader mutation boundary.
  final JetStreamLeaderStepDownRequest requestStepDown;

  /// Sends exactly one stepdown mutation after validating current topology.
  ///
  /// A transport or timeout failure after publication is ambiguous. This method
  /// never retries the mutation. Reconcile with [readInfo] before any new
  /// mutation attempt.
  Future<JetStreamLeaderStepDownReceipt> requestLeaderStepDown(
    String stream, {
    JetStreamLeaderStepDownSelection selection = const JetStreamAnyLeader(),
  }) async {
    validateJetStreamStreamName(stream);
    final before = await readInfo(stream, requestTimeout);
    final cluster = before.cluster;
    if (cluster == null) {
      throw StateError("Stream $stream is not clustered");
    }
    final previousLeader = cluster.leader;
    if (previousLeader == null || previousLeader.isEmpty) {
      throw StateError("Stream $stream has no current leader");
    }
    final eligibleFollowers = cluster.replicas
        .where(
          (peer) =>
              peer.name != previousLeader && peer.current && !peer.offline,
        )
        .toList(growable: false);
    if (eligibleFollowers.isEmpty) {
      throw StateError("Stream $stream has no current online follower");
    }
    if (selection case JetStreamPreferredLeader(:final server)) {
      final preferred = cluster.replicas.where((peer) => peer.name == server);
      if (preferred.isEmpty) {
        throw ArgumentError.value(
          server,
          "selection",
          "Must name a replica of stream $stream",
        );
      }
      final peer = preferred.single;
      if (server == previousLeader || !peer.current || peer.offline) {
        throw StateError(
          "Preferred server $server is not a current online follower",
        );
      }
    }

    final subject = "$apiPrefix.STREAM.LEADER.STEPDOWN.$stream";
    final response = await requestStepDown(
      subject,
      selection.toJson(),
      requestTimeout,
    );
    response.throwApiError();
    if (!response.optionalBoolean("success")) {
      throw JetStreamProtocolException(
        "Leader stepdown response did not confirm success",
        subject: subject,
        fieldPath: "success",
      );
    }
    return JetStreamLeaderStepDownReceipt(
      stream: stream,
      previousLeader: previousLeader,
      previousLeaderSince: cluster.leaderSince,
      selection: selection,
    );
  }

  /// Waits for a separately accepted mutation to reach the requested leader.
  ///
  /// Read failures are safe to retry within [maximumWait]. The mutation itself
  /// is never repeated. Preferred selection converges only on the exact server.
  Future<JetStreamStreamInfo> awaitLeaderChange(
    JetStreamLeaderStepDownReceipt receipt, {
    Duration maximumWait = const Duration(seconds: 30),
    Duration pollInterval = const Duration(milliseconds: 100),
  }) async {
    _validatePositive(maximumWait, "maximumWait");
    _validatePositive(pollInterval, "pollInterval");
    final clock = Stopwatch()..start();
    JetStreamStreamInfo? lastInfo;
    Object? lastCause;
    StackTrace? lastCauseStackTrace;

    while (clock.elapsed < maximumWait) {
      final remaining = maximumWait - clock.elapsed;
      try {
        lastInfo = await readInfo(
          receipt.stream,
          _shorter(requestTimeout, remaining),
        );
        lastCause = null;
        lastCauseStackTrace = null;
        if (_hasConverged(receipt, lastInfo)) return lastInfo;
      } on Exception catch (error, stackTrace) {
        lastCause = error;
        lastCauseStackTrace = stackTrace;
      }

      final afterRead = maximumWait - clock.elapsed;
      if (afterRead <= Duration.zero) break;
      await Future<void>.delayed(_shorter(pollInterval, afterRead));
    }

    throw JetStreamLeaderConvergenceException(
      receipt: receipt,
      maximumWait: maximumWait,
      lastInfo: lastInfo,
      cause: lastCause,
      causeStackTrace: lastCauseStackTrace,
    );
  }

  /// Requests one stepdown and then waits for observed convergence.
  Future<JetStreamStreamInfo> stepDownLeader(
    String stream, {
    JetStreamLeaderStepDownSelection selection = const JetStreamAnyLeader(),
    Duration maximumWait = const Duration(seconds: 30),
    Duration pollInterval = const Duration(milliseconds: 100),
  }) async {
    final receipt = await requestLeaderStepDown(stream, selection: selection);
    return awaitLeaderChange(
      receipt,
      maximumWait: maximumWait,
      pollInterval: pollInterval,
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

bool _hasConverged(
  JetStreamLeaderStepDownReceipt receipt,
  JetStreamStreamInfo info,
) {
  final leader = info.cluster?.leader;
  if (leader == null || leader.isEmpty) return false;
  return switch (receipt.selection) {
    JetStreamPreferredLeader(:final server) => leader == server,
    JetStreamAnyLeader() ||
    JetStreamLeaderPlacement() => leader != receipt.previousLeader,
  };
}

Duration _shorter(Duration first, Duration second) =>
    first <= second ? first : second;

void _validatePositive(Duration value, String name) {
  if (value <= Duration.zero) {
    throw ArgumentError.value(value, name, "Must be positive");
  }
}
