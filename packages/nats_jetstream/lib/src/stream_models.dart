import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/stream_config.dart";
import "package:nats_jetstream/src/stream_source.dart";

/// Current stream storage state.
final class JetStreamStreamState {
  /// Creates an immutable state snapshot.
  JetStreamStreamState({
    required this.messages,
    required this.bytes,
    required this.firstSequence,
    required this.firstTimestamp,
    required this.lastSequence,
    required this.lastTimestamp,
    required this.consumerCount,
    required List<BigInt> deleted,
    required this.deletedCount,
    required this.subjectCount,
    required Map<String, BigInt> subjects,
    required Map<String, Object?> raw,
  }) : deleted = List.unmodifiable(deleted),
       subjects = Map.unmodifiable(subjects),
       raw = immutableExactJsonObject(raw);

  /// Stored message count.
  final BigInt messages;

  /// Stored byte count.
  final BigInt bytes;

  /// First retained sequence.
  final BigInt firstSequence;

  /// First retained message timestamp.
  final String firstTimestamp;

  /// Last assigned sequence.
  final BigInt lastSequence;

  /// Last assigned message timestamp.
  final String lastTimestamp;

  /// Exact current consumer count.
  final BigInt consumerCount;

  /// Immutable deleted sequence gaps when requested.
  final List<BigInt> deleted;

  /// Number of deleted sequence gaps.
  final BigInt deletedCount;

  /// Number of subjects with retained messages.
  final BigInt subjectCount;

  /// Immutable per subject message counts when requested.
  final Map<String, BigInt> subjects;

  /// Deeply immutable exact server state including unknown fields.
  final Map<String, Object?> raw;
}

/// One stream replica state.
final class JetStreamPeerInfo {
  /// Creates an immutable peer snapshot.
  JetStreamPeerInfo({
    required this.name,
    required this.current,
    required this.offline,
    required this.activeNanoseconds,
    required this.lag,
    required Map<String, Object?> raw,
  }) : raw = immutableExactJsonObject(raw);

  /// Server name.
  final String name;

  /// Whether the replica is current.
  final bool current;

  /// Whether the replica is offline.
  final bool offline;

  /// Time since the replica was active.
  final BigInt activeNanoseconds;

  /// Pending replicated operations.
  final BigInt lag;

  /// Deeply immutable exact server peer data.
  final Map<String, Object?> raw;
}

/// Stream cluster placement and leadership.
final class JetStreamClusterInfo {
  /// Creates an immutable cluster snapshot.
  JetStreamClusterInfo({
    required this.name,
    required this.raftGroup,
    required this.leader,
    required this.leaderSince,
    required this.systemAccount,
    required this.trafficAccount,
    required List<JetStreamPeerInfo> replicas,
    required Map<String, Object?> raw,
  }) : replicas = List.unmodifiable(replicas),
       raw = immutableExactJsonObject(raw);

  /// Cluster name.
  final String? name;

  /// Raft group name.
  final String? raftGroup;

  /// Current leader server name.
  final String? leader;

  /// Current leader election timestamp.
  final String? leaderSince;

  /// Whether replication uses the system account.
  final bool systemAccount;

  /// Replication traffic account.
  final String? trafficAccount;

  /// Immutable replica states.
  final List<JetStreamPeerInfo> replicas;

  /// Deeply immutable exact server cluster data.
  final Map<String, Object?> raw;
}

/// Current source or mirror replication state.
final class JetStreamStreamSourceInfo {
  /// Creates an immutable source snapshot.
  JetStreamStreamSourceInfo({
    required this.name,
    required this.lag,
    required this.activeNanoseconds,
    required this.filterSubject,
    required List<JetStreamSubjectTransform> subjectTransforms,
    required Map<String, Object?> raw,
  }) : subjectTransforms = List.unmodifiable(subjectTransforms),
       raw = immutableExactJsonObject(raw);

  /// Upstream stream name.
  final String name;

  /// Messages behind the upstream stream.
  final BigInt lag;

  /// Time since upstream activity. Negative one means no activity.
  final BigInt activeNanoseconds;

  /// Configured filter subject.
  final String? filterSubject;

  /// Immutable configured subject transformations.
  final List<JetStreamSubjectTransform> subjectTransforms;

  /// Deeply immutable exact server source data.
  final Map<String, Object?> raw;
}

/// Complete stream configuration, state, and cluster snapshot.
final class JetStreamStreamInfo {
  /// Creates an immutable stream snapshot.
  JetStreamStreamInfo({
    required this.config,
    required this.created,
    required this.state,
    required this.cluster,
    required this.mirror,
    required List<JetStreamStreamSourceInfo> sources,
    required this.timestamp,
    required Map<String, Object?> raw,
  }) : sources = List.unmodifiable(sources),
       raw = immutableExactJsonObject(raw);

  /// Effective server configuration.
  final JetStreamStreamConfig config;

  /// Stream creation timestamp.
  final String created;

  /// Current storage state.
  final JetStreamStreamState state;

  /// Cluster state when clustered.
  final JetStreamClusterInfo? cluster;

  /// Mirror replication state.
  final JetStreamStreamSourceInfo? mirror;

  /// Immutable source replication states.
  final List<JetStreamStreamSourceInfo> sources;

  /// Server observation timestamp.
  final String timestamp;

  /// Deeply immutable exact server information including unknown fields.
  final Map<String, Object?> raw;

  /// Stored message count.
  BigInt get messages => state.messages;

  /// Highest sequence assigned by this stream.
  BigInt get lastSequence => state.lastSequence;
}
