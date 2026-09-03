import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/stream_config.dart";
import "package:nats_jetstream/src/stream_models.dart";
import "package:nats_jetstream/src/stream_source.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// Decodes a complete stream information response.
JetStreamStreamInfo decodeStreamInfo(
  JetStreamWireResponse response, {
  Map<String, Object?>? source,
}) {
  try {
    final root = source ?? response.body;
    final config = response.requiredObject("config", source: root);
    final state = response.requiredObject("state", source: root);
    final cluster = response.optionalObject("cluster", source: root);
    final mirror = response.optionalObject("mirror", source: root);
    return JetStreamStreamInfo(
      config: JetStreamStreamConfig.fromJson(config),
      created: response.requiredRfc3339("created", source: root),
      state: _state(response, state),
      cluster: cluster == null ? null : _cluster(response, cluster),
      mirror: mirror == null ? null : _source(response, mirror),
      sources: List.unmodifiable(
        response
            .optionalObjectList("sources", source: root)
            .map((source) => _source(response, source)),
      ),
      timestamp: response.requiredRfc3339("ts", source: root),
      raw: immutableExactJsonObject(root),
    );
  } on ArgumentError catch (error) {
    throw JetStreamProtocolException(
      "Invalid stream information",
      subject: response.subject,
      cause: error,
    );
  } on TypeError catch (error) {
    throw JetStreamProtocolException(
      "Invalid stream information",
      subject: response.subject,
      cause: error,
    );
  }
}

/// Requires one decoded stream response to match the requested stream.
JetStreamStreamInfo requireStreamIdentity(
  JetStreamWireResponse response,
  JetStreamStreamInfo info,
  String expectedStream,
) {
  if (info.config.name != expectedStream) {
    throw JetStreamProtocolException(
      "Stream response identity does not match the request",
      subject: response.subject,
      fieldPath: "config.name",
    );
  }
  return info;
}

JetStreamStreamState _state(
  JetStreamWireResponse response,
  Map<String, Object?> source,
) {
  return JetStreamStreamState(
    messages: response.requiredUnsigned("state.messages", source: source),
    bytes: response.requiredUnsigned("state.bytes", source: source),
    firstSequence: response.requiredUnsigned("state.first_seq", source: source),
    firstTimestamp: response.requiredRfc3339("state.first_ts", source: source),
    lastSequence: response.requiredUnsigned("state.last_seq", source: source),
    lastTimestamp: response.requiredRfc3339("state.last_ts", source: source),
    consumerCount: response.requiredNonNegativeInt64(
      "state.consumer_count",
      source: source,
    ),
    deleted: response.optionalUnsignedList("state.deleted", source: source),
    deletedCount:
        response.optionalUnsigned("state.num_deleted", source: source) ??
        BigInt.zero,
    subjectCount:
        response.optionalUnsigned("state.num_subjects", source: source) ??
        BigInt.zero,
    subjects: response.optionalUnsignedMap("state.subjects", source: source),
    raw: immutableExactJsonObject(source),
  );
}

JetStreamClusterInfo _cluster(
  JetStreamWireResponse response,
  Map<String, Object?> source,
) {
  return JetStreamClusterInfo(
    name: response.optionalString("cluster.name", source: source),
    raftGroup: response.optionalString("cluster.raft_group", source: source),
    leader: response.optionalString("cluster.leader", source: source),
    leaderSince: response.optionalRfc3339(
      "cluster.leader_since",
      source: source,
    ),
    systemAccount: response.optionalBoolean(
      "cluster.system_account",
      source: source,
    ),
    trafficAccount: response.optionalString(
      "cluster.traffic_account",
      source: source,
    ),
    replicas: List.unmodifiable(
      response
          .optionalObjectList("cluster.replicas", source: source)
          .map((peer) => _peer(response, peer)),
    ),
    raw: immutableExactJsonObject(source),
  );
}

JetStreamPeerInfo _peer(
  JetStreamWireResponse response,
  Map<String, Object?> source,
) {
  return JetStreamPeerInfo(
    name: response.requiredString("cluster.replicas.name", source: source),
    current: response.requiredBoolean(
      "cluster.replicas.current",
      source: source,
    ),
    offline: response.optionalBoolean(
      "cluster.replicas.offline",
      source: source,
    ),
    activeNanoseconds: response.requiredNonNegativeInt64(
      "cluster.replicas.active",
      source: source,
    ),
    lag:
        response.optionalUnsigned("cluster.replicas.lag", source: source) ??
        BigInt.zero,
    raw: immutableExactJsonObject(source),
  );
}

JetStreamStreamSourceInfo _source(
  JetStreamWireResponse response,
  Map<String, Object?> source,
) {
  return JetStreamStreamSourceInfo(
    name: response.requiredString("sources.name", source: source),
    lag: response.requiredUnsigned("sources.lag", source: source),
    activeNanoseconds: response.requiredSignedInt64(
      "sources.active",
      source: source,
    ),
    filterSubject: response.optionalString(
      "sources.filter_subject",
      source: source,
    ),
    subjectTransforms: List.unmodifiable(
      response
          .optionalObjectList("sources.subject_transforms", source: source)
          .map(
            (transform) => JetStreamSubjectTransform(
              source: response.requiredString(
                "sources.subject_transforms.src",
                source: transform,
              ),
              destination: response.requiredString(
                "sources.subject_transforms.dest",
                source: transform,
              ),
            ),
          ),
    ),
    raw: immutableExactJsonObject(source),
  );
}
