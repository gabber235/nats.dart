import "package:nats_jetstream/src/kv.dart";
import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/stream_source.dart";

/// Encodes one normalized KV backing stream configuration.
Map<String, Object?> encodeKeyValueStreamConfig(
  JetStreamKeyValueBucketConfig config,
  String stream,
) {
  return {
    "name": stream,
    "description": ?config.description,
    if (config.mirror == null) "subjects": ["\$KV.${config.name}.>"],
    "retention": "limits",
    "max_msgs_per_subject": config.history,
    "max_age": ?config.maximumAge?.nanoseconds,
    "max_bytes": config.maximumBytes ?? BigInt.from(-1),
    "max_msg_size": config.maximumValueBytes ?? BigInt.from(-1),
    "duplicate_window": _duplicateWindow(config.maximumAge),
    "storage": config.storage.wireValue,
    "num_replicas": config.replicas,
    "discard": "new",
    "allow_rollup_hdrs": true,
    "deny_delete": true,
    "allow_direct": true,
    if (config.metadata.isNotEmpty) "metadata": config.metadata,
    if (config.compression) "compression": "s2",
    if (config.limitMarkerTtl != null) "allow_msg_ttl": true,
    "subject_delete_marker_ttl": ?config.limitMarkerTtl?.nanoseconds,
    "placement": ?config.placement?.toJson(),
    "republish": ?config.republish?.toJson(),
    if (config.mirror != null)
      "mirror": _normalizedMirror(config.mirror!).toJson(),
    if (config.mirror != null) "mirror_direct": true,
    if (config.sources.isNotEmpty)
      "sources": config.sources
          .map((source) => _normalizedSource(source, config.name).toJson())
          .toList(growable: false),
  };
}

/// Replaces managed KV fields while retaining unknown stream configuration.
Map<String, Object?> mergeKeyValueStreamConfig(
  Map<String, Object?> current,
  Map<String, Object?> desired,
  Map<String, String> userMetadata,
) {
  final updated = Map<String, Object?>.from(current);
  for (final field in _managedConfigFields) {
    if (desired.containsKey(field)) {
      updated[field] = desired[field];
    } else {
      updated.remove(field);
    }
  }
  final currentMetadata = current["metadata"];
  final metadata = <String, String>{
    if (currentMetadata is Map<String, Object?>)
      for (final entry in currentMetadata.entries)
        if (entry.key.startsWith("_nats.") && entry.value is String)
          entry.key: entry.value as String,
    ...userMetadata,
  };
  if (metadata.isEmpty) {
    updated.remove("metadata");
  } else {
    updated["metadata"] = metadata;
  }
  return updated;
}

/// Returns a validated bucket name from a normalized or public source name.
String keyValueSourceBucket(String name) {
  final bucket = name.startsWith("KV_") ? name.substring(3) : name;
  validateJetStreamKeyValueBucket(bucket);
  return bucket;
}

JetStreamStreamSource _normalizedMirror(JetStreamStreamSource mirror) {
  return _copySource(
    mirror,
    name: _streamName(keyValueSourceBucket(mirror.name)),
  );
}

JetStreamStreamSource _normalizedSource(
  JetStreamStreamSource source,
  String targetBucket,
) {
  if (source.subjectTransforms.isNotEmpty) {
    return _copySource(source, name: source.name);
  }
  final sourceBucket = keyValueSourceBucket(source.name);
  if (source.external != null && sourceBucket == targetBucket) {
    return _copySource(source, name: _streamName(sourceBucket));
  }
  return _copySource(
    source,
    name: _streamName(sourceBucket),
    transforms: [
      JetStreamSubjectTransform(
        source: "\$KV.$sourceBucket.>",
        destination: "\$KV.$targetBucket.>",
      ),
    ],
  );
}

JetStreamStreamSource _copySource(
  JetStreamStreamSource source, {
  required String name,
  List<JetStreamSubjectTransform>? transforms,
}) {
  return JetStreamStreamSource(
    name: name,
    startSequence: source.startSequence,
    startTime: source.startTime,
    filterSubject: source.filterSubject,
    subjectTransforms: transforms ?? source.subjectTransforms,
    external: source.external,
    consumer: source.consumer,
  );
}

String _streamName(String bucket) => "KV_$bucket";

BigInt _duplicateWindow(JetStreamNanoseconds? maximumAge) {
  final twoMinutes = BigInt.from(120000000000);
  if (maximumAge == null || maximumAge.nanoseconds > twoMinutes) {
    return twoMinutes;
  }
  return maximumAge.nanoseconds;
}

const _managedConfigFields = {
  "description",
  "subjects",
  "retention",
  "max_msgs_per_subject",
  "max_age",
  "max_bytes",
  "max_msg_size",
  "duplicate_window",
  "storage",
  "num_replicas",
  "discard",
  "allow_rollup_hdrs",
  "deny_delete",
  "allow_direct",
  "compression",
  "allow_msg_ttl",
  "subject_delete_marker_ttl",
  "placement",
  "republish",
  "mirror",
  "mirror_direct",
  "sources",
};
