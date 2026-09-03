import "dart:typed_data";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/byte_snapshot.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_utf8.dart";
import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/publish_options.dart";
import "package:nats_jetstream/src/stream_config.dart";
import "package:nats_jetstream/src/stream_source.dart";
import "package:nats_jetstream/src/user_metadata.dart";
import "package:nats_jetstream/src/wire_integer.dart";

/// Operation represented by one stored KV revision.
enum JetStreamKeyValueOperation {
  /// A value was stored.
  put,

  /// A delete marker was stored.
  delete,

  /// A purge marker replaced subject history.
  purge,
}

/// Validated KV bucket configuration.
final class JetStreamKeyValueBucketConfig {
  /// Creates a bucket configuration.
  factory JetStreamKeyValueBucketConfig({
    required String name,
    int history = 1,
    JetStreamStorage storage = JetStreamStorage.file,
    JetStreamNanoseconds? maximumAge,
    BigInt? maximumBytes,
    BigInt? maximumValueBytes,
    int replicas = 1,
    String? description,
    Map<String, String> metadata = const {},
    bool compression = false,
    JetStreamNanoseconds? limitMarkerTtl,
    JetStreamPlacement? placement,
    JetStreamRepublish? republish,
    JetStreamStreamSource? mirror,
    List<JetStreamStreamSource> sources = const [],
  }) {
    validateJetStreamKeyValueBucket(name);
    if (history < 1 || history > 64) {
      throw ArgumentError.value(history, "history", "Must be between 1 and 64");
    }
    if (maximumAge != null && maximumAge.nanoseconds <= BigInt.zero) {
      throw ArgumentError.value(maximumAge, "maximumAge", "Must be positive");
    }
    if (maximumAge != null) {
      validatePositiveJetStreamDuration(maximumAge.nanoseconds, "maximumAge");
    }
    if (maximumBytes != null && maximumBytes <= BigInt.zero) {
      throw ArgumentError.value(
        maximumBytes,
        "maximumBytes",
        "Must be positive",
      );
    }
    if (maximumBytes != null) {
      validateJetStreamInt64(maximumBytes, "maximumBytes");
    }
    if (maximumValueBytes != null && maximumValueBytes <= BigInt.zero) {
      throw ArgumentError.value(
        maximumValueBytes,
        "maximumValueBytes",
        "Must be positive",
      );
    }
    if (maximumValueBytes != null) {
      validateJetStreamMessageSize(maximumValueBytes, "maximumValueBytes");
    }
    if (replicas < 1 || replicas > 5) {
      throw ArgumentError.value(replicas, "replicas", "Must be 1 through 5");
    }
    if (description != null &&
        encodeExactUtf8(description, name: "description").length > 4096) {
      throw ArgumentError("Description exceeds 4096 UTF8 bytes", "description");
    }
    if (metadata.length > 256 || metadata.keys.any((key) => key.isEmpty)) {
      throw ArgumentError.value(metadata, "metadata", "Invalid metadata");
    }
    final capturedMetadata = captureJetStreamUserMetadata(metadata);
    if (limitMarkerTtl != null &&
        limitMarkerTtl.nanoseconds < BigInt.from(1000000000)) {
      throw ArgumentError.value(
        limitMarkerTtl,
        "limitMarkerTtl",
        "Must be at least one second",
      );
    }
    if (limitMarkerTtl != null) {
      validatePositiveJetStreamDuration(
        limitMarkerTtl.nanoseconds,
        "limitMarkerTtl",
      );
    }
    if (mirror != null && sources.isNotEmpty) {
      throw ArgumentError("mirror and sources are mutually exclusive");
    }
    if (mirror != null) {
      _validateKeyValueMirror(mirror);
    }
    for (final source in sources) {
      _validateKeyValueSource(source);
    }
    return JetStreamKeyValueBucketConfig._(
      name,
      history,
      storage,
      maximumAge,
      maximumBytes,
      maximumValueBytes,
      replicas,
      description,
      capturedMetadata,
      compression,
      limitMarkerTtl,
      placement,
      republish,
      mirror,
      List.unmodifiable(sources),
    );
  }

  const JetStreamKeyValueBucketConfig._(
    this.name,
    this.history,
    this.storage,
    this.maximumAge,
    this.maximumBytes,
    this.maximumValueBytes,
    this.replicas,
    this.description,
    this.metadata,
    this.compression,
    this.limitMarkerTtl,
    this.placement,
    this.republish,
    this.mirror,
    this.sources,
  );

  /// Public bucket name.
  final String name;

  /// Revisions retained for each key.
  final int history;

  /// Stream storage implementation.
  final JetStreamStorage storage;

  /// Optional revision age limit.
  final JetStreamNanoseconds? maximumAge;

  /// Optional total stream byte limit.
  final BigInt? maximumBytes;

  /// Optional maximum bytes in one value.
  final BigInt? maximumValueBytes;

  /// Stream replica count.
  final int replicas;

  /// Optional human readable bucket description.
  final String? description;

  /// Immutable bucket metadata.
  final Map<String, String> metadata;

  /// Whether S2 storage compression is enabled.
  final bool compression;

  /// Retention for generated per key limit markers.
  final JetStreamNanoseconds? limitMarkerTtl;

  /// Placement constraints for the backing stream.
  final JetStreamPlacement? placement;

  /// Immediate republish mapping for KV revisions.
  final JetStreamRepublish? republish;

  /// Optional KV stream mirrored into this bucket.
  final JetStreamStreamSource? mirror;

  /// Immutable KV streams sourced into this bucket.
  final List<JetStreamStreamSource> sources;
}

void _validateKeyValueMirror(JetStreamStreamSource source) {
  final bucket = source.name.startsWith("KV_")
      ? source.name.substring(3)
      : source.name;
  validateJetStreamKeyValueBucket(bucket);
  if (source.filterSubject != null || source.subjectTransforms.isNotEmpty) {
    throw ArgumentError.value(
      source,
      "mirror",
      "KV mirrors do not accept subject mappings",
    );
  }
  if (source.consumer != null) {
    throw ArgumentError.value(
      source,
      "mirror",
      "KV mirrors cannot use a dedicated source consumer",
    );
  }
}

void _validateKeyValueSource(JetStreamStreamSource source) {
  if (source.subjectTransforms.isNotEmpty) return;
  final bucket = source.name.startsWith("KV_")
      ? source.name.substring(3)
      : source.name;
  validateJetStreamKeyValueBucket(bucket);
  if (source.filterSubject != null) {
    throw ArgumentError.value(
      source,
      "sources",
      "KV sources use automatic mapping or explicit subject transforms",
    );
  }
  if (source.consumer != null) {
    throw ArgumentError.value(
      source,
      "sources",
      "KV sources cannot use a dedicated source consumer",
    );
  }
}

/// Immutable result for one wildcard KV mutation.
final class JetStreamKeyValueMutationResult {
  /// Creates a result while snapshotting exact key revisions.
  JetStreamKeyValueMutationResult(Map<String, BigInt> revisions)
    : revisions = Map.unmodifiable(revisions);

  /// Resulting marker revision for every matched live key.
  final Map<String, BigInt> revisions;

  /// Number of matched live keys.
  int get matched => revisions.length;
}

/// A wildcard KV mutation stopped after applying a known prefix of keys.
final class JetStreamKeyValueMutationException implements Exception {
  /// Creates a partial mutation failure while preserving completed revisions.
  JetStreamKeyValueMutationException({
    required this.bucket,
    required this.operation,
    required this.filter,
    required Map<String, BigInt> completed,
    required this.cause,
  }) : completed = Map.unmodifiable(completed);

  /// Bucket being mutated.
  final String bucket;

  /// Logical mutation name.
  final String operation;

  /// Relative key filter being expanded.
  final String filter;

  /// Exact marker revisions completed before failure.
  final Map<String, BigInt> completed;

  /// Original publication or protocol failure.
  final Object cause;

  @override
  String toString() =>
      "JetStreamKeyValueMutationException: $operation failed on $bucket after "
      "${completed.length} keys";
}

/// Confirmed cleanup of current KV delete and purge markers.
final class JetStreamKeyValuePurgeDeletesResult {
  /// Creates an exact aggregate cleanup result.
  const JetStreamKeyValuePurgeDeletesResult({
    required this.purged,
    required this.processedMarkers,
    required this.retainedMarkers,
    required this.removedMarkers,
  });

  /// Total stream messages removed.
  final BigInt purged;

  /// Current marker keys processed.
  final int processedMarkers;

  /// Recent markers retained while their older history was removed.
  final int retainedMarkers;

  /// Old markers removed with their history.
  final int removedMarkers;
}

/// Delete marker cleanup stopped after a known set of key purges.
final class JetStreamKeyValuePurgeDeletesException implements Exception {
  /// Creates a partial cleanup failure.
  JetStreamKeyValuePurgeDeletesException({
    required this.bucket,
    required this.purged,
    required List<String> completedKeys,
    required this.cause,
  }) : completedKeys = List.unmodifiable(completedKeys);

  /// Bucket being cleaned.
  final String bucket;

  /// Exact messages removed before failure.
  final BigInt purged;

  /// Keys whose purge requests completed.
  final List<String> completedKeys;

  /// Original stream purge or protocol failure.
  final Object cause;

  @override
  String toString() =>
      "JetStreamKeyValuePurgeDeletesException: cleanup failed on $bucket after "
      "${completedKeys.length} keys";
}

/// Backwards compatible KV name for [JetStreamMessageLifetime].
typedef JetStreamKeyValueLifetime = JetStreamMessageLifetime;

/// Immutable KV revision.
final class JetStreamKeyValueEntry {
  /// Creates an entry while snapshotting its bytes.
  JetStreamKeyValueEntry({
    required this.bucket,
    required this.key,
    required List<int> value,
    required this.revision,
    required this.created,
    required this.operation,
    BigInt? delta,
  }) : delta = delta ?? BigInt.zero,
       value = checkedJetStreamByteSnapshot(value, name: "KV entry");

  /// Bucket name.
  final String bucket;

  /// Key relative to the bucket prefix.
  final String key;

  /// Immutable value bytes, empty for markers.
  final Uint8List value;

  /// Exact stream revision.
  final BigInt revision;

  /// Exact RFC 3339 creation timestamp.
  final String created;

  /// Stored operation.
  final JetStreamKeyValueOperation operation;

  /// Messages pending after this revision in a finite history snapshot.
  final BigInt delta;
}

/// A compare and set operation used a stale or conflicting revision.
final class JetStreamKeyValueConflictException implements Exception {
  /// Creates a typed revision conflict while preserving the server rejection.
  const JetStreamKeyValueConflictException({
    required this.bucket,
    required this.key,
    required this.operation,
    required this.expectedRevision,
    required this.cause,
  });

  /// KV bucket.
  final String bucket;

  /// Conflicting key.
  final String key;

  /// Logical mutation.
  final String operation;

  /// Caller supplied or create only revision.
  final BigInt expectedRevision;

  /// Stable JetStream API rejection.
  final JetStreamApiException cause;

  @override
  String toString() =>
      "JetStreamKeyValueConflictException: $operation conflict on $bucket/$key";
}

/// Validates a KV bucket name.
void validateJetStreamKeyValueBucket(String name) {
  if (name.isEmpty ||
      name.length > 255 ||
      !RegExp(r"^[A-Za-z0-9_-]+$").hasMatch(name)) {
    throw ArgumentError.value(name, "name", "Invalid KV bucket name");
  }
}

/// Validates a KV key and its NATS subject representation.
void validateJetStreamKeyValueKey(String key) {
  if (key.isEmpty ||
      key.length > 4096 ||
      key.startsWith(".") ||
      key.endsWith(".")) {
    throw ArgumentError.value(key, "key", "Invalid KV key");
  }
  for (final unit in key.codeUnits) {
    if (unit <= 32 || unit == 127 || unit == 42 || unit == 62) {
      throw ArgumentError.value(key, "key", "Invalid KV key");
    }
  }
  if (key.split(".").any((token) => token.isEmpty)) {
    throw ArgumentError.value(key, "key", "Invalid KV key");
  }
}

/// Validates a relative KV key filter with NATS wildcards.
void validateJetStreamKeyValueFilter(String filter) {
  if (filter.length > 4096) {
    throw ArgumentError.value(filter, "filter", "Too long");
  }
  try {
    validateSubscriptionSubject(filter);
  } on NatsSubjectException catch (error) {
    throw ArgumentError.value(filter, "filter", error.message);
  }
}

/// Validates logical filter hierarchy before a key codec is selected.
void validateJetStreamKeyValueLogicalFilter(String filter) {
  if (filter.isEmpty ||
      filter.length > 4096 ||
      filter.startsWith(".") ||
      filter.endsWith(".")) {
    throw ArgumentError.value(filter, "filter", "Invalid KV key filter");
  }
  final tokens = filter.split(".");
  for (var index = 0; index < tokens.length; index += 1) {
    final token = tokens[index];
    if (token.isEmpty ||
        (token.contains("*") && token != "*") ||
        (token.contains(">") && token != ">") ||
        (token == ">" && index != tokens.length - 1)) {
      throw ArgumentError.value(filter, "filter", "Invalid KV key filter");
    }
  }
}
