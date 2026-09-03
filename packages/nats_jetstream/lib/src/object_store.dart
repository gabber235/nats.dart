import "dart:convert";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/stream_config.dart";
import "package:nats_jetstream/src/stream_source.dart";
import "package:nats_jetstream/src/wire_integer.dart";

/// Default Object Store chunk size used by maintained NATS clients.
const jetStreamObjectDefaultChunkSize = 128 * 1024;

/// Chunk publication protocol for one Object Store put.
enum JetStreamObjectPutMode {
  /// Publish and acknowledge one chunk at a time.
  sequential,

  /// Use the NATS 2.14 fast batch protocol with gap rejection.
  ///
  /// The bucket must be created or updated with
  /// [JetStreamObjectStoreConfig.experimentalFastIngest] enabled.
  experimentalFastIngest,
}

/// Validated Object Store bucket configuration.
final class JetStreamObjectStoreConfig {
  /// Creates a bucket configuration.
  factory JetStreamObjectStoreConfig({
    required String name,
    String? description,
    JetStreamStorage storage = JetStreamStorage.file,
    JetStreamNanoseconds? maximumAge,
    BigInt? maximumBytes,
    int replicas = 1,
    Map<String, String> metadata = const {},
    bool compression = false,
    JetStreamPlacement? placement,
    bool experimentalFastIngest = false,
  }) {
    validateJetStreamObjectBucket(name);
    if (description != null) {
      _validateUtf8Bound(description, "description", 4096);
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
    if (replicas < 1 || replicas > 5) {
      throw ArgumentError.value(replicas, "replicas", "Must be 1 through 5");
    }
    _validateStringMap(metadata, "metadata");
    if (metadata.keys.any((key) => key.startsWith("_nats."))) {
      throw ArgumentError.value(
        metadata,
        "metadata",
        "Keys starting with _nats. are reserved by the server",
      );
    }
    return JetStreamObjectStoreConfig._(
      name,
      description,
      storage,
      maximumAge,
      maximumBytes,
      replicas,
      Map.unmodifiable(metadata),
      compression,
      placement,
      experimentalFastIngest,
    );
  }

  const JetStreamObjectStoreConfig._(
    this.name,
    this.description,
    this.storage,
    this.maximumAge,
    this.maximumBytes,
    this.replicas,
    this.metadata,
    this.compression,
    this.placement,
    this.experimentalFastIngest,
  );

  /// Public bucket name.
  final String name;

  /// Optional stream description.
  final String? description;

  /// Stream storage implementation.
  final JetStreamStorage storage;

  /// Optional object retention duration.
  final JetStreamNanoseconds? maximumAge;

  /// Optional total stream byte limit.
  final BigInt? maximumBytes;

  /// Stream replica count.
  final int replicas;

  /// Immutable bucket metadata.
  final Map<String, String> metadata;

  /// Whether server side S2 storage compression is enabled.
  final bool compression;

  /// Backing stream placement constraints.
  final JetStreamPlacement? placement;

  /// Whether NATS 2.14 fast batch ingest is enabled for this bucket.
  final bool experimentalFastIngest;
}

/// Exact runtime and configuration snapshot for one Object Store bucket.
final class JetStreamObjectStoreStatus {
  /// Creates an immutable status snapshot from a verified stream response.
  JetStreamObjectStoreStatus({
    required this.name,
    required this.description,
    required this.maximumAge,
    required this.maximumBytes,
    required this.storage,
    required this.replicas,
    required this.sealed,
    required this.storageBytes,
    required this.streamMessages,
    required Map<String, String> metadata,
    Map<String, String> systemMetadata = const {},
    required this.compression,
    this.placement,
    this.experimentalFastIngest = false,
  }) : metadata = Map.unmodifiable(metadata),
       systemMetadata = Map.unmodifiable(systemMetadata);

  /// Public bucket name.
  final String name;

  /// Optional bucket description.
  final String? description;

  /// Exact configured object expiration. Zero means unlimited.
  final JetStreamNanoseconds maximumAge;

  /// Exact stream byte limit. Negative one means unlimited.
  final BigInt maximumBytes;

  /// Backing stream storage implementation.
  final JetStreamStorage storage;

  /// Configured stream replica count.
  final int replicas;

  /// Whether every further mutation is permanently rejected.
  final bool sealed;

  /// Current bytes used by metadata and chunks.
  final BigInt storageBytes;

  /// Current backing stream message count.
  final BigInt streamMessages;

  /// Immutable bucket metadata.
  final Map<String, String> metadata;

  /// Immutable NATS capability metadata using reserved `_nats.` keys.
  final Map<String, String> systemMetadata;

  /// Whether server side S2 storage compression is enabled.
  final bool compression;

  /// Backing stream placement constraints.
  final JetStreamPlacement? placement;

  /// Whether the backing stream accepts experimental fast batch ingest.
  final bool experimentalFastIngest;
}

/// User supplied metadata for one object upload.
final class JetStreamObjectMetadata {
  /// Creates immutable object metadata.
  factory JetStreamObjectMetadata({
    required String name,
    String? description,
    NatsHeaders? headers,
    Map<String, String> metadata = const {},
    int chunkSize = jetStreamObjectDefaultChunkSize,
  }) {
    validateJetStreamObjectName(name);
    if (description != null) {
      _validateUtf8Bound(description, "description", 4096);
    }
    _validateStringMap(metadata, "metadata");
    if (chunkSize < 1 || chunkSize > 8 * 1024 * 1024) {
      throw ArgumentError.value(
        chunkSize,
        "chunkSize",
        "Must be 1 through 8388608 bytes",
      );
    }
    return JetStreamObjectMetadata._(
      name,
      description,
      headers,
      Map.unmodifiable(metadata),
      chunkSize,
    );
  }

  const JetStreamObjectMetadata._(
    this.name,
    this.description,
    this.headers,
    this.metadata,
    this.chunkSize,
  );

  /// Unrestricted public object name.
  final String name;

  /// Optional human readable description.
  final String? description;

  /// Optional object metadata headers.
  final NatsHeaders? headers;

  /// Immutable application metadata.
  final Map<String, String> metadata;

  /// Maximum raw bytes in one stored chunk.
  final int chunkSize;
}

/// Mutable metadata fields accepted by an Object Store metadata update.
final class JetStreamObjectMetadataUpdate {
  /// Creates a validated immutable metadata replacement.
  factory JetStreamObjectMetadataUpdate({
    required String name,
    String? description,
    NatsHeaders? headers,
    Map<String, String> metadata = const {},
  }) {
    validateJetStreamObjectName(name);
    if (description != null) {
      _validateUtf8Bound(description, "description", 4096);
    }
    _validateStringMap(metadata, "metadata");
    return JetStreamObjectMetadataUpdate._(
      name,
      description,
      headers,
      Map.unmodifiable(metadata),
    );
  }

  const JetStreamObjectMetadataUpdate._(
    this.name,
    this.description,
    this.headers,
    this.metadata,
  );

  /// New object name. A different value requests a rename.
  final String name;

  /// Replacement description, or `null` to clear it.
  final String? description;

  /// Replacement headers, or `null` to clear them.
  final NatsHeaders? headers;

  /// Replacement application metadata.
  final Map<String, String> metadata;
}

/// A link stored as Object Store metadata.
final class JetStreamObjectLink {
  /// Creates a validated object or bucket link.
  factory JetStreamObjectLink({required String bucket, String? name}) {
    validateJetStreamObjectBucket(bucket);
    if (name != null) {
      validateJetStreamObjectName(name);
    }
    return JetStreamObjectLink._(bucket, name);
  }

  const JetStreamObjectLink._(this.bucket, this.name);

  /// Target bucket.
  final String bucket;

  /// Target object, or `null` for a bucket link.
  final String? name;
}

/// Immutable committed Object Store metadata.
final class JetStreamObjectInfo {
  /// Creates an object information snapshot.
  JetStreamObjectInfo({
    required this.name,
    required this.bucket,
    required this.identifier,
    required this.revision,
    required this.size,
    required this.chunks,
    required this.digest,
    required this.modified,
    required this.deleted,
    required this.chunkSize,
    this.description,
    this.headers,
    Map<String, String> metadata = const {},
    this.link,
  }) : metadata = Map.unmodifiable(metadata);

  /// Public object name.
  final String name;

  /// Owning bucket.
  final String bucket;

  /// Opaque chunk subject identifier.
  final String identifier;

  /// Exact stream revision of the current metadata record.
  final BigInt revision;

  /// Exact object byte size.
  final BigInt size;

  /// Exact chunk count.
  final BigInt chunks;

  /// RFC 6234 style SHA 256 digest.
  final String digest;

  /// Exact server assigned RFC 3339 metadata timestamp.
  final String modified;

  /// Whether this metadata is a deletion marker.
  final bool deleted;

  /// Maximum chunk size selected when the object was stored.
  final int chunkSize;

  /// Optional human readable description.
  final String? description;

  /// Optional object metadata headers.
  final NatsHeaders? headers;

  /// Immutable application metadata.
  final Map<String, String> metadata;

  /// Optional object or bucket link.
  final JetStreamObjectLink? link;
}

/// Validates an Object Store bucket name.
void validateJetStreamObjectBucket(String name) {
  if (name.isEmpty ||
      name.length > 255 ||
      !RegExp(r"^[A-Za-z0-9_-]+$").hasMatch(name)) {
    throw ArgumentError.value(name, "name", "Invalid Object Store bucket");
  }
}

/// Validates an unrestricted name against the default NATS control line bound.
void validateJetStreamObjectName(String name) {
  if (name.isEmpty) {
    throw ArgumentError.value(name, "name", "Must not be empty");
  }
  final bytes = _exactUtf8(name, "name");
  if (base64Url.encode(bytes).length > 3072) {
    throw ArgumentError.value(
      name,
      "name",
      "Encoded name exceeds the safe subject bound",
    );
  }
}

void _validateStringMap(Map<String, String> values, String parameter) {
  if (values.length > 256) {
    throw ArgumentError.value(
      values,
      parameter,
      "Must have at most 256 entries",
    );
  }
  var bytes = 0;
  for (final entry in values.entries) {
    if (entry.key.isEmpty) {
      throw ArgumentError.value(values, parameter, "Keys must not be empty");
    }
    bytes += _exactUtf8(entry.key, parameter).length;
    bytes += _exactUtf8(entry.value, parameter).length;
  }
  if (bytes > 64 * 1024) {
    throw ArgumentError.value(values, parameter, "Must not exceed 65536 bytes");
  }
}

void _validateUtf8Bound(String value, String parameter, int maximumBytes) {
  if (_exactUtf8(value, parameter).length > maximumBytes) {
    throw ArgumentError.value(
      value,
      parameter,
      "Must not exceed $maximumBytes UTF 8 bytes",
    );
  }
}

List<int> _exactUtf8(String value, String parameter) {
  final bytes = utf8.encode(value);
  if (utf8.decode(bytes) != value) {
    throw ArgumentError.value(
      value,
      parameter,
      "Must contain exact UTF 8 text",
    );
  }
  return bytes;
}
