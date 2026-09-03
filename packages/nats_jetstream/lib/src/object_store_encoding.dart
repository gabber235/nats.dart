import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/object_store.dart";

/// Encodes public metadata into the ObjectInfo JSON shape.
Map<String, Object?> encodeObjectInfo({
  required JetStreamObjectMetadata metadata,
  required String bucket,
  required String identifier,
  required BigInt size,
  required BigInt chunks,
  required String digest,
}) {
  return {
    "name": metadata.name,
    "description": ?metadata.description,
    "headers": ?_encodeHeadersOrNull(metadata.headers),
    if (metadata.metadata.isNotEmpty) "metadata": metadata.metadata,
    "options": {"max_chunk_size": metadata.chunkSize},
    "bucket": bucket,
    "nuid": identifier,
    "size": size,
    "chunks": chunks,
    "digest": digest,
  };
}

/// Encodes a deletion marker while preserving user metadata.
Map<String, Object?> encodeObjectDeletion(JetStreamObjectInfo info) {
  return {
    "name": info.name,
    "description": ?info.description,
    "headers": ?_encodeHeadersOrNull(info.headers),
    if (info.metadata.isNotEmpty) "metadata": info.metadata,
    "options": {
      "link": ?_encodeLink(info.link),
      "max_chunk_size": info.chunkSize,
    },
    "bucket": info.bucket,
    "nuid": info.identifier,
    "size": BigInt.zero,
    "chunks": BigInt.zero,
    "deleted": true,
  };
}

/// Encodes a rename source tombstone with an independent chunk identity.
Map<String, Object?> encodeObjectRenameDeletion(
  JetStreamObjectInfo info,
  String identifier,
) {
  return {
    "name": info.name,
    "description": ?info.description,
    "headers": ?_encodeHeadersOrNull(info.headers),
    if (info.metadata.isNotEmpty) "metadata": info.metadata,
    "options": {"max_chunk_size": info.chunkSize},
    "bucket": info.bucket,
    "nuid": identifier,
    "size": BigInt.zero,
    "chunks": BigInt.zero,
    "deleted": true,
  };
}

/// Encodes existing immutable object fields with replacement user metadata.
Map<String, Object?> encodeObjectMetadataUpdate(
  JetStreamObjectInfo info,
  JetStreamObjectMetadataUpdate update,
) => {
  "name": update.name,
  "description": ?update.description,
  "headers": ?_encodeHeadersOrNull(update.headers),
  if (update.metadata.isNotEmpty) "metadata": update.metadata,
  "options": {
    "link": ?_encodeLink(info.link),
    "max_chunk_size": info.chunkSize,
  },
  "bucket": info.bucket,
  "nuid": info.identifier,
  "size": info.size,
  "chunks": info.chunks,
  if (info.digest.isNotEmpty) "digest": info.digest,
};

/// Encodes a zero byte object or bucket link.
Map<String, Object?> encodeObjectLink({
  required String name,
  required String bucket,
  required String identifier,
  required JetStreamObjectLink link,
}) => {
  "name": name,
  "options": {
    "link": _encodeLink(link),
    "max_chunk_size": jetStreamObjectDefaultChunkSize,
  },
  "bucket": bucket,
  "nuid": identifier,
  "size": BigInt.zero,
  "chunks": BigInt.zero,
};

/// Encodes one decoded metadata snapshot into its canonical stored shape.
Map<String, Object?> encodeCommittedObjectInfo(JetStreamObjectInfo info) => {
  "name": info.name,
  "description": ?info.description,
  "headers": ?_encodeHeadersOrNull(info.headers),
  if (info.metadata.isNotEmpty) "metadata": info.metadata,
  "options": {
    "link": ?_encodeLink(info.link),
    "max_chunk_size": info.chunkSize,
  },
  "bucket": info.bucket,
  "nuid": info.identifier,
  "size": info.size,
  "chunks": info.chunks,
  if (info.digest.isNotEmpty) "digest": info.digest,
  if (info.deleted) "deleted": true,
};

/// Whether decoded metadata exactly matches one canonical publication body.
bool objectInfoMatchesEncoding(
  JetStreamObjectInfo info,
  Map<String, Object?> body,
) {
  final expected = encodeExactJson(body);
  final actual = encodeExactJson(encodeCommittedObjectInfo(info));
  if (expected.length != actual.length) {
    return false;
  }
  for (var index = 0; index < expected.length; index += 1) {
    if (expected[index] != actual[index]) {
      return false;
    }
  }
  return true;
}

Map<String, Object?> _encodeHeaders(NatsHeaders headers) {
  final result = <String, List<Object?>>{};
  for (final entry in headers.entries) {
    result.putIfAbsent(entry.key, () => <Object?>[]).add(entry.value);
  }
  return result;
}

Map<String, Object?>? _encodeHeadersOrNull(NatsHeaders? headers) =>
    headers == null ? null : _encodeHeaders(headers);

Map<String, Object?>? _encodeLink(JetStreamObjectLink? link) =>
    link == null ? null : {"bucket": link.bucket, "name": ?link.name};
