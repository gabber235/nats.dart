import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/object_store.dart";
import "package:nats_jetstream/src/object_store_digest.dart";
import "package:nats_jetstream/src/timestamp.dart";

final _maximumUint64 = (BigInt.one << 64) - BigInt.one;

/// Decodes one stored metadata payload with its server assigned identity.
JetStreamObjectInfo decodeObjectInfoPayload(
  List<int> payload, {
  required String subject,
  required String expectedBucket,
  required String expectedName,
  required String modified,
  required BigInt revision,
}) {
  late final Map<String, Object?> value;
  try {
    value = decodeExactJsonObject(payload, maxBytes: 1024 * 1024);
  } on FormatException catch (error) {
    throw JetStreamProtocolException(
      "Object metadata contains invalid JSON",
      subject: subject,
      cause: error,
    );
  }
  return decodeObjectInfo(
    value,
    subject: subject,
    expectedBucket: expectedBucket,
    expectedName: expectedName,
    modified: modified,
    revision: revision,
  );
}

/// Decodes ObjectInfo JSON after a direct metadata lookup.
JetStreamObjectInfo decodeObjectInfo(
  Map<String, Object?> value, {
  required String subject,
  required String expectedBucket,
  required String expectedName,
  required String modified,
  required BigInt revision,
}) {
  try {
    validateJetStreamRfc3339(modified, "message.time");
  } on ArgumentError catch (error) {
    throw JetStreamProtocolException(
      "Object metadata timestamp must be RFC 3339",
      subject: subject,
      fieldPath: "message.time",
      cause: error,
    );
  }
  final name = _string(value, "name", subject);
  final bucket = _string(value, "bucket", subject);
  final identifier = _string(value, "nuid", subject);
  if (name != expectedName) {
    throw JetStreamProtocolException(
      "Object metadata identity does not match its subject",
      subject: subject,
      fieldPath: "name",
    );
  }
  try {
    validateJetStreamObjectBucket(bucket);
  } on ArgumentError catch (error) {
    throw JetStreamProtocolException(
      "Object metadata contains an invalid bucket",
      subject: subject,
      fieldPath: "bucket",
      cause: error,
    );
  }
  if (bucket != expectedBucket) {
    throw JetStreamProtocolException(
      "Object metadata bucket does not match its subject",
      subject: subject,
      fieldPath: "bucket",
    );
  }
  if (!RegExp(r"^[A-Za-z0-9]{22}$").hasMatch(identifier)) {
    throw JetStreamProtocolException(
      "Object metadata contains an invalid identifier",
      subject: subject,
      fieldPath: "nuid",
    );
  }
  final options = _optionalObject(value, "options", subject);
  final chunkSize = options == null
      ? jetStreamObjectDefaultChunkSize
      : _optionalSafeInt(
              options,
              "max_chunk_size",
              subject,
              minimum: 1,
              maximum: 8 * 1024 * 1024,
            ) ??
            jetStreamObjectDefaultChunkSize;
  final linkValue = options == null
      ? null
      : _optionalObject(options, "link", subject);
  final targetName = linkValue == null
      ? null
      : _optionalString(linkValue, "name", subject);
  late final JetStreamObjectLink? link;
  try {
    link = linkValue == null
        ? null
        : JetStreamObjectLink(
            bucket: _string(linkValue, "bucket", subject),
            name: targetName == null || targetName.isEmpty ? null : targetName,
          );
  } on ArgumentError catch (error) {
    throw JetStreamProtocolException(
      "Object metadata contains an invalid link",
      subject: subject,
      fieldPath: "options.link",
      cause: error,
    );
  }
  final size = _unsigned(value, "size", subject);
  final chunks = _unsigned(value, "chunks", subject);
  final digest = _optionalString(value, "digest", subject) ?? "";
  final deleted = _optionalBoolean(value, "deleted", subject) ?? false;
  if (deleted || link != null) {
    if (size != BigInt.zero || chunks != BigInt.zero || digest.isNotEmpty) {
      throw JetStreamProtocolException(
        "Deleted objects and links must not contain object bytes",
        subject: subject,
        fieldPath: "size",
      );
    }
  } else {
    if ((size == BigInt.zero) != (chunks == BigInt.zero) ||
        (size > BigInt.zero && chunks > size)) {
      throw JetStreamProtocolException(
        "Object size and chunk count are inconsistent",
        subject: subject,
        fieldPath: "chunks",
      );
    }
    validateObjectDigest(digest, subject);
  }
  return JetStreamObjectInfo(
    name: name,
    bucket: bucket,
    identifier: identifier,
    revision: revision,
    size: size,
    chunks: chunks,
    digest: digest,
    modified: modified,
    deleted: deleted,
    chunkSize: chunkSize,
    description: _optionalString(value, "description", subject),
    headers: _decodeHeaders(value, subject),
    metadata: _decodeStringMap(value, "metadata", subject),
    link: link,
  );
}

NatsHeaders? _decodeHeaders(Map<String, Object?> source, String subject) {
  final value = source["headers"];
  if (value == null) {
    return null;
  }
  if (value is! Map<String, Object?>) {
    throw _fieldError(subject, "headers");
  }
  final entries = <MapEntry<String, String>>[];
  for (final entry in value.entries) {
    if (entry.value is! List<Object?>) {
      throw _fieldError(subject, "headers.${entry.key}");
    }
    for (final item in entry.value! as List<Object?>) {
      if (item is! String) {
        throw _fieldError(subject, "headers.${entry.key}");
      }
      entries.add(MapEntry(entry.key, item));
    }
  }
  try {
    return NatsHeaders(entries: entries);
  } on NatsHeaderException catch (error) {
    throw JetStreamProtocolException(
      "Object metadata contains invalid headers",
      subject: subject,
      fieldPath: "headers",
      cause: error,
    );
  }
}

Map<String, String> _decodeStringMap(
  Map<String, Object?> source,
  String field,
  String subject,
) {
  final value = source[field];
  if (value == null) {
    return const {};
  }
  if (value is! Map<String, Object?>) {
    throw _fieldError(subject, field);
  }
  final result = <String, String>{};
  for (final entry in value.entries) {
    if (entry.value is! String) {
      throw _fieldError(subject, "$field.${entry.key}");
    }
    result[entry.key] = entry.value! as String;
  }
  return result;
}

Map<String, Object?>? _optionalObject(
  Map<String, Object?> source,
  String field,
  String subject,
) {
  final value = source[field];
  if (value == null) {
    return null;
  }
  if (value is! Map<String, Object?>) {
    throw _fieldError(subject, field);
  }
  return value;
}

String _string(Map<String, Object?> source, String field, String subject) {
  final value = source[field];
  if (value is! String || value.isEmpty) {
    throw _fieldError(subject, field);
  }
  return value;
}

String? _optionalString(
  Map<String, Object?> source,
  String field,
  String subject,
) {
  final value = source[field];
  if (value == null) {
    return null;
  }
  if (value is! String) {
    throw _fieldError(subject, field);
  }
  return value;
}

bool? _optionalBoolean(
  Map<String, Object?> source,
  String field,
  String subject,
) {
  final value = source[field];
  if (value == null) {
    return null;
  }
  if (value is! bool) {
    throw _fieldError(subject, field);
  }
  return value;
}

BigInt _unsigned(Map<String, Object?> source, String field, String subject) {
  final value = source[field];
  if (value is! BigInt || value.isNegative || value > _maximumUint64) {
    throw _fieldError(subject, field);
  }
  return value;
}

int? _optionalSafeInt(
  Map<String, Object?> source,
  String field,
  String subject, {
  required int minimum,
  required int maximum,
}) {
  final value = source[field];
  if (value == null) {
    return null;
  }
  if (value is! BigInt ||
      value < BigInt.from(minimum) ||
      value > BigInt.from(maximum)) {
    throw _fieldError(subject, field);
  }
  return value.toInt();
}

JetStreamProtocolException _fieldError(String subject, String field) =>
    JetStreamProtocolException(
      "Object metadata field has the wrong shape",
      subject: subject,
      fieldPath: field,
    );
