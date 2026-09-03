import "dart:convert";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/consumer_delivery.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/kv.dart";
import "package:nats_jetstream/src/kv_codecs.dart";
import "package:nats_jetstream/src/timestamp.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// Decodes one stored KV message with exact revision semantics.
JetStreamKeyValueEntry decodeStoredKeyValueEntry(
  JetStreamWireResponse response,
  Map<String, Object?> stored, {
  required String bucket,
  required String encodedKey,
  required JetStreamKeyValueCodecs codecs,
}) {
  final payload = response.optionalString("message.data", source: stored);
  final headerData = response.optionalString("message.hdrs", source: stored);
  late final List<int> storedValue;
  late final NatsHeaders? headers;
  try {
    storedValue = payload == null ? const [] : base64Decode(payload);
    headers = headerData == null
        ? null
        : NatsHeaders.parse(base64Decode(headerData));
  } on FormatException catch (error) {
    throw JetStreamProtocolException(
      "KV response contains invalid base64",
      subject: response.subject,
      cause: error,
    );
  }
  final operation = keyValueOperation(headers);
  return JetStreamKeyValueEntry(
    bucket: bucket,
    key: codecs.decodeKey(encodedKey),
    value: operation == JetStreamKeyValueOperation.put
        ? codecs.decodeValue(storedValue)
        : const [],
    revision: response.requiredPositiveUint64("message.seq", source: stored),
    created: response.requiredRfc3339("message.time", source: stored),
    operation: operation,
  );
}

/// Decodes one KV consumer delivery with exact ACK subject metadata.
JetStreamKeyValueEntry decodeKeyValueDelivery(
  JetStreamDelivery delivery, {
  required String bucket,
  String? subjectPrefix,
  JetStreamKeyValueCodecs codecs = const JetStreamKeyValueCodecs(),
  bool decodeValue = true,
}) {
  final message = delivery.message;
  final prefix = subjectPrefix ?? "\$KV.$bucket.";
  if (!message.subject.startsWith(prefix)) {
    throw const JetStreamProtocolException(
      "KV consumer delivered a subject outside the bucket",
    );
  }
  final metadata = delivery.metadata;
  if (metadata == null) {
    throw const JetStreamProtocolException(
      "KV consumer delivery is missing JetStream metadata",
    );
  }
  final encodedKey = message.subject.substring(prefix.length);
  validateJetStreamKeyValueKey(encodedKey);
  final operation = keyValueOperation(message.headers);
  return JetStreamKeyValueEntry(
    bucket: bucket,
    key: codecs.decodeKey(encodedKey),
    value: decodeValue && operation == JetStreamKeyValueOperation.put
        ? codecs.decodeValue(message.payload)
        : const [],
    revision: metadata.streamSequence,
    created: jetStreamTimestamp(metadata.timestampNanoseconds),
    operation: operation,
    delta: metadata.pending,
  );
}

/// Maps explicit and server generated KV tombstone headers.
JetStreamKeyValueOperation keyValueOperation(NatsHeaders? headers) {
  return switch (headers?.first("KV-Operation")) {
    "DEL" => JetStreamKeyValueOperation.delete,
    "PURGE" => JetStreamKeyValueOperation.purge,
    _ => switch (headers?.first("Nats-Marker-Reason")) {
      "Remove" => JetStreamKeyValueOperation.delete,
      "MaxAge" || "Purge" => JetStreamKeyValueOperation.purge,
      _ => JetStreamKeyValueOperation.put,
    },
  };
}
