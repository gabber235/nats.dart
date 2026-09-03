import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/kv.dart";
import "package:nats_jetstream/src/kv_source_wire.dart";
import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/stream_config.dart";
import "package:nats_jetstream/src/stream_models.dart";
import "package:nats_jetstream/src/stream_source.dart";

/// Immutable current status for one verified KV bucket.
final class JetStreamKeyValueStatus {
  /// Creates a complete immutable status snapshot.
  factory JetStreamKeyValueStatus({
    required String bucket,
    required BigInt values,
    required BigInt sizeBytes,
    required int history,
    required JetStreamNanoseconds? maximumAge,
    required JetStreamNanoseconds? markerLifetime,
    required String storageWireValue,
    required JetStreamStorage? knownStorage,
    required int replicas,
    required String? description,
    required BigInt maximumBytes,
    required BigInt maximumValueBytes,
    required JetStreamPlacement? placement,
    required JetStreamRepublish? republish,
    JetStreamStreamSource? mirror,
    List<JetStreamStreamSource> sources = const [],
    required Map<String, String> metadata,
    required String compressionWireValue,
    required JetStreamStreamInfo streamInfo,
  }) {
    return JetStreamKeyValueStatus._(
      bucket: bucket,
      values: values,
      sizeBytes: sizeBytes,
      history: history,
      maximumAge: maximumAge,
      markerLifetime: markerLifetime,
      storageWireValue: storageWireValue,
      knownStorage: knownStorage,
      replicas: replicas,
      description: description,
      maximumBytes: maximumBytes,
      maximumValueBytes: maximumValueBytes,
      placement: placement,
      republish: republish,
      mirror: mirror,
      sources: List.unmodifiable(sources),
      metadata: Map.unmodifiable(metadata),
      compressionWireValue: compressionWireValue,
      streamInfo: streamInfo,
    );
  }

  const JetStreamKeyValueStatus._({
    required this.bucket,
    required this.values,
    required this.sizeBytes,
    required this.history,
    required this.maximumAge,
    required this.markerLifetime,
    required this.storageWireValue,
    required this.knownStorage,
    required this.replicas,
    required this.description,
    required this.maximumBytes,
    required this.maximumValueBytes,
    required this.placement,
    required this.republish,
    required this.mirror,
    required this.sources,
    required this.metadata,
    required this.compressionWireValue,
    required this.streamInfo,
  });

  /// Public bucket name without the backing stream prefix.
  final String bucket;

  /// Current stored revision count, including retained markers.
  final BigInt values;

  /// Current backing stream storage bytes.
  final BigInt sizeBytes;

  /// Maximum retained revisions per key.
  final int history;

  /// Maximum revision age, or `null` when unlimited.
  final JetStreamNanoseconds? maximumAge;

  /// Server generated marker lifetime, or `null` when disabled.
  final JetStreamNanoseconds? markerLifetime;

  /// Exact storage implementation value for forward compatibility.
  final String storageWireValue;

  /// Known storage implementation, or `null` for a newer server value.
  final JetStreamStorage? knownStorage;

  /// Backing stream replica count.
  final int replicas;

  /// Human readable bucket description.
  final String? description;

  /// Maximum bucket bytes. Negative one means unlimited.
  final BigInt maximumBytes;

  /// Maximum value bytes. Negative one means unlimited.
  final BigInt maximumValueBytes;

  /// Backing stream placement constraints.
  final JetStreamPlacement? placement;

  /// Immediate republish mapping for KV revisions.
  final JetStreamRepublish? republish;

  /// Optional stream mirrored into this bucket.
  final JetStreamStreamSource? mirror;

  /// Immutable streams sourced into this bucket.
  final List<JetStreamStreamSource> sources;

  /// Immutable bucket metadata.
  final Map<String, String> metadata;

  /// Exact backing stream compression value for forward compatibility.
  final String compressionWireValue;

  /// Known compression state, or `null` for a newer server value.
  bool? get compression => switch (compressionWireValue) {
    "none" => false,
    "s2" => true,
    _ => null,
  };

  /// Complete immutable backing stream information.
  final JetStreamStreamInfo streamInfo;
}

/// Decodes and verifies one backing stream status.
JetStreamKeyValueStatus decodeKeyValueStatus(
  JetStreamStreamInfo info, {
  required String subject,
}) {
  final config = info.config;
  final stream = config.name;
  if (!stream.startsWith("KV_") || stream.length == 3) {
    throw JetStreamProtocolException(
      "Stream is not a KV bucket",
      subject: subject,
      fieldPath: "config.name",
    );
  }
  final bucket = stream.substring(3);
  late final JetStreamStreamSource? mirror;
  late final List<JetStreamStreamSource> sources;
  try {
    mirror = decodeKeyValueStreamSource(config.raw["mirror"], "config.mirror");
    sources = decodeKeyValueStreamSources(config.raw["sources"]);
  } on Object catch (error) {
    throw JetStreamProtocolException(
      "KV backing stream source configuration is malformed",
      subject: subject,
      fieldPath: "config",
      cause: error,
    );
  }
  if (mirror != null && sources.isNotEmpty) {
    throw JetStreamProtocolException(
      "KV backing stream cannot define both mirror and sources",
      subject: subject,
      fieldPath: "config",
    );
  }
  if (mirror != null &&
      (!mirror.name.startsWith("KV_") || mirror.name.length == 3)) {
    throw JetStreamProtocolException(
      "KV mirror source is not a KV stream",
      subject: subject,
      fieldPath: "config.mirror.name",
    );
  }
  if (mirror != null) {
    try {
      validateJetStreamKeyValueBucket(mirror.name.substring(3));
    } on ArgumentError catch (error) {
      throw JetStreamProtocolException(
        "KV mirror source has an invalid bucket name",
        subject: subject,
        fieldPath: "config.mirror.name",
        cause: error,
      );
    }
  }
  final expectedSubject = "\$KV.$bucket.>";
  final validSubjects = mirror == null
      ? config.subjects.length == 1 && config.subjects.single == expectedSubject
      : config.subjects.isEmpty && config.raw["mirror_direct"] == true;
  if (!validSubjects) {
    throw JetStreamProtocolException(
      "KV backing stream has an invalid subject binding",
      subject: subject,
      fieldPath: "config.subjects",
    );
  }
  try {
    final history = _integer(
      config.raw["max_msgs_per_subject"] ?? BigInt.one,
      "config.max_msgs_per_subject",
    );
    if (history < BigInt.one || history > BigInt.from(64)) {
      throw ArgumentError.value(history, "history", "Must be 1 through 64");
    }
    final maximumAge = _duration(config.raw["max_age"], "config.max_age");
    final markerLifetime = _duration(
      config.raw["subject_delete_marker_ttl"],
      "config.subject_delete_marker_ttl",
    );
    return JetStreamKeyValueStatus(
      bucket: bucket,
      values: info.state.messages,
      sizeBytes: info.state.bytes,
      history: history.toInt(),
      maximumAge: maximumAge,
      markerLifetime: markerLifetime,
      storageWireValue: config.storageWireValue,
      knownStorage: config.knownStorage,
      replicas: config.replicas,
      description: config.description,
      maximumBytes: _integer(
        config.raw["max_bytes"] ?? BigInt.from(-1),
        "config.max_bytes",
      ),
      maximumValueBytes: _integer(
        config.raw["max_msg_size"] ?? BigInt.from(-1),
        "config.max_msg_size",
      ),
      placement: config.placement,
      republish: _republish(config.raw["republish"]),
      mirror: mirror,
      sources: sources,
      metadata: config.metadata,
      compressionWireValue: _compression(config.raw["compression"]),
      streamInfo: info,
    );
  } on JetStreamProtocolException {
    rethrow;
  } on Object catch (error) {
    throw JetStreamProtocolException(
      "KV backing stream configuration is malformed",
      subject: subject,
      fieldPath: "config",
      cause: error,
    );
  }
}

JetStreamNanoseconds? _duration(Object? value, String name) {
  if (value == null) return null;
  final nanoseconds = _integer(value, name);
  if (nanoseconds.isNegative) {
    throw ArgumentError.value(nanoseconds, name, "Must not be negative");
  }
  return nanoseconds == BigInt.zero ? null : JetStreamNanoseconds(nanoseconds);
}

String _compression(Object? value) {
  if (value == null) return "none";
  if (value is String && value.isNotEmpty) return value;
  throw ArgumentError.value(value, "compression", "Must be a string");
}

JetStreamRepublish? _republish(Object? value) {
  if (value == null) return null;
  if (value is! Map<String, Object?>) {
    throw ArgumentError.value(value, "republish", "Must be an object");
  }
  final source = value["src"];
  final destination = value["dest"];
  final headersOnly = value["headers_only"] ?? false;
  if ((source != null && source is! String) ||
      destination is! String ||
      headersOnly is! bool) {
    throw ArgumentError.value(value, "republish", "Invalid republish mapping");
  }
  return JetStreamRepublish(
    source: source as String?,
    destination: destination,
    headersOnly: headersOnly,
  );
}

BigInt _integer(Object value, String name) => switch (value) {
  BigInt() => value,
  int() => BigInt.from(value),
  _ => throw ArgumentError.value(value, name, "Must be an integer"),
};
