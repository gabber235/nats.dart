import "dart:convert";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/identifiers.dart";
import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/stream_source.dart";
import "package:nats_jetstream/src/subject_pattern_set.dart";
import "package:nats_jetstream/src/user_metadata.dart";
import "package:nats_jetstream/src/wire_integer.dart";

/// Stream storage implementation.
enum JetStreamStorage {
  /// File backed storage.
  file("file"),

  /// Process memory storage.
  memory("memory");

  const JetStreamStorage(this.wireValue);

  /// JSON API value.
  final String wireValue;
}

/// Stream retention policy.
enum JetStreamRetentionPolicy {
  /// Retain until a configured limit is reached.
  limits("limits"),

  /// Retain while any consumer has interest.
  interest("interest"),

  /// Remove after the first consumer acknowledgement.
  workQueue("workqueue");

  const JetStreamRetentionPolicy(this.wireValue);

  /// JSON API value.
  final String wireValue;
}

/// Behavior when a stream reaches a storage limit.
enum JetStreamDiscardPolicy {
  /// Remove old messages.
  oldMessages("old"),

  /// Reject new messages.
  newMessages("new");

  const JetStreamDiscardPolicy(this.wireValue);

  /// JSON API value.
  final String wireValue;
}

/// Stream storage compression.
enum JetStreamCompression {
  /// Disable compression.
  none("none"),

  /// Use S2 compression.
  s2("s2");

  const JetStreamCompression(this.wireValue);

  /// JSON API value.
  final String wireValue;
}

/// Stream persistence acknowledgement mode.
enum JetStreamPersistMode {
  /// Acknowledge after the normal persistence boundary.
  defaultMode("default"),

  /// Permit acknowledgement before an asynchronous disk flush.
  asynchronous("async");

  const JetStreamPersistMode(this.wireValue);

  /// JSON API value.
  final String wireValue;
}

/// Immutable, forward compatible stream configuration.
final class JetStreamStreamConfig {
  /// Creates and validates a stream configuration.
  factory JetStreamStreamConfig({
    required String name,
    List<String> subjects = const [],
    String? description,
    JetStreamRetentionPolicy retention = JetStreamRetentionPolicy.limits,
    int maximumConsumers = -1,
    BigInt? maximumMessages,
    BigInt? maximumBytes,
    JetStreamDiscardPolicy discard = JetStreamDiscardPolicy.oldMessages,
    bool discardNewPerSubject = false,
    JetStreamNanoseconds? maximumAge,
    BigInt? maximumMessagesPerSubject,
    int maximumMessageSize = -1,
    JetStreamStorage storage = JetStreamStorage.file,
    int replicas = 1,
    bool noAcknowledgements = false,
    JetStreamNanoseconds? duplicateWindow,
    JetStreamPlacement? placement,
    JetStreamStreamSource? mirror,
    List<JetStreamStreamSource> sources = const [],
    bool sealed = false,
    bool denyDelete = false,
    bool denyPurge = false,
    bool allowRollup = false,
    JetStreamCompression compression = JetStreamCompression.none,
    BigInt? firstSequence,
    JetStreamSubjectTransform? subjectTransform,
    JetStreamRepublish? republish,
    bool allowDirect = false,
    bool mirrorDirect = false,
    JetStreamStreamConsumerLimits? consumerLimits,
    Map<String, String> metadata = const {},
    String? templateOwner,
    bool allowMessageTtl = false,
    JetStreamNanoseconds? subjectDeleteMarkerTtl,
    bool allowMessageCounter = false,
    bool allowAtomicPublish = false,
    bool allowMessageSchedules = false,
    JetStreamPersistMode persistMode = JetStreamPersistMode.defaultMode,
    bool allowFastBatchPublish = false,
  }) {
    final capturedMetadata = captureJetStreamUserMetadata(metadata);
    final maxMessages = maximumMessages ?? BigInt.from(-1);
    final maxBytes = maximumBytes ?? BigInt.from(-1);
    final maxPerSubject = maximumMessagesPerSubject ?? BigInt.from(-1);
    _validateConfig(
      name: name,
      subjects: subjects,
      maximumConsumers: maximumConsumers,
      maximumMessages: maxMessages,
      maximumBytes: maxBytes,
      discard: discard,
      discardNewPerSubject: discardNewPerSubject,
      maximumAge: maximumAge,
      maximumMessagesPerSubject: maxPerSubject,
      maximumMessageSize: maximumMessageSize,
      replicas: replicas,
      duplicateWindow: duplicateWindow,
      mirror: mirror,
      sources: sources,
      firstSequence: firstSequence,
      subjectDeleteMarkerTtl: subjectDeleteMarkerTtl,
    );
    return JetStreamStreamConfig.fromJson({
      "name": name,
      "description": ?description,
      if (subjects.isNotEmpty) "subjects": List<String>.from(subjects),
      "retention": retention.wireValue,
      "max_consumers": maximumConsumers,
      "max_msgs": maxMessages,
      "max_bytes": maxBytes,
      "discard": discard.wireValue,
      if (discardNewPerSubject) "discard_new_per_subject": true,
      "max_age": maximumAge?.nanoseconds ?? BigInt.zero,
      "max_msgs_per_subject": maxPerSubject,
      "max_msg_size": maximumMessageSize,
      "storage": storage.wireValue,
      "num_replicas": replicas,
      if (noAcknowledgements) "no_ack": true,
      "duplicate_window": ?duplicateWindow?.nanoseconds,
      "placement": ?placement?.toJson(),
      "mirror": ?mirror?.toJson(),
      if (sources.isNotEmpty)
        "sources": sources.map((value) => value.toJson()).toList(),
      if (sealed) "sealed": true,
      if (denyDelete) "deny_delete": true,
      if (denyPurge) "deny_purge": true,
      if (allowRollup) "allow_rollup_hdrs": true,
      "compression": compression.wireValue,
      "first_seq": ?firstSequence,
      if (subjectTransform != null)
        "subject_transform": subjectTransform.toJson(),
      if (republish != null) "republish": republish.toJson(),
      "allow_direct": allowDirect,
      "mirror_direct": mirrorDirect,
      if (consumerLimits != null) "consumer_limits": consumerLimits.toJson(),
      if (capturedMetadata.isNotEmpty) "metadata": capturedMetadata,
      "template_owner": ?templateOwner,
      if (allowMessageTtl) "allow_msg_ttl": true,
      if (subjectDeleteMarkerTtl != null)
        "subject_delete_marker_ttl": subjectDeleteMarkerTtl.nanoseconds,
      if (allowMessageCounter) "allow_msg_counter": true,
      if (allowAtomicPublish) "allow_atomic": true,
      if (allowMessageSchedules) "allow_msg_schedules": true,
      if (persistMode != JetStreamPersistMode.defaultMode)
        "persist_mode": persistMode.wireValue,
      if (allowFastBatchPublish) "allow_batched": true,
    });
  }

  /// Creates exact stream JSON while preserving unknown fields.
  factory JetStreamStreamConfig.fromJson(Map<String, Object?> json) {
    final raw = immutableExactJsonObject(json);
    _validateRaw(raw);
    return JetStreamStreamConfig._(raw);
  }

  const JetStreamStreamConfig._(this.raw);

  /// Deeply immutable exact JSON, including unknown future fields.
  final Map<String, Object?> raw;

  /// Stream name.
  String get name => raw["name"]! as String;

  /// Optional description.
  String? get description => raw["description"] as String?;

  /// Captured subjects.
  List<String> get subjects => List.unmodifiable(
    (raw["subjects"] as List<Object?>? ?? const []).cast<String>(),
  );

  /// Storage implementation.
  JetStreamStorage get storage =>
      _enumValue(JetStreamStorage.values, raw["storage"] ?? "file");

  /// Exact storage wire value, including values added by newer servers.
  String get storageWireValue => raw["storage"] as String? ?? "file";

  /// Known storage implementation, or `null` for a newer server value.
  JetStreamStorage? get knownStorage =>
      _knownEnumValue(JetStreamStorage.values, storageWireValue);

  /// Configured stream replica count.
  int get replicas =>
      _integer(raw["num_replicas"] ?? 1, "num_replicas").toInt();

  /// Placement constraints.
  JetStreamPlacement? get placement {
    final value = raw["placement"] as Map<String, Object?>?;
    return value == null
        ? null
        : JetStreamPlacement(
            cluster: value["cluster"] as String?,
            tags: List.unmodifiable(
              (value["tags"] as List<Object?>? ?? const []).cast<String>(),
            ),
          );
  }

  /// Application metadata.
  Map<String, String> get metadata => Map.unmodifiable(
    (raw["metadata"] as Map<String, Object?>? ?? const {})
        .cast<String, String>(),
  );

  /// Enables per message TTL headers.
  bool get allowMessageTtl => raw["allow_msg_ttl"] as bool? ?? false;

  /// Retention for subject delete markers.
  JetStreamNanoseconds? get subjectDeleteMarkerTtl =>
      _duration("subject_delete_marker_ttl");

  /// Enables message counters.
  bool get allowMessageCounter => raw["allow_msg_counter"] as bool? ?? false;

  /// Enables atomic publish batches.
  bool get allowAtomicPublish => raw["allow_atomic"] as bool? ?? false;

  /// Enables server message schedules.
  bool get allowMessageSchedules =>
      raw["allow_msg_schedules"] as bool? ?? false;

  /// Enables non atomic fast batch ingest.
  bool get allowFastBatchPublish => raw["allow_batched"] as bool? ?? false;

  /// Returns exact JSON suitable for create or update.
  Map<String, Object?> toJson() => raw;

  /// Copies common editable fields while retaining unknown future fields.
  ///
  /// Set [clearMirror] with [subjects] to promote a mirror to a normal stream.
  JetStreamStreamConfig copyWith({
    String? description,
    bool clearDescription = false,
    List<String>? subjects,
    bool clearMirror = false,
  }) {
    if (description != null && clearDescription) {
      throw ArgumentError("Description cannot be set and cleared together");
    }
    final updated = Map<String, Object?>.from(raw);
    if (description != null) {
      updated["description"] = description;
    } else if (clearDescription) {
      updated.remove("description");
    }
    if (subjects != null) {
      updated["subjects"] = List<String>.from(subjects);
    }
    if (clearMirror) {
      updated
        ..remove("mirror")
        ..["mirror_direct"] = false;
    }
    return JetStreamStreamConfig.fromJson(updated);
  }

  JetStreamNanoseconds? _duration(String key) {
    final value = raw[key];
    return value == null ? null : JetStreamNanoseconds(_integer(value, key));
  }
}

T _enumValue<T>(Iterable<T> values, Object value) {
  final known = _knownEnumValue(values, value);
  if (known != null) {
    return known;
  }
  throw ArgumentError.value(value, "value", "Unknown stream enum value");
}

T? _knownEnumValue<T>(Iterable<T> values, Object value) {
  if (value is! String) {
    throw ArgumentError.value(value, "value", "Stream enum must be a string");
  }
  for (final candidate in values) {
    final wire = switch (candidate) {
      final JetStreamStorage item => item.wireValue,
      final JetStreamRetentionPolicy item => item.wireValue,
      final JetStreamDiscardPolicy item => item.wireValue,
      final JetStreamCompression item => item.wireValue,
      final JetStreamPersistMode item => item.wireValue,
      _ => null,
    };
    if (wire == value) {
      return candidate;
    }
  }
  return null;
}

BigInt _integer(Object value, String name) => switch (value) {
  BigInt() => value,
  int() => BigInt.from(value),
  _ => throw ArgumentError.value(value, name, "Must be an integer"),
};

void _validateRaw(Map<String, Object?> raw) {
  final name = raw["name"];
  if (name is! String) {
    throw ArgumentError.value(name, "name", "Must be a string");
  }
  validateJetStreamStreamName(name);
  final subjectsValue = raw["subjects"];
  if (subjectsValue != null && subjectsValue is! List<Object?>) {
    throw ArgumentError.value(subjectsValue, "subjects", "Must be a list");
  }
  final subjects = subjectsValue as List<Object?>? ?? const [];
  if (subjects.any((value) => value is! String)) {
    throw ArgumentError.value(subjects, "subjects", "Must contain strings");
  }
  for (final subject in subjects.cast<String>()) {
    validateSubscriptionSubject(subject);
  }
  if (subjects.isNotEmpty && raw["mirror"] != null) {
    throw ArgumentError("subjects and mirror are mutually exclusive");
  }
  final storage = _knownEnumValue(
    JetStreamStorage.values,
    raw["storage"] ?? "file",
  );
  final retention = _knownEnumValue(
    JetStreamRetentionPolicy.values,
    raw["retention"] ?? "limits",
  );
  final discard = _knownEnumValue(
    JetStreamDiscardPolicy.values,
    raw["discard"] ?? "old",
  );
  _knownEnumValue(JetStreamCompression.values, raw["compression"] ?? "none");
  final persistMode = _knownEnumValue(
    JetStreamPersistMode.values,
    raw["persist_mode"] ?? "default",
  );
  _validateRawText(raw);
  _validateRawLimits(raw);
  _validateRawSourceRanges(raw);
  _validateRawFeatures(
    raw,
    subjects: subjects.cast<String>(),
    storage: storage,
    retention: retention,
    discard: discard,
    persistMode: persistMode,
  );
}

void _validateRawSourceRanges(Map<String, Object?> raw) {
  final rawSources = raw["sources"];
  final sources = <Object?>[
    raw["mirror"],
    if (rawSources is List<Object?>) ...rawSources,
  ];
  for (final source in sources) {
    if (source is! Map<String, Object?>) continue;
    if (source["opt_start_seq"] case final Object value) {
      validatePositiveJetStreamUint64(
        _integer(value, "opt_start_seq"),
        "opt_start_seq",
      );
    }
  }
}

void _validateRawText(Map<String, Object?> raw) {
  final description = raw["description"];
  if (description != null && description is! String) {
    throw ArgumentError.value(description, "description", "Must be a string");
  }
  if (description is String && utf8.encode(description).length > 4096) {
    throw ArgumentError.value(
      description,
      "description",
      "Must not exceed 4096 UTF8 bytes",
    );
  }
  final metadata = raw["metadata"];
  if (metadata != null &&
      (metadata is! Map<String, Object?> ||
          metadata.values.any((value) => value is! String))) {
    throw ArgumentError.value(metadata, "metadata", "Must be a string map");
  }
  if (metadata is Map<String, Object?>) {
    final bytes = metadata.entries.fold<int>(
      0,
      (total, entry) =>
          total +
          utf8.encode(entry.key).length +
          utf8.encode(entry.value! as String).length,
    );
    if (bytes > 128 * 1024) {
      throw ArgumentError.value(
        metadata,
        "metadata",
        "Must not exceed 128 KiB",
      );
    }
  }
}

void _validateRawLimits(Map<String, Object?> raw) {
  for (final field in [
    "max_consumers",
    "max_msgs",
    "max_bytes",
    "max_msgs_per_subject",
  ]) {
    validateJetStreamLimit64(_rawInteger(raw, field, -BigInt.one), field);
  }
  validateJetStreamMessageSize(
    _rawInteger(raw, "max_msg_size", -BigInt.one),
    "max_msg_size",
  );
  final replicas = _rawInteger(raw, "num_replicas", BigInt.one);
  if (replicas.isNegative || replicas > BigInt.from(5)) {
    throw ArgumentError.value(
      replicas,
      "num_replicas",
      "Must be between zero and five",
    );
  }
  final maximumAge = _rawInteger(raw, "max_age", BigInt.zero);
  final duplicateWindow = _rawInteger(raw, "duplicate_window", BigInt.zero);
  validateJetStreamDuration(maximumAge, "max_age");
  validateJetStreamDuration(duplicateWindow, "duplicate_window");
  if (raw["first_seq"] case final Object value) {
    validateJetStreamUint64(_integer(value, "first_seq"), "first_seq");
  }
  final minimumWindow = BigInt.from(100000000);
  if ((maximumAge > BigInt.zero && maximumAge < minimumWindow) ||
      (duplicateWindow > BigInt.zero && duplicateWindow < minimumWindow)) {
    throw ArgumentError(
      "Maximum age and duplicate window must be at least 100 ms",
    );
  }
  if (maximumAge > BigInt.zero && duplicateWindow > maximumAge) {
    throw ArgumentError("Duplicate window must not exceed maximum age");
  }
  if (raw["subject_delete_marker_ttl"] case final Object value) {
    validateJetStreamDuration(
      _integer(value, "subject_delete_marker_ttl"),
      "subject_delete_marker_ttl",
    );
  }
  if (raw["consumer_limits"] case final Map<String, Object?> limits) {
    if (limits["inactive_threshold"] case final Object value) {
      validateJetStreamDuration(
        _integer(value, "consumer_limits.inactive_threshold"),
        "consumer_limits.inactive_threshold",
      );
    }
    if (limits["max_ack_pending"] case final Object value) {
      validateJetStreamLimit64(
        _integer(value, "consumer_limits.max_ack_pending"),
        "consumer_limits.max_ack_pending",
      );
    }
  }
}

void _validateRawFeatures(
  Map<String, Object?> raw, {
  required List<String> subjects,
  required JetStreamStorage? storage,
  required JetStreamRetentionPolicy? retention,
  required JetStreamDiscardPolicy? discard,
  required JetStreamPersistMode? persistMode,
}) {
  final replicas = _rawInteger(raw, "num_replicas", BigInt.one);
  final perSubject = _rawInteger(raw, "max_msgs_per_subject", BigInt.from(-1));
  final mirror = raw["mirror"];
  final sourcesValue = raw["sources"];
  if (sourcesValue != null && sourcesValue is! List<Object?>) {
    throw ArgumentError.value(sourcesValue, "sources", "Must be a list");
  }
  final sources = sourcesValue as List<Object?>? ?? const [];
  final allowRollup = _rawBoolean(raw, "allow_rollup_hdrs");
  final allowTtl = _rawBoolean(raw, "allow_msg_ttl");
  final allowCounter = _rawBoolean(raw, "allow_msg_counter");
  final allowAtomic = _rawBoolean(raw, "allow_atomic");
  final allowSchedules = _rawBoolean(raw, "allow_msg_schedules");
  final allowBatched = _rawBoolean(raw, "allow_batched");
  validateDisjointNatsSubjectPatterns(subjects);
  if (subjects.contains(">") &&
      (!_rawBoolean(raw, "no_ack") || replicas != BigInt.one)) {
    throw ArgumentError(
      "The full wildcard requires no acknowledgements and one replica",
    );
  }
  if (_rawBoolean(raw, "discard_new_per_subject") &&
      (discard != null && discard != JetStreamDiscardPolicy.newMessages ||
          perSubject <= BigInt.zero)) {
    throw ArgumentError(
      "Per subject discard requires discard new and a positive limit",
    );
  }
  if (_rawBoolean(raw, "deny_purge") && allowRollup) {
    throw ArgumentError("Deny purge and rollup are mutually exclusive");
  }
  final marker = _rawInteger(raw, "subject_delete_marker_ttl", BigInt.zero);
  if (marker > BigInt.zero &&
      (marker < BigInt.from(1000000000) || !allowTtl || mirror != null)) {
    throw ArgumentError(
      "Subject delete markers require TTL and at least one second",
    );
  }
  if (allowCounter &&
      (discard == JetStreamDiscardPolicy.newMessages ||
          allowTtl ||
          allowSchedules ||
          retention != null && retention != JetStreamRetentionPolicy.limits)) {
    throw ArgumentError("Counter stream configuration is incompatible");
  }
  if (allowSchedules &&
      (discard == JetStreamDiscardPolicy.newMessages ||
          sources.isNotEmpty ||
          mirror != null)) {
    throw ArgumentError("Scheduled stream configuration is incompatible");
  }
  if (persistMode == JetStreamPersistMode.asynchronous &&
      (storage != null && storage != JetStreamStorage.file ||
          replicas != BigInt.one ||
          allowAtomic)) {
    throw ArgumentError(
      "Asynchronous persistence requires file storage and one replica",
    );
  }
  if (mirror != null &&
      (subjects.isNotEmpty ||
          sources.isNotEmpty ||
          raw["first_seq"] != null ||
          allowCounter ||
          allowAtomic ||
          allowBatched ||
          allowSchedules ||
          marker > BigInt.zero)) {
    throw ArgumentError("Mirror stream configuration is incompatible");
  }
}

BigInt _rawInteger(Map<String, Object?> raw, String key, BigInt defaultValue) {
  final value = raw[key];
  return value == null ? defaultValue : _integer(value, key);
}

bool _rawBoolean(Map<String, Object?> raw, String key) {
  final value = raw[key];
  if (value == null) return false;
  if (value is bool) return value;
  throw ArgumentError.value(value, key, "Must be a boolean");
}

void _validateConfig({
  required String name,
  required List<String> subjects,
  required int maximumConsumers,
  required BigInt maximumMessages,
  required BigInt maximumBytes,
  required JetStreamDiscardPolicy discard,
  required bool discardNewPerSubject,
  required JetStreamNanoseconds? maximumAge,
  required BigInt maximumMessagesPerSubject,
  required int maximumMessageSize,
  required int replicas,
  required JetStreamNanoseconds? duplicateWindow,
  required JetStreamStreamSource? mirror,
  required List<JetStreamStreamSource> sources,
  required BigInt? firstSequence,
  required JetStreamNanoseconds? subjectDeleteMarkerTtl,
}) {
  validateJetStreamStreamName(name);
  for (final subject in subjects) {
    validateSubscriptionSubject(subject);
  }
  if (subjects.isNotEmpty && mirror != null) {
    throw ArgumentError("subjects and mirror are mutually exclusive");
  }
  if (mirror != null && sources.isNotEmpty) {
    throw ArgumentError("mirror and sources are mutually exclusive");
  }
  if (maximumConsumers < -1 ||
      maximumMessages < BigInt.from(-1) ||
      maximumBytes < BigInt.from(-1)) {
    throw ArgumentError("Stream limits must be negative one or nonnegative");
  }
  validateJetStreamLimit64(BigInt.from(maximumConsumers), "maximumConsumers");
  validateJetStreamLimit64(maximumMessages, "maximumMessages");
  validateJetStreamLimit64(maximumBytes, "maximumBytes");
  validateJetStreamLimit64(
    maximumMessagesPerSubject,
    "maximumMessagesPerSubject",
  );
  validateJetStreamMessageSize(
    BigInt.from(maximumMessageSize),
    "maximumMessageSize",
  );
  if (maximumMessagesPerSubject < BigInt.from(-1) || maximumMessageSize < -1) {
    throw ArgumentError(
      "Per message limits must be negative one or nonnegative",
    );
  }
  if (discardNewPerSubject &&
      (discard != JetStreamDiscardPolicy.newMessages ||
          maximumMessagesPerSubject <= BigInt.zero)) {
    throw ArgumentError(
      "discardNewPerSubject requires new message discard and a positive "
      "subject limit",
    );
  }
  if (replicas < 0 || replicas > 5) {
    throw ArgumentError.value(
      replicas,
      "replicas",
      "Must be between zero and five",
    );
  }
  for (final value in [maximumAge, duplicateWindow, subjectDeleteMarkerTtl]) {
    if (value != null && value.nanoseconds <= BigInt.zero) {
      throw ArgumentError.value(value, "duration", "Must be positive");
    }
    if (value != null) {
      validatePositiveJetStreamDuration(value.nanoseconds, "duration");
    }
  }
  if (firstSequence != null) {
    validatePositiveJetStreamUint64(firstSequence, "firstSequence");
  }
}
