import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/object_store.dart";
import "package:nats_jetstream/src/stream_models.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// Returns the backing stream name for [bucket].
String objectStreamName(String bucket) => "OBJ_$bucket";

/// Returns the two protocol subjects owned by [bucket].
List<String> objectStreamSubjects(String bucket) => [
  "\$O.$bucket.C.>",
  "\$O.$bucket.M.>",
];

/// Verifies one response is the exact requested Object Store bucket.
void verifyObjectStoreIdentity(JetStreamWireResponse response, String bucket) {
  final config = response.requiredObject("config");
  final name = response.requiredString("config.name", source: config);
  final subjects = response.requiredStringList(
    "config.subjects",
    source: config,
  );
  _verifyIdentity(
    name,
    subjects,
    config: config,
    bucket: bucket,
    subject: response.subject,
  );
}

/// Verifies and returns the bucket represented by [info].
String verifyObjectStoreStreamInfo(
  JetStreamStreamInfo info, {
  String? bucket,
  required String subject,
}) {
  final stream = info.config.name;
  if (!stream.startsWith("OBJ_") || stream.length == 4) {
    throw JetStreamProtocolException(
      "Stream is not an Object Store bucket",
      subject: subject,
      fieldPath: "config.name",
    );
  }
  final actualBucket = stream.substring(4);
  try {
    validateJetStreamObjectBucket(actualBucket);
  } on ArgumentError catch (error) {
    throw JetStreamProtocolException(
      "Object Store backing stream has an invalid bucket name",
      subject: subject,
      fieldPath: "config.name",
      cause: error,
    );
  }
  _verifyIdentity(
    stream,
    info.config.subjects,
    config: info.config.raw,
    bucket: bucket ?? actualBucket,
    subject: subject,
  );
  return actualBucket;
}

/// Decodes one already verified backing stream snapshot.
JetStreamObjectStoreStatus decodeObjectStoreStatus(
  JetStreamStreamInfo info, {
  required String subject,
  String? expectedBucket,
}) {
  final bucket = verifyObjectStoreStreamInfo(
    info,
    bucket: expectedBucket,
    subject: subject,
  );
  final config = info.config;
  try {
    final maximumAge = _integer(config.raw["max_age"] ?? BigInt.zero);
    if (maximumAge.isNegative) {
      throw ArgumentError.value(maximumAge, "max_age", "Must not be negative");
    }
    final maximumBytes = _integer(config.raw["max_bytes"] ?? BigInt.from(-1));
    if (maximumBytes < BigInt.from(-1)) {
      throw ArgumentError.value(
        maximumBytes,
        "max_bytes",
        "Must be negative one or greater",
      );
    }
    final compression = config.raw["compression"] ?? "none";
    if (compression != "none" && compression != "s2") {
      throw ArgumentError.value(
        compression,
        "compression",
        "Must be none or s2",
      );
    }
    final sealed = config.raw["sealed"] ?? false;
    if (sealed is! bool) {
      throw ArgumentError.value(sealed, "sealed", "Must be a boolean");
    }
    final experimentalFastIngest = config.raw["allow_batched"] ?? false;
    if (experimentalFastIngest is! bool) {
      throw ArgumentError.value(
        experimentalFastIngest,
        "allow_batched",
        "Must be a boolean",
      );
    }
    final storage = config.knownStorage;
    if (storage == null) {
      throw ArgumentError.value(
        config.storageWireValue,
        "storage",
        "Unknown storage implementation",
      );
    }
    return JetStreamObjectStoreStatus(
      name: bucket,
      description: config.description,
      maximumAge: JetStreamNanoseconds(maximumAge),
      maximumBytes: maximumBytes,
      storage: storage,
      replicas: config.replicas,
      sealed: sealed,
      storageBytes: info.state.bytes,
      streamMessages: info.state.messages,
      metadata: {
        for (final entry in config.metadata.entries)
          if (!entry.key.startsWith("_nats.")) entry.key: entry.value,
      },
      systemMetadata: {
        for (final entry in config.metadata.entries)
          if (entry.key.startsWith("_nats.")) entry.key: entry.value,
      },
      compression: compression == "s2",
      placement: config.placement,
      experimentalFastIngest: experimentalFastIngest,
    );
  } on JetStreamProtocolException {
    rethrow;
  } on Object catch (error) {
    throw JetStreamProtocolException(
      "Object Store backing stream configuration is malformed",
      subject: subject,
      fieldPath: "config",
      cause: error,
    );
  }
}

void _verifyIdentity(
  String stream,
  List<String> subjects, {
  required Map<String, Object?> config,
  required String bucket,
  required String subject,
}) {
  final expected = objectStreamSubjects(bucket).toSet();
  final sealed = config["sealed"] == true;
  if (stream != objectStreamName(bucket) ||
      subjects.length != expected.length ||
      !subjects.toSet().containsAll(expected) ||
      (config["retention"] ?? "limits") != "limits" ||
      config["discard"] != "new" ||
      config["allow_rollup_hdrs"] != !sealed ||
      config["allow_direct"] != true) {
    throw JetStreamProtocolException(
      "Stream is not the requested Object Store bucket",
      subject: subject,
      fieldPath: "config",
    );
  }
}

BigInt _integer(Object value) => switch (value) {
  BigInt() => value,
  int() => BigInt.from(value),
  _ => throw ArgumentError.value(value, "value", "Must be an integer"),
};
