import "package:nats_jetstream/src/stream_source.dart";
import "package:nats_jetstream/src/timestamp.dart";

/// Decodes an immutable stream source list from exact JSON.
List<JetStreamStreamSource> decodeKeyValueStreamSources(Object? value) {
  if (value == null) return const [];
  if (value is! List<Object?>) {
    throw ArgumentError.value(value, "sources", "Must be an array");
  }
  return List.unmodifiable(
    value.map((entry) => decodeKeyValueStreamSource(entry, "config.sources")!),
  );
}

/// Decodes one immutable stream source from exact JSON.
JetStreamStreamSource? decodeKeyValueStreamSource(Object? value, String name) {
  if (value == null) return null;
  if (value is! Map<String, Object?>) {
    throw ArgumentError.value(value, name, "Must be an object");
  }
  final stream = value["name"];
  final startSequence = value["opt_start_seq"];
  final startTime = value["opt_start_time"];
  final filterSubject = value["filter_subject"];
  final transforms = value["subject_transforms"];
  final external = value["external"];
  final consumer = value["consumer"];
  if (stream is! String ||
      (startSequence != null && startSequence is! BigInt) ||
      (startTime != null && startTime is! String) ||
      (filterSubject != null && filterSubject is! String) ||
      (transforms != null && transforms is! List<Object?>) ||
      (external != null && external is! Map<String, Object?>) ||
      (consumer != null && consumer is! Map<String, Object?>)) {
    throw ArgumentError.value(value, name, "Invalid stream source");
  }
  final validatedStartTime = startTime == null
      ? null
      : validateJetStreamRfc3339(startTime as String, "$name.opt_start_time");
  return JetStreamStreamSource(
    name: stream,
    startSequence: startSequence as BigInt?,
    startTime: validatedStartTime == null
        ? null
        : DateTime.parse(validatedStartTime).toUtc(),
    filterSubject: filterSubject as String?,
    subjectTransforms: [
      for (final transform in transforms as List<Object?>? ?? const [])
        _transform(transform),
    ],
    external: external == null
        ? null
        : _external(external as Map<String, Object?>),
    consumer: consumer == null
        ? null
        : _consumer(consumer as Map<String, Object?>),
  );
}

JetStreamSubjectTransform _transform(Object? value) {
  if (value is! Map<String, Object?> ||
      value["src"] is! String ||
      value["dest"] is! String) {
    throw ArgumentError.value(value, "subject_transforms", "Invalid mapping");
  }
  return JetStreamSubjectTransform(
    source: value["src"]! as String,
    destination: value["dest"]! as String,
  );
}

JetStreamExternalStream _external(Map<String, Object?> value) {
  final api = value["api"];
  final deliver = value["deliver"];
  if (api is! String || deliver is! String) {
    throw ArgumentError.value(value, "external", "Invalid external routing");
  }
  return JetStreamExternalStream(apiPrefix: api, deliverPrefix: deliver);
}

JetStreamStreamConsumerSource _consumer(Map<String, Object?> value) {
  final name = value["name"];
  final deliver = value["deliver_subject"];
  if (name is! String || deliver is! String) {
    throw ArgumentError.value(value, "consumer", "Invalid source consumer");
  }
  return JetStreamStreamConsumerSource(name: name, deliverSubject: deliver);
}
