import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/consumer_validation.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/identifiers.dart";
import "package:nats_jetstream/src/subject_pattern_set.dart";
import "package:nats_jetstream/src/subject_transform_validation.dart";
import "package:nats_jetstream/src/timestamp.dart";
import "package:nats_jetstream/src/wire_integer.dart";

/// Placement constraints for one stream.
final class JetStreamPlacement {
  /// Creates immutable placement constraints.
  factory JetStreamPlacement({String? cluster, List<String> tags = const []}) {
    if (cluster != null && cluster.isEmpty) {
      throw ArgumentError.value(cluster, "cluster", "Must not be empty");
    }
    if (tags.any((tag) => tag.isEmpty)) {
      throw ArgumentError.value(tags, "tags", "Must not contain empty tags");
    }
    return JetStreamPlacement._(cluster, List.unmodifiable(tags));
  }

  const JetStreamPlacement._(this.cluster, this.tags);

  /// Preferred cluster.
  final String? cluster;

  /// Required server tags.
  final List<String> tags;

  /// Exact JSON representation.
  Map<String, Object?> toJson() => {
    if (cluster != null) "cluster": cluster,
    if (tags.isNotEmpty) "tags": tags,
  };
}

/// One subject transformation.
final class JetStreamSubjectTransform {
  /// Creates a validated transformation.
  factory JetStreamSubjectTransform({
    required String source,
    required String destination,
  }) {
    validateJetStreamSubjectTransform(source, destination);
    return JetStreamSubjectTransform._(source, destination);
  }

  const JetStreamSubjectTransform._(this.source, this.destination);

  /// Source subject pattern.
  final String source;

  /// Destination subject pattern.
  final String destination;

  /// Exact JSON representation.
  Map<String, Object?> toJson() => {"src": source, "dest": destination};
}

/// Immediate republish configuration.
final class JetStreamRepublish {
  /// Creates a validated republish mapping.
  factory JetStreamRepublish({
    String? source,
    required String destination,
    bool headersOnly = false,
  }) {
    validateJetStreamSubjectTransform(source ?? ">", destination);
    return JetStreamRepublish._(source, destination, headersOnly);
  }

  const JetStreamRepublish._(this.source, this.destination, this.headersOnly);

  /// Optional source subject pattern.
  final String? source;

  /// Destination subject pattern.
  final String destination;

  /// Whether only headers are republished.
  final bool headersOnly;

  /// Exact JSON representation.
  Map<String, Object?> toJson() => {
    if (source != null) "src": source,
    "dest": destination,
    if (headersOnly) "headers_only": true,
  };
}

/// Consumer defaults and limits attached to a stream.
final class JetStreamStreamConsumerLimits {
  /// Creates stream consumer limits.
  factory JetStreamStreamConsumerLimits({
    BigInt? inactiveThresholdNanoseconds,
    int? maximumAckPending,
  }) {
    if (inactiveThresholdNanoseconds != null &&
        inactiveThresholdNanoseconds <= BigInt.zero) {
      throw ArgumentError.value(
        inactiveThresholdNanoseconds,
        "inactiveThresholdNanoseconds",
        "Must be positive",
      );
    }
    if (inactiveThresholdNanoseconds != null) {
      validatePositiveJetStreamDuration(
        inactiveThresholdNanoseconds,
        "inactiveThresholdNanoseconds",
      );
    }
    if (maximumAckPending != null &&
        maximumAckPending != -1 &&
        maximumAckPending <= 0) {
      throw ArgumentError.value(
        maximumAckPending,
        "maximumAckPending",
        "Must be positive or negative one",
      );
    }
    return JetStreamStreamConsumerLimits._(
      inactiveThresholdNanoseconds,
      maximumAckPending,
    );
  }

  const JetStreamStreamConsumerLimits._(
    this.inactiveThresholdNanoseconds,
    this.maximumAckPending,
  );

  /// Consumer inactivity default in exact nanoseconds.
  final BigInt? inactiveThresholdNanoseconds;

  /// Maximum unacknowledged messages.
  final int? maximumAckPending;

  /// Exact JSON representation.
  Map<String, Object?> toJson() => {
    if (inactiveThresholdNanoseconds != null)
      "inactive_threshold": inactiveThresholdNanoseconds,
    if (maximumAckPending != null) "max_ack_pending": maximumAckPending,
  };
}

/// Cross account source routing.
final class JetStreamExternalStream {
  /// Creates validated external routing.
  factory JetStreamExternalStream({
    required String apiPrefix,
    required String deliverPrefix,
  }) {
    validateJetStreamApiPrefix(apiPrefix);
    validatePublishSubject("$deliverPrefix.PROBE");
    return JetStreamExternalStream._(apiPrefix, deliverPrefix);
  }

  const JetStreamExternalStream._(this.apiPrefix, this.deliverPrefix);

  /// Imported JetStream API prefix.
  final String apiPrefix;

  /// Imported delivery prefix.
  final String deliverPrefix;

  /// Exact JSON representation.
  Map<String, Object?> toJson() => {"api": apiPrefix, "deliver": deliverPrefix};
}

/// Durable consumer identity used by NATS 2.14 reliable sourcing.
final class JetStreamStreamConsumerSource {
  /// Creates a validated consumer identity.
  factory JetStreamStreamConsumerSource({
    String? name,
    String? deliverSubject,
  }) {
    if (name == null || deliverSubject == null) {
      throw ArgumentError("Both name and deliverSubject are required");
    }
    validateJetStreamConsumerName(name);
    validatePublishSubject(deliverSubject);
    return JetStreamStreamConsumerSource._(name, deliverSubject);
  }

  const JetStreamStreamConsumerSource._(this.name, this.deliverSubject);

  /// Consumer name.
  final String? name;

  /// Consumer delivery subject.
  final String? deliverSubject;

  /// Exact JSON representation.
  Map<String, Object?> toJson() => {
    if (name != null) "name": name,
    if (deliverSubject != null) "deliver_subject": deliverSubject,
  };
}

/// One source or mirror stream definition.
final class JetStreamStreamSource {
  /// Creates a validated source definition.
  factory JetStreamStreamSource({
    required String name,
    BigInt? startSequence,
    DateTime? startTime,
    String? filterSubject,
    List<JetStreamSubjectTransform> subjectTransforms = const [],
    JetStreamExternalStream? external,
    JetStreamStreamConsumerSource? consumer,
  }) {
    validateJetStreamStreamName(name);
    if (startSequence != null) {
      validatePositiveJetStreamUint64(startSequence, "startSequence");
    }
    if (startSequence != null && startTime != null) {
      throw ArgumentError("startSequence and startTime are mutually exclusive");
    }
    if (startTime != null && !startTime.isUtc) {
      throw ArgumentError.value(startTime, "startTime", "Must be UTC");
    }
    if (startTime != null) {
      serializeJetStreamRfc3339(startTime, "startTime");
    }
    if (filterSubject != null) {
      validateSubscriptionSubject(filterSubject);
    }
    if (filterSubject != null && subjectTransforms.isNotEmpty) {
      throw ArgumentError(
        "filterSubject and subjectTransforms are mutually exclusive",
      );
    }
    validateDisjointNatsSubjectPatterns(
      subjectTransforms
          .map((transform) => transform.source.isEmpty ? ">" : transform.source)
          .toList(),
      name: "subjectTransforms",
    );
    if (consumer != null &&
        (startSequence != null ||
            startTime != null ||
            filterSubject != null ||
            subjectTransforms.isNotEmpty)) {
      throw ArgumentError(
        "A durable source consumer cannot use start or filter options",
      );
    }
    return JetStreamStreamSource._(
      name,
      startSequence,
      startTime,
      filterSubject,
      List.unmodifiable(subjectTransforms),
      external,
      consumer,
    );
  }

  const JetStreamStreamSource._(
    this.name,
    this.startSequence,
    this.startTime,
    this.filterSubject,
    this.subjectTransforms,
    this.external,
    this.consumer,
  );

  /// Source stream name.
  final String name;

  /// Optional first source sequence.
  final BigInt? startSequence;

  /// Optional first source time.
  final DateTime? startTime;

  /// Optional source subject filter.
  final String? filterSubject;

  /// Source subject transformations.
  final List<JetStreamSubjectTransform> subjectTransforms;

  /// Optional cross account routing.
  final JetStreamExternalStream? external;

  /// Optional NATS 2.14 durable consumer identity.
  final JetStreamStreamConsumerSource? consumer;

  /// Exact JSON representation.
  Map<String, Object?> toJson() => immutableExactJsonObject({
    "name": name,
    if (startSequence != null) "opt_start_seq": startSequence,
    if (startTime != null)
      "opt_start_time": serializeJetStreamRfc3339(startTime!, "startTime"),
    if (filterSubject != null) "filter_subject": filterSubject,
    if (subjectTransforms.isNotEmpty)
      "subject_transforms": subjectTransforms
          .map((value) => value.toJson())
          .toList(),
    if (external != null) "external": external!.toJson(),
    if (consumer != null) "consumer": consumer!.toJson(),
  });
}
