import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/consumer_validation.dart";
import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/subject_pattern_set.dart";
import "package:nats_jetstream/src/timestamp.dart";
import "package:nats_jetstream/src/wire_integer.dart";

/// Consumer acknowledgement policy.
enum JetStreamConsumerAckPolicy {
  /// Delivery itself acknowledges the message.
  none("none"),

  /// One acknowledgement covers all earlier deliveries.
  all("all"),

  /// Every message requires an acknowledgement.
  explicit("explicit"),

  /// Server sourcing uses persistence flow control acknowledgements.
  flowControl("flow_control");

  const JetStreamConsumerAckPolicy(this.wireValue);

  /// JSON value.
  final String wireValue;
}

/// Consumer replay pacing.
enum JetStreamConsumerReplayPolicy {
  /// Deliver as quickly as possible.
  instant("instant"),

  /// Preserve original message timing.
  original("original");

  const JetStreamConsumerReplayPolicy(this.wireValue);

  /// JSON value.
  final String wireValue;
}

/// Latest pull consumer priority selection policy.
enum JetStreamConsumerPriorityPolicy {
  /// Requests use ordinary first available selection.
  none("none"),

  /// Requests become eligible after their overflow thresholds are met.
  overflow("overflow"),

  /// One client remains pinned while it renews within the timeout.
  pinnedClient("pinned_client"),

  /// Lower numeric request priority wins.
  prioritized("prioritized");

  const JetStreamConsumerPriorityPolicy(this.wireValue);

  /// JSON API value.
  final String wireValue;
}

/// Validated starting position for a consumer.
final class JetStreamConsumerStart {
  /// Starts at the first available stream message.
  const JetStreamConsumerStart.all()
    : policy = "all",
      sequence = null,
      time = null;

  /// Starts at the last available stream message.
  const JetStreamConsumerStart.last()
    : policy = "last",
      sequence = null,
      time = null;

  /// Starts after the current stream end.
  const JetStreamConsumerStart.newMessages()
    : policy = "new",
      sequence = null,
      time = null;

  /// Starts at the last message for every filtered subject.
  const JetStreamConsumerStart.lastPerSubject()
    : policy = "last_per_subject",
      sequence = null,
      time = null;

  /// Starts at one exact positive stream sequence.
  factory JetStreamConsumerStart.sequence(BigInt sequence) {
    validatePositiveJetStreamUint64(sequence, "sequence");
    return JetStreamConsumerStart._("by_start_sequence", sequence, null);
  }

  /// Starts at one exact UTC instant.
  factory JetStreamConsumerStart.time(DateTime time) {
    if (!time.isUtc) {
      throw ArgumentError.value(time, "time", "Must be UTC");
    }
    return JetStreamConsumerStart._(
      "by_start_time",
      null,
      serializeJetStreamRfc3339(time, "time"),
    );
  }

  const JetStreamConsumerStart._(this.policy, this.sequence, this.time);

  /// Delivery policy JSON value.
  final String policy;

  /// Exact starting stream sequence.
  final BigInt? sequence;

  /// RFC 3339 UTC starting time.
  final String? time;
}

/// Validated consumer subject filters.
final class JetStreamConsumerFilters {
  /// Delivers every stream subject.
  const JetStreamConsumerFilters.all() : subjects = const [];

  /// Delivers one subscription subject.
  factory JetStreamConsumerFilters.single(String subject) {
    validateSubscriptionSubject(subject);
    return JetStreamConsumerFilters._([subject]);
  }

  /// Delivers messages matching any supplied subscription subject.
  factory JetStreamConsumerFilters.multiple(Iterable<String> subjects) {
    final captured = List<String>.unmodifiable(subjects);
    if (captured.length < 2) {
      throw ArgumentError.value(subjects, "subjects", "Requires at least two");
    }
    for (final subject in captured) {
      validateSubscriptionSubject(subject);
    }
    validateDisjointNatsSubjectPatterns(captured);
    return JetStreamConsumerFilters._(captured);
  }

  const JetStreamConsumerFilters._(this.subjects);

  /// Zero, one, or many filter subjects.
  final List<String> subjects;
}

/// Durable or named pull consumer configuration.
final class JetStreamPullConsumerConfig {
  /// Creates a valid pull consumer configuration.
  factory JetStreamPullConsumerConfig({
    required String name,
    bool durable = true,
    String? description,
    JetStreamConsumerStart start = const JetStreamConsumerStart.all(),
    JetStreamConsumerFilters filters = const JetStreamConsumerFilters.all(),
    JetStreamConsumerAckPolicy ackPolicy = JetStreamConsumerAckPolicy.explicit,
    JetStreamConsumerReplayPolicy replayPolicy =
        JetStreamConsumerReplayPolicy.instant,
    JetStreamNanoseconds? ackWait,
    int? maximumDeliveries,
    List<JetStreamNanoseconds> backoff = const [],
    int? maximumAckPending,
    int? maximumWaiting,
    int? maximumBatch,
    JetStreamNanoseconds? maximumExpires,
    int? maximumBytes,
    JetStreamNanoseconds? inactiveThreshold,
    bool headersOnly = false,
    int? samplePercentage,
    Map<String, String> metadata = const {},
    DateTime? pauseUntil,
    bool memoryStorage = false,
    int replicas = 0,
    JetStreamConsumerPriorityPolicy priorityPolicy =
        JetStreamConsumerPriorityPolicy.none,
    List<String> priorityGroups = const [],
    JetStreamNanoseconds? priorityTimeout,
  }) {
    validateJetStreamConsumerName(name);
    validateConsumerCommon(
      description,
      ackPolicy == JetStreamConsumerAckPolicy.flowControl,
      ackWait,
      maximumDeliveries,
      backoff,
      maximumAckPending,
    );
    validatePositiveConsumerOption(maximumWaiting, "maximumWaiting");
    validatePositiveConsumerOption(maximumBatch, "maximumBatch");
    validatePositiveConsumerOption(maximumBytes, "maximumBytes");
    if (maximumExpires != null) {
      validateJetStreamDuration(maximumExpires.nanoseconds, "maximumExpires");
    }
    if (inactiveThreshold != null) {
      validateJetStreamDuration(
        inactiveThreshold.nanoseconds,
        "inactiveThreshold",
      );
    }
    validateConsumerSampling(samplePercentage);
    validateConsumerPause(pauseUntil);
    validateConsumerStorage(replicas);
    _validatePriority(priorityPolicy, priorityGroups, priorityTimeout);
    final capturedMetadata = captureConsumerMetadata(metadata);
    final capturedPriorityGroups = List<String>.unmodifiable(priorityGroups);
    return JetStreamPullConsumerConfig._(
      name,
      durable,
      description,
      start,
      filters,
      ackPolicy,
      replayPolicy,
      ackWait,
      maximumDeliveries,
      List.unmodifiable(backoff),
      maximumAckPending,
      maximumWaiting,
      maximumBatch,
      maximumExpires,
      maximumBytes,
      inactiveThreshold,
      headersOnly,
      samplePercentage,
      capturedMetadata,
      pauseUntil,
      memoryStorage,
      replicas,
      priorityPolicy,
      capturedPriorityGroups,
      priorityTimeout,
    );
  }

  const JetStreamPullConsumerConfig._(
    this.name,
    this.durable,
    this.description,
    this.start,
    this.filters,
    this.ackPolicy,
    this.replayPolicy,
    this.ackWait,
    this.maximumDeliveries,
    this.backoff,
    this.maximumAckPending,
    this.maximumWaiting,
    this.maximumBatch,
    this.maximumExpires,
    this.maximumBytes,
    this.inactiveThreshold,
    this.headersOnly,
    this.samplePercentage,
    this.metadata,
    this.pauseUntil,
    this.memoryStorage,
    this.replicas,
    this.priorityPolicy,
    this.priorityGroups,
    this.priorityTimeout,
  );

  /// Stable consumer name.
  final String name;

  /// Whether server state survives inactive subscriptions.
  final bool durable;

  /// Optional human readable purpose.
  final String? description;

  /// Initial delivery position.
  final JetStreamConsumerStart start;

  /// Stream subject filters.
  final JetStreamConsumerFilters filters;

  /// Message acknowledgement policy.
  final JetStreamConsumerAckPolicy ackPolicy;

  /// Delivery replay pacing.
  final JetStreamConsumerReplayPolicy replayPolicy;

  /// Redelivery wait when backoff is absent.
  final JetStreamNanoseconds? ackWait;

  /// Delivery attempt limit, or minus one for unlimited.
  final int? maximumDeliveries;

  /// Per attempt redelivery delays.
  final List<JetStreamNanoseconds> backoff;

  /// Maximum unacknowledged messages.
  final int? maximumAckPending;

  /// Maximum waiting pull requests.
  final int? maximumWaiting;

  /// Server accepted pull batch limit.
  final int? maximumBatch;

  /// Server accepted pull expiry limit.
  final JetStreamNanoseconds? maximumExpires;

  /// Server accepted pull byte limit.
  final int? maximumBytes;

  /// Automatic inactive cleanup threshold.
  final JetStreamNanoseconds? inactiveThreshold;

  /// Whether deliveries contain headers without payload bytes.
  final bool headersOnly;

  /// Percentage of acknowledgements sampled for advisories.
  final int? samplePercentage;

  /// Immutable server visible metadata.
  final Map<String, String> metadata;

  /// UTC deadline until which delivery is paused.
  final DateTime? pauseUntil;

  /// Whether consumer state is kept in memory.
  final bool memoryStorage;

  /// Consumer replica count. Zero inherits the stream.
  final int replicas;

  /// Pull request selection policy.
  final JetStreamConsumerPriorityPolicy priorityPolicy;

  /// Eligible priority groups.
  final List<String> priorityGroups;

  /// Pinned client renewal timeout.
  final JetStreamNanoseconds? priorityTimeout;
}

void _validatePriority(
  JetStreamConsumerPriorityPolicy policy,
  List<String> groups,
  JetStreamNanoseconds? timeout,
) {
  if (policy == JetStreamConsumerPriorityPolicy.none) {
    if (groups.isNotEmpty || timeout != null) {
      throw ArgumentError("Priority groups and timeout require a policy");
    }
    return;
  }
  if (groups.isEmpty) {
    throw ArgumentError.value(groups, "priorityGroups", "Must not be empty");
  }
  for (final group in groups) {
    validateConsumerPriorityGroup(group);
  }
  if (groups.toSet().length != groups.length) {
    throw ArgumentError.value(groups, "priorityGroups", "Must be unique");
  }
  if (timeout != null && timeout.nanoseconds <= BigInt.zero) {
    throw ArgumentError.value(timeout, "priorityTimeout", "Must be positive");
  }
  if (timeout != null) {
    validatePositiveJetStreamDuration(timeout.nanoseconds, "priorityTimeout");
  }
}
