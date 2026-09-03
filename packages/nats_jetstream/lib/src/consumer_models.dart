import "package:nats_jetstream/src/consumer_config.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/models.dart";

/// Pull or push delivery kind returned by the server.
enum JetStreamConsumerKind {
  /// Client initiated pull delivery.
  pull,

  /// Server initiated push delivery.
  push,

  /// Server managed reliable stream sourcing.
  reliableSource,
}

/// Exact stream and consumer sequence position.
final class JetStreamConsumerSequence {
  /// Creates an exact position.
  const JetStreamConsumerSequence({
    required this.consumer,
    required this.stream,
    this.lastActive,
  });

  /// Consumer relative delivery sequence.
  final BigInt consumer;

  /// Stream sequence.
  final BigInt stream;

  /// Exact RFC 3339 activity timestamp, when supplied.
  final String? lastActive;
}

/// Server normalized consumer configuration snapshot.
final class JetStreamConsumerConfigSnapshot {
  /// Creates a normalized immutable snapshot.
  JetStreamConsumerConfigSnapshot({
    required this.name,
    required this.durableName,
    required this.kind,
    required this.deliverSubject,
    required this.deliverGroup,
    required this.deliverPolicy,
    required this.ackPolicy,
    required this.replayPolicy,
    required List<String> filterSubjects,
    required this.description,
    required this.startSequence,
    required this.startTime,
    required this.ackWait,
    required this.maximumDeliveries,
    required List<JetStreamNanoseconds> backoff,
    required this.maximumAckPending,
    required this.maximumWaiting,
    required this.maximumBatch,
    required this.maximumExpires,
    required this.maximumBytes,
    required this.rateLimitBitsPerSecond,
    required this.sampleFrequency,
    required this.idleHeartbeat,
    required this.inactiveThreshold,
    required this.headersOnly,
    required this.flowControl,
    required this.memoryStorage,
    required this.replicas,
    required Map<String, String> metadata,
    required this.pauseUntil,
    required this.priorityPolicy,
    required List<String> priorityGroups,
    required this.priorityTimeout,
    required Map<String, Object?> raw,
  }) : filterSubjects = List.unmodifiable(filterSubjects),
       backoff = List.unmodifiable(backoff),
       metadata = Map.unmodifiable(metadata),
       priorityGroups = List.unmodifiable(priorityGroups),
       raw = immutableExactJsonObject(raw);

  /// Server normalized consumer name.
  final String name;

  /// Durable name, when configured.
  final String? durableName;

  /// Pull, push, or reliable source kind.
  final JetStreamConsumerKind kind;

  /// Push delivery subject, when present.
  final String? deliverSubject;

  /// Push queue group, when present.
  final String? deliverGroup;

  /// Server delivery policy value.
  final String deliverPolicy;

  /// Server acknowledgement policy.
  final JetStreamConsumerAckPolicy ackPolicy;

  /// Server replay policy.
  final JetStreamConsumerReplayPolicy replayPolicy;

  /// Zero, one, or many filter subjects.
  final List<String> filterSubjects;

  /// Server normalized description.
  final String? description;

  /// Exact optional starting sequence.
  final BigInt? startSequence;

  /// Exact optional RFC 3339 starting time.
  final String? startTime;

  /// Server normalized acknowledgement wait.
  final JetStreamNanoseconds? ackWait;

  /// Delivery attempt limit.
  final int? maximumDeliveries;

  /// Server normalized redelivery delays.
  final List<JetStreamNanoseconds> backoff;

  /// Maximum unacknowledged messages.
  final int? maximumAckPending;

  /// Maximum waiting pull requests.
  final int? maximumWaiting;

  /// Maximum pull batch size.
  final int? maximumBatch;

  /// Maximum pull request expiry.
  final JetStreamNanoseconds? maximumExpires;

  /// Maximum pull response bytes.
  final int? maximumBytes;

  /// Maximum push delivery bit rate.
  final BigInt? rateLimitBitsPerSecond;

  /// Server sampling grammar, when configured.
  final String? sampleFrequency;

  /// Server push heartbeat interval.
  final JetStreamNanoseconds? idleHeartbeat;

  /// Automatic inactive cleanup threshold.
  final JetStreamNanoseconds? inactiveThreshold;

  /// Whether payload bytes are omitted.
  final bool headersOnly;

  /// Whether push flow control is active.
  final bool flowControl;

  /// Whether consumer state uses memory storage.
  final bool memoryStorage;

  /// Effective consumer replica count.
  final int replicas;

  /// Immutable server visible metadata.
  final Map<String, String> metadata;

  /// Exact UTC pause deadline.
  final String? pauseUntil;

  /// Pull request priority policy.
  final JetStreamConsumerPriorityPolicy priorityPolicy;

  /// Eligible pull request priority groups.
  final List<String> priorityGroups;

  /// Pinned client renewal timeout.
  final JetStreamNanoseconds? priorityTimeout;

  /// Deeply immutable exact JSON for forward compatible inspection.
  final Map<String, Object?> raw;
}

/// Current runtime state for one consumer priority group.
final class JetStreamConsumerPriorityGroupState {
  /// Creates an immutable priority group state.
  const JetStreamConsumerPriorityGroupState({
    required this.group,
    required this.pinnedClientId,
    required this.pinnedAt,
  });

  /// Configured group name.
  final String group;

  /// Current pinned request client identifier.
  final String? pinnedClientId;

  /// Exact RFC 3339 pin timestamp.
  final String? pinnedAt;
}

/// Current server state for one named consumer.
final class JetStreamConsumerInfo {
  /// Creates an immutable consumer state snapshot.
  JetStreamConsumerInfo({
    required this.streamName,
    required this.name,
    required this.created,
    required this.config,
    required this.delivered,
    required this.acknowledgementFloor,
    required this.acknowledgementsPending,
    required this.redelivered,
    required this.waiting,
    required this.pending,
    required this.paused,
    required this.pushBound,
    required this.pauseRemaining,
    required this.timestamp,
    required List<JetStreamConsumerPriorityGroupState> priorityGroups,
    required Map<String, Object?> raw,
  }) : priorityGroups = List.unmodifiable(priorityGroups),
       raw = immutableExactJsonObject(raw);

  /// Owning stream name.
  final String streamName;

  /// Consumer name.
  final String name;

  /// Exact RFC 3339 creation timestamp.
  final String created;

  /// Server normalized configuration.
  final JetStreamConsumerConfigSnapshot config;

  /// Last delivered position.
  final JetStreamConsumerSequence delivered;

  /// Last acknowledged position.
  final JetStreamConsumerSequence acknowledgementFloor;

  /// Messages delivered and awaiting acknowledgement.
  final BigInt acknowledgementsPending;

  /// Messages redelivered.
  final BigInt redelivered;

  /// Pull requests currently waiting.
  final BigInt waiting;

  /// Messages available for delivery.
  final BigInt pending;

  /// Whether delivery is paused.
  final bool paused;

  /// Whether a push subscriber is currently bound.
  final bool pushBound;

  /// Remaining pause duration reported by the server.
  final JetStreamNanoseconds? pauseRemaining;

  /// Exact RFC 3339 time at which this snapshot was gathered.
  final String timestamp;

  /// Runtime priority group state.
  final List<JetStreamConsumerPriorityGroupState> priorityGroups;

  /// Deeply immutable exact JSON for forward compatible inspection.
  final Map<String, Object?> raw;
}

/// One exact page of detailed consumer state.
final class JetStreamConsumerPage {
  /// Creates an immutable page.
  JetStreamConsumerPage({
    required this.total,
    required this.offset,
    required this.limit,
    required List<JetStreamConsumerInfo> consumers,
    required List<String> missing,
    required Map<String, String> offline,
  }) : consumers = List.unmodifiable(consumers),
       missing = List.unmodifiable(missing),
       offline = Map.unmodifiable(offline);

  /// Total matching consumers.
  final BigInt total;

  /// Page offset.
  final BigInt offset;

  /// Server page limit.
  final BigInt limit;

  /// Detailed members in this page.
  final List<JetStreamConsumerInfo> consumers;

  /// Cluster members whose information was missing.
  final List<String> missing;

  /// Consumer names mapped to offline reasons.
  final Map<String, String> offline;
}

/// One exact page of consumer names.
final class JetStreamConsumerNamePage {
  /// Creates an immutable name page.
  JetStreamConsumerNamePage({
    required this.total,
    required this.offset,
    required this.limit,
    required List<String> names,
  }) : names = List.unmodifiable(names);

  /// Total names reported while this page was gathered.
  final BigInt total;

  /// Exact page offset.
  final BigInt offset;

  /// Server page size limit.
  final BigInt limit;

  /// Consumer names in server order.
  final List<String> names;
}

/// Result of pausing or resuming one consumer.
final class JetStreamConsumerPauseResult {
  /// Creates an immutable pause result.
  const JetStreamConsumerPauseResult({
    required this.paused,
    required this.pauseUntil,
    required this.pauseRemaining,
  });

  /// Whether the consumer is paused after the operation.
  final bool paused;

  /// Exact server pause deadline, including the zero time after resume.
  final String pauseUntil;

  /// Remaining pause duration when positive.
  final JetStreamNanoseconds? pauseRemaining;
}

/// Confirmed result of the NATS 2.14 consumer reset API.
final class JetStreamConsumerResetResult {
  /// Creates reset evidence with returned current state.
  const JetStreamConsumerResetResult({
    required this.resetSequence,
    required this.info,
  });

  /// First stream sequence after reset.
  final BigInt resetSequence;

  /// Current state returned by the reset mutation.
  final JetStreamConsumerInfo info;
}
