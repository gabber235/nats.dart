import "package:nats_jetstream/src/consumer_config.dart";
import "package:nats_jetstream/src/consumer_models.dart";
import "package:nats_jetstream/src/consumer_push_config.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/timestamp.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// Encodes a pull consumer through exact JSON compatible values.
Map<String, Object?> encodePullConsumerConfig(
  JetStreamPullConsumerConfig config,
) => {
  ..._base(
    name: config.name,
    durable: config.durable,
    description: config.description,
    start: config.start,
    filters: config.filters,
    ackPolicy: config.ackPolicy,
    replayPolicy: config.replayPolicy,
    samplePercentage: config.samplePercentage,
    metadata: config.metadata,
    pauseUntil: config.pauseUntil,
    memoryStorage: config.memoryStorage,
    replicas: config.replicas,
  ),
  "ack_wait": ?config.ackWait?.nanoseconds,
  "max_deliver": ?config.maximumDeliveries,
  if (config.backoff.isNotEmpty)
    "backoff": config.backoff.map((value) => value.nanoseconds).toList(),
  "max_ack_pending": ?config.maximumAckPending,
  "max_waiting": ?config.maximumWaiting,
  "max_batch": ?config.maximumBatch,
  "max_expires": ?config.maximumExpires?.nanoseconds,
  "max_bytes": ?config.maximumBytes,
  "inactive_threshold": ?config.inactiveThreshold?.nanoseconds,
  if (config.headersOnly) "headers_only": true,
  if (config.priorityPolicy != JetStreamConsumerPriorityPolicy.none)
    "priority_policy": config.priorityPolicy.wireValue,
  if (config.priorityGroups.isNotEmpty)
    "priority_groups": config.priorityGroups,
  "priority_timeout": ?config.priorityTimeout?.nanoseconds,
};

/// Encodes a push consumer through exact JSON compatible values.
Map<String, Object?> encodePushConsumerConfig(
  JetStreamPushConsumerConfig config,
) => {
  ..._base(
    name: config.name,
    durable: config.durable,
    description: config.description,
    start: config.start,
    filters: config.filters,
    ackPolicy: config.ackPolicy,
    replayPolicy: config.replayPolicy,
    samplePercentage: config.samplePercentage,
    metadata: config.metadata,
    pauseUntil: config.pauseUntil,
    memoryStorage: config.memoryStorage,
    replicas: config.replicas,
  ),
  "deliver_subject": config.deliverSubject,
  "deliver_group": ?config.deliverGroup,
  "rate_limit_bps": ?config.rateLimitBitsPerSecond,
  "ack_wait": ?config.ackWait?.nanoseconds,
  "max_deliver": ?config.maximumDeliveries,
  if (config.backoff.isNotEmpty)
    "backoff": config.backoff.map((value) => value.nanoseconds).toList(),
  "max_ack_pending": ?config.maximumAckPending,
  "inactive_threshold": ?config.inactiveThreshold?.nanoseconds,
  if (config.headersOnly) "headers_only": true,
  if (config.flowControl) "flow_control": true,
  "idle_heartbeat": ?config.idleHeartbeat?.nanoseconds,
};

/// Encodes the strict NATS 2.14 reliable sourcing consumer shape.
Map<String, Object?> encodeReliableSourceConsumerConfig(
  JetStreamReliableSourceConsumerConfig config,
) => {
  ..._base(
    name: config.name,
    durable: true,
    description: config.description,
    start: config.start,
    filters: config.filters,
    ackPolicy: JetStreamConsumerAckPolicy.flowControl,
    replayPolicy: config.replayPolicy,
  ),
  "deliver_subject": config.deliverSubject,
  "flow_control": true,
  "idle_heartbeat": BigInt.from(1000000000),
  "max_ack_pending": config.maximumAckPending,
  "max_deliver": -1,
  "metadata": {...config.metadata, "_nats.req.level": "4"},
};

/// Decodes current consumer state from a response or nested page entry.
JetStreamConsumerInfo decodeConsumerInfo(
  JetStreamWireResponse response, {
  Map<String, Object?>? source,
}) {
  final root = source ?? response.body;
  final config = response.requiredObject("config", source: root);
  final delivered = response.requiredObject("delivered", source: root);
  final floor = response.requiredObject("ack_floor", source: root);
  final ackPolicy = _ackPolicy(
    response.requiredString("config.ack_policy", source: config),
    response.subject,
  );
  final deliverSubject = response.optionalString(
    "config.deliver_subject",
    source: config,
  );
  final kind = ackPolicy == JetStreamConsumerAckPolicy.flowControl
      ? JetStreamConsumerKind.reliableSource
      : deliverSubject == null
      ? JetStreamConsumerKind.pull
      : JetStreamConsumerKind.push;
  final singleFilter = response.optionalString(
    "config.filter_subject",
    source: config,
  );
  final multipleFilters = response.optionalStringList(
    "config.filter_subjects",
    source: config,
  );
  if (singleFilter != null && multipleFilters.isNotEmpty) {
    throw JetStreamProtocolException(
      "Consumer response contains both filter forms",
      subject: response.subject,
      fieldPath: "config.filter_subjects",
    );
  }
  return JetStreamConsumerInfo(
    streamName: response.requiredString("stream_name", source: root),
    name: response.requiredString("name", source: root),
    created: response.requiredRfc3339("created", source: root),
    config: JetStreamConsumerConfigSnapshot(
      name: response.requiredString("config.name", source: config),
      durableName: response.optionalString(
        "config.durable_name",
        source: config,
      ),
      kind: kind,
      deliverSubject: deliverSubject,
      deliverGroup: response.optionalString(
        "config.deliver_group",
        source: config,
      ),
      deliverPolicy: response.requiredString(
        "config.deliver_policy",
        source: config,
      ),
      ackPolicy: ackPolicy,
      replayPolicy: _replayPolicy(
        response.requiredString("config.replay_policy", source: config),
        response.subject,
      ),
      filterSubjects: singleFilter == null ? multipleFilters : [singleFilter],
      description: response.optionalString(
        "config.description",
        source: config,
      ),
      startSequence: response.optionalUnsigned(
        "config.opt_start_seq",
        source: config,
      ),
      startTime: response.optionalRfc3339(
        "config.opt_start_time",
        source: config,
      ),
      ackWait: response.optionalPositiveNanoseconds(
        "config.ack_wait",
        source: config,
      ),
      maximumDeliveries: response.optionalInt(
        "config.max_deliver",
        source: config,
      ),
      maximumAckPending: response.optionalInt(
        "config.max_ack_pending",
        source: config,
      ),
      maximumWaiting: response.optionalNonNegativeInt(
        "config.max_waiting",
        source: config,
      ),
      maximumBatch: response.optionalNonNegativeInt(
        "config.max_batch",
        source: config,
      ),
      maximumExpires: response.optionalNanoseconds(
        "config.max_expires",
        source: config,
      ),
      maximumBytes: response.optionalNonNegativeInt(
        "config.max_bytes",
        source: config,
      ),
      backoff: response
          .optionalNonNegativeInt64List("config.backoff", source: config)
          .map(JetStreamNanoseconds.new)
          .toList(growable: false),
      rateLimitBitsPerSecond: response.optionalUnsigned(
        "config.rate_limit_bps",
        source: config,
      ),
      sampleFrequency: response.optionalString(
        "config.sample_freq",
        source: config,
      ),
      idleHeartbeat: response.optionalNanoseconds(
        "config.idle_heartbeat",
        source: config,
      ),
      inactiveThreshold: response.optionalNanoseconds(
        "config.inactive_threshold",
        source: config,
      ),
      headersOnly: response.optionalBoolean(
        "config.headers_only",
        source: config,
      ),
      flowControl: response.optionalBoolean(
        "config.flow_control",
        source: config,
      ),
      memoryStorage: response.optionalBoolean(
        "config.mem_storage",
        source: config,
      ),
      replicas:
          response.optionalBoundedInt(
            "config.num_replicas",
            minimum: 0,
            maximum: 5,
            source: config,
          ) ??
          0,
      metadata: response.optionalStringMap("config.metadata", source: config),
      pauseUntil: response.optionalRfc3339(
        "config.pause_until",
        source: config,
      ),
      priorityPolicy: _priorityPolicy(
        response.optionalString("config.priority_policy", source: config) ??
            "none",
        response.subject,
      ),
      priorityGroups: response.optionalStringList(
        "config.priority_groups",
        source: config,
      ),
      priorityTimeout: response.optionalNanoseconds(
        "config.priority_timeout",
        source: config,
      ),
      raw: immutableExactJsonObject(config),
    ),
    delivered: _sequence(response, delivered),
    acknowledgementFloor: _sequence(response, floor),
    acknowledgementsPending: response.requiredNonNegativeInt64(
      "num_ack_pending",
      source: root,
    ),
    redelivered: response.requiredNonNegativeInt64(
      "num_redelivered",
      source: root,
    ),
    waiting: response.requiredNonNegativeInt64("num_waiting", source: root),
    pending: response.requiredUnsigned("num_pending", source: root),
    paused: response.optionalBoolean("paused", source: root),
    pushBound: response.optionalBoolean("push_bound", source: root),
    pauseRemaining: response.optionalNanoseconds(
      "pause_remaining",
      source: root,
    ),
    timestamp: response.requiredRfc3339("ts", source: root),
    priorityGroups: response
        .optionalObjectList("priority_groups", source: root)
        .map((entry) => _priorityGroup(response, entry))
        .toList(growable: false),
    raw: immutableExactJsonObject(root),
  );
}

/// Requires one decoded consumer response to match its request context.
JetStreamConsumerInfo requireConsumerIdentity(
  JetStreamWireResponse response,
  JetStreamConsumerInfo info, {
  required String expectedStream,
  String? expectedConsumer,
}) {
  if (info.streamName != expectedStream) {
    throw JetStreamProtocolException(
      "Consumer response stream identity does not match the request",
      subject: response.subject,
      fieldPath: "stream_name",
    );
  }
  if (expectedConsumer != null && info.name != expectedConsumer) {
    throw JetStreamProtocolException(
      "Consumer response identity does not match the request",
      subject: response.subject,
      fieldPath: "name",
    );
  }
  if (info.config.name != info.name) {
    throw JetStreamProtocolException(
      "Consumer configuration identity does not match the response",
      subject: response.subject,
      fieldPath: "config.name",
    );
  }
  return info;
}

Map<String, Object?> _base({
  required String name,
  required bool durable,
  required String? description,
  required JetStreamConsumerStart start,
  required JetStreamConsumerFilters filters,
  required JetStreamConsumerAckPolicy ackPolicy,
  required JetStreamConsumerReplayPolicy replayPolicy,
  int? samplePercentage,
  Map<String, String> metadata = const {},
  DateTime? pauseUntil,
  bool memoryStorage = false,
  int replicas = 0,
}) => {
  "name": name,
  if (durable) "durable_name": name,
  "description": ?description,
  "deliver_policy": start.policy,
  "opt_start_seq": ?start.sequence,
  "opt_start_time": ?start.time,
  "ack_policy": ackPolicy.wireValue,
  "replay_policy": replayPolicy.wireValue,
  if (samplePercentage != null) "sample_freq": "$samplePercentage%",
  if (metadata.isNotEmpty) "metadata": metadata,
  "pause_until": ?pauseUntil == null
      ? null
      : serializeJetStreamRfc3339(pauseUntil, "pauseUntil"),
  if (memoryStorage) "mem_storage": true,
  if (replicas != 0) "num_replicas": replicas,
  if (filters.subjects.length == 1) "filter_subject": filters.subjects.single,
  if (filters.subjects.length > 1) "filter_subjects": filters.subjects,
};

JetStreamConsumerSequence _sequence(
  JetStreamWireResponse response,
  Map<String, Object?> source,
) => JetStreamConsumerSequence(
  consumer: response.requiredUnsigned("consumer_seq", source: source),
  stream: response.requiredUnsigned("stream_seq", source: source),
  lastActive: response.optionalRfc3339("last_active", source: source),
);

JetStreamConsumerAckPolicy _ackPolicy(String value, String subject) {
  for (final policy in JetStreamConsumerAckPolicy.values) {
    if (policy.wireValue == value) {
      return policy;
    }
  }
  throw JetStreamProtocolException(
    "Unknown consumer acknowledgement policy",
    subject: subject,
    fieldPath: "config.ack_policy",
  );
}

JetStreamConsumerPriorityPolicy _priorityPolicy(String value, String subject) {
  for (final policy in JetStreamConsumerPriorityPolicy.values) {
    if (policy.wireValue == value) {
      return policy;
    }
  }
  throw JetStreamProtocolException(
    "Unknown consumer priority policy",
    subject: subject,
    fieldPath: "config.priority_policy",
  );
}

JetStreamConsumerPriorityGroupState _priorityGroup(
  JetStreamWireResponse response,
  Map<String, Object?> source,
) => JetStreamConsumerPriorityGroupState(
  group: response.requiredString("priority_groups.group", source: source),
  pinnedClientId: response.optionalString(
    "priority_groups.pinned_client_id",
    source: source,
  ),
  pinnedAt: response.optionalRfc3339(
    "priority_groups.pinned_ts",
    source: source,
  ),
);

JetStreamConsumerReplayPolicy _replayPolicy(String value, String subject) {
  for (final policy in JetStreamConsumerReplayPolicy.values) {
    if (policy.wireValue == value) {
      return policy;
    }
  }
  throw JetStreamProtocolException(
    "Unknown consumer replay policy",
    subject: subject,
    fieldPath: "config.replay_policy",
  );
}
