import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/consumer_config.dart";
import "package:nats_jetstream/src/consumer_validation.dart";
import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/wire_integer.dart";

/// Durable or named push consumer configuration.
final class JetStreamPushConsumerConfig {
  /// Creates a validated push consumer configuration.
  factory JetStreamPushConsumerConfig({
    required String name,
    required String deliverSubject,
    bool durable = true,
    String? deliverGroup,
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
    JetStreamNanoseconds? inactiveThreshold,
    bool headersOnly = false,
    bool flowControl = false,
    JetStreamNanoseconds? idleHeartbeat,
    bool memoryStorage = false,
    int replicas = 0,
    BigInt? rateLimitBitsPerSecond,
    int? samplePercentage,
    Map<String, String> metadata = const {},
    DateTime? pauseUntil,
  }) {
    validateJetStreamConsumerName(name);
    validatePublishSubject(deliverSubject);
    if (deliverGroup != null) {
      validateQueueGroup(deliverGroup);
    }
    validateConsumerCommon(
      description,
      ackPolicy == JetStreamConsumerAckPolicy.flowControl,
      ackWait,
      maximumDeliveries,
      backoff,
      maximumAckPending,
    );
    if (idleHeartbeat != null &&
        idleHeartbeat.nanoseconds < BigInt.from(100000000)) {
      throw ArgumentError.value(
        idleHeartbeat,
        "idleHeartbeat",
        "Must be at least 100 milliseconds",
      );
    }
    if (idleHeartbeat != null) {
      validatePositiveJetStreamDuration(
        idleHeartbeat.nanoseconds,
        "idleHeartbeat",
      );
    }
    if (inactiveThreshold != null) {
      validateJetStreamDuration(
        inactiveThreshold.nanoseconds,
        "inactiveThreshold",
      );
    }
    if (flowControl && idleHeartbeat == null) {
      throw ArgumentError("Flow control requires an idle heartbeat");
    }
    validateConsumerStorage(replicas);
    if (rateLimitBitsPerSecond != null) {
      validatePositiveJetStreamUint64(
        rateLimitBitsPerSecond,
        "rateLimitBitsPerSecond",
      );
    }
    validateConsumerSampling(samplePercentage);
    validateConsumerPause(pauseUntil);
    final capturedMetadata = captureConsumerMetadata(metadata);
    return JetStreamPushConsumerConfig._(
      name,
      deliverSubject,
      durable,
      deliverGroup,
      description,
      start,
      filters,
      ackPolicy,
      replayPolicy,
      ackWait,
      maximumDeliveries,
      List.unmodifiable(backoff),
      maximumAckPending,
      inactiveThreshold,
      headersOnly,
      flowControl,
      idleHeartbeat,
      memoryStorage,
      replicas,
      rateLimitBitsPerSecond,
      samplePercentage,
      capturedMetadata,
      pauseUntil,
    );
  }

  const JetStreamPushConsumerConfig._(
    this.name,
    this.deliverSubject,
    this.durable,
    this.deliverGroup,
    this.description,
    this.start,
    this.filters,
    this.ackPolicy,
    this.replayPolicy,
    this.ackWait,
    this.maximumDeliveries,
    this.backoff,
    this.maximumAckPending,
    this.inactiveThreshold,
    this.headersOnly,
    this.flowControl,
    this.idleHeartbeat,
    this.memoryStorage,
    this.replicas,
    this.rateLimitBitsPerSecond,
    this.samplePercentage,
    this.metadata,
    this.pauseUntil,
  );

  /// Stable consumer name.
  final String name;

  /// Core subject that receives pushed messages.
  final String deliverSubject;

  /// Whether server state survives inactive subscriptions.
  final bool durable;

  /// Optional delivery queue group.
  final String? deliverGroup;

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

  /// Automatic inactive cleanup threshold.
  final JetStreamNanoseconds? inactiveThreshold;

  /// Whether deliveries contain headers without payload bytes.
  final bool headersOnly;

  /// Whether Core flow control frames are enabled.
  final bool flowControl;

  /// Server heartbeat interval.
  final JetStreamNanoseconds? idleHeartbeat;

  /// Whether consumer state is kept in memory instead of stream storage.
  final bool memoryStorage;

  /// Consumer state replica count. Zero inherits the stream configuration.
  final int replicas;

  /// Maximum push delivery bit rate.
  final BigInt? rateLimitBitsPerSecond;

  /// Percentage of acknowledgements sampled for advisories.
  final int? samplePercentage;

  /// Immutable server visible metadata.
  final Map<String, String> metadata;

  /// UTC deadline until which delivery is paused.
  final DateTime? pauseUntil;
}

/// Strict NATS 2.14 configuration for server managed reliable sourcing.
final class JetStreamReliableSourceConsumerConfig {
  /// Creates the only valid `AckFlowControl` consumer shape.
  factory JetStreamReliableSourceConsumerConfig({
    required String name,
    required String deliverSubject,
    required int maximumAckPending,
    JetStreamConsumerStart start = const JetStreamConsumerStart.all(),
    JetStreamConsumerFilters filters = const JetStreamConsumerFilters.all(),
    JetStreamConsumerReplayPolicy replayPolicy =
        JetStreamConsumerReplayPolicy.instant,
    String? description,
    Map<String, String> metadata = const {},
  }) {
    validateJetStreamConsumerName(name);
    validatePublishSubject(deliverSubject);
    if (description != null) {
      validateConsumerUtf8Field(description, "description", 4096);
    }
    if (maximumAckPending < 1) {
      throw ArgumentError.value(
        maximumAckPending,
        "maximumAckPending",
        "Must be positive",
      );
    }
    final capturedMetadata = captureConsumerMetadata(metadata);
    return JetStreamReliableSourceConsumerConfig._(
      name,
      deliverSubject,
      maximumAckPending,
      start,
      filters,
      replayPolicy,
      description,
      capturedMetadata,
    );
  }

  const JetStreamReliableSourceConsumerConfig._(
    this.name,
    this.deliverSubject,
    this.maximumAckPending,
    this.start,
    this.filters,
    this.replayPolicy,
    this.description,
    this.metadata,
  );

  /// Durable consumer name.
  final String name;

  /// Internal source delivery subject.
  final String deliverSubject;

  /// Positive persistence acknowledgement window.
  final int maximumAckPending;

  /// Initial delivery position.
  final JetStreamConsumerStart start;

  /// Stream subject filters.
  final JetStreamConsumerFilters filters;

  /// Delivery replay pacing.
  final JetStreamConsumerReplayPolicy replayPolicy;

  /// Optional human readable purpose.
  final String? description;

  /// Immutable user metadata added beside required NATS capability metadata.
  final Map<String, String> metadata;
}
