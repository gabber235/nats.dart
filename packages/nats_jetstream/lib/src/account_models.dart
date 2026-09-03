import "package:nats_jetstream/src/exact_json.dart";

/// Exact JetStream limits applied to one account or named tier.
final class JetStreamAccountLimits {
  /// Creates an immutable limits snapshot.
  factory JetStreamAccountLimits({
    required BigInt maximumMemoryBytes,
    required BigInt maximumStorageBytes,
    required BigInt maximumStreams,
    required BigInt maximumConsumers,
    required BigInt maximumAcknowledgementsPending,
    required BigInt maximumMemoryStreamBytes,
    required BigInt maximumStorageStreamBytes,
    required bool maximumBytesRequired,
    required Map<String, Object?> raw,
  }) => JetStreamAccountLimits._(
    maximumMemoryBytes: maximumMemoryBytes,
    maximumStorageBytes: maximumStorageBytes,
    maximumStreams: maximumStreams,
    maximumConsumers: maximumConsumers,
    maximumAcknowledgementsPending: maximumAcknowledgementsPending,
    maximumMemoryStreamBytes: maximumMemoryStreamBytes,
    maximumStorageStreamBytes: maximumStorageStreamBytes,
    maximumBytesRequired: maximumBytesRequired,
    raw: immutableExactJsonObject(raw),
  );

  const JetStreamAccountLimits._({
    required this.maximumMemoryBytes,
    required this.maximumStorageBytes,
    required this.maximumStreams,
    required this.maximumConsumers,
    required this.maximumAcknowledgementsPending,
    required this.maximumMemoryStreamBytes,
    required this.maximumStorageStreamBytes,
    required this.maximumBytesRequired,
    required this.raw,
  });

  /// Maximum memory storage bytes, or a negative value for unlimited.
  final BigInt maximumMemoryBytes;

  /// Maximum file storage bytes, or a negative value for unlimited.
  final BigInt maximumStorageBytes;

  /// Maximum stream count, or a negative value for unlimited.
  final BigInt maximumStreams;

  /// Maximum consumer count, or a negative value for unlimited.
  final BigInt maximumConsumers;

  /// Maximum outstanding acknowledgements per consumer.
  final BigInt maximumAcknowledgementsPending;

  /// Maximum bytes allowed in one memory stream.
  final BigInt maximumMemoryStreamBytes;

  /// Maximum bytes allowed in one file stream.
  final BigInt maximumStorageStreamBytes;

  /// Whether every stream must configure its maximum byte count.
  final bool maximumBytesRequired;

  /// Deeply immutable complete limits object.
  final Map<String, Object?> raw;
}

/// Exact API request counters reported for one JetStream account.
final class JetStreamApiStats {
  /// Creates an immutable API snapshot.
  factory JetStreamApiStats({
    required int level,
    required BigInt total,
    required BigInt errors,
    required BigInt inflight,
    required Map<String, Object?> raw,
  }) => JetStreamApiStats._(
    level: level,
    total: total,
    errors: errors,
    inflight: inflight,
    raw: immutableExactJsonObject(raw),
  );

  const JetStreamApiStats._({
    required this.level,
    required this.total,
    required this.errors,
    required this.inflight,
    required this.raw,
  });

  /// Active JetStream API level.
  final int level;

  /// API requests received since server start.
  final BigInt total;

  /// API requests that produced error responses.
  final BigInt errors;

  /// API requests currently being processed.
  final BigInt inflight;

  /// Deeply immutable complete API object.
  final Map<String, Object?> raw;
}

/// Usage and limits for one dynamically named JetStream account tier.
final class JetStreamAccountTier {
  /// Creates an immutable tier snapshot.
  factory JetStreamAccountTier({
    required BigInt memoryBytes,
    required BigInt storageBytes,
    required BigInt reservedMemoryBytes,
    required BigInt reservedStorageBytes,
    required BigInt streams,
    required BigInt consumers,
    required JetStreamAccountLimits limits,
    required Map<String, Object?> raw,
  }) => JetStreamAccountTier._(
    memoryBytes: memoryBytes,
    storageBytes: storageBytes,
    reservedMemoryBytes: reservedMemoryBytes,
    reservedStorageBytes: reservedStorageBytes,
    streams: streams,
    consumers: consumers,
    limits: limits,
    raw: immutableExactJsonObject(raw),
  );

  const JetStreamAccountTier._({
    required this.memoryBytes,
    required this.storageBytes,
    required this.reservedMemoryBytes,
    required this.reservedStorageBytes,
    required this.streams,
    required this.consumers,
    required this.limits,
    required this.raw,
  });

  /// Memory bytes currently used.
  final BigInt memoryBytes;

  /// File storage bytes currently used.
  final BigInt storageBytes;

  /// Memory bytes reserved by configured streams.
  final BigInt reservedMemoryBytes;

  /// File storage bytes reserved by configured streams.
  final BigInt reservedStorageBytes;

  /// Active stream count.
  final BigInt streams;

  /// Active consumer count.
  final BigInt consumers;

  /// Limits applied to this tier.
  final JetStreamAccountLimits limits;

  /// Deeply immutable complete tier object.
  final Map<String, Object?> raw;
}

/// JetStream account capability and resource snapshot.
final class JetStreamAccountInfo {
  /// Creates an immutable account snapshot.
  factory JetStreamAccountInfo({
    required BigInt memoryBytes,
    required BigInt storageBytes,
    required BigInt reservedMemoryBytes,
    required BigInt reservedStorageBytes,
    required BigInt streams,
    required BigInt consumers,
    required JetStreamAccountLimits limits,
    required JetStreamApiStats api,
    required String? domain,
    required Map<String, JetStreamAccountTier> tiers,
    required Map<String, Object?> raw,
  }) => JetStreamAccountInfo._(
    memoryBytes: memoryBytes,
    storageBytes: storageBytes,
    reservedMemoryBytes: reservedMemoryBytes,
    reservedStorageBytes: reservedStorageBytes,
    streams: streams,
    consumers: consumers,
    limits: limits,
    api: api,
    domain: domain,
    tiers: Map.unmodifiable(tiers),
    raw: immutableExactJsonObject(raw),
  );

  const JetStreamAccountInfo._({
    required this.memoryBytes,
    required this.storageBytes,
    required this.reservedMemoryBytes,
    required this.reservedStorageBytes,
    required this.streams,
    required this.consumers,
    required this.limits,
    required this.api,
    required this.domain,
    required this.tiers,
    required this.raw,
  });

  /// Memory bytes currently used.
  final BigInt memoryBytes;

  /// File storage bytes currently used.
  final BigInt storageBytes;

  /// Memory bytes reserved by configured streams.
  final BigInt reservedMemoryBytes;

  /// File storage bytes reserved by configured streams.
  final BigInt reservedStorageBytes;

  /// Active stream count.
  final BigInt streams;

  /// Active consumer count.
  final BigInt consumers;

  /// Limits applied to the account total.
  final JetStreamAccountLimits limits;

  /// API request capability and counters.
  final JetStreamApiStats api;

  /// Configured JetStream domain, when present.
  final String? domain;

  /// Dynamic usage tiers indexed by the exact server supplied name.
  final Map<String, JetStreamAccountTier> tiers;

  /// Deeply immutable complete account response.
  final Map<String, Object?> raw;

  /// Highest supported JetStream API level.
  int get apiLevel => api.level;
}
