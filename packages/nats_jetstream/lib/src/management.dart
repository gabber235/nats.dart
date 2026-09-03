import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/account_models.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// Internal owner for JetStream management requests and wire mapping.
final class JetStreamManagement {
  /// Creates a management client over one Core connection.
  const JetStreamManagement(this.connection, this.apiPrefix, this.timeout);

  /// Core connection used for requests.
  final NatsConnection connection;

  /// Management API prefix.
  final String apiPrefix;

  /// Total deadline for one request.
  final Duration timeout;

  /// Reads account resource and capability state.
  Future<JetStreamAccountInfo> accountInfo() async {
    final response = await _request("$apiPrefix.INFO");
    return decodeAccountInfo(response);
  }

  /// Decodes one strict account information response.
  static JetStreamAccountInfo decodeAccountInfo(
    JetStreamWireResponse response,
  ) {
    final limits = response.requiredObject("limits");
    final api = response.requiredObject("api");
    final tiersObject = response.optionalObject("tiers");
    final tiers = <String, JetStreamAccountTier>{};
    if (tiersObject != null) {
      for (final entry in tiersObject.entries) {
        final source = entry.value;
        if (source is! Map<String, Object?>) {
          throw JetStreamProtocolException(
            "Field tiers.${entry.key} must be an object",
            subject: response.subject,
            fieldPath: "tiers.${entry.key}",
          );
        }
        tiers[entry.key] = _decodeTier(response, entry.key, source);
      }
    }
    return JetStreamAccountInfo(
      memoryBytes: response.requiredUnsigned("memory"),
      storageBytes: response.requiredUnsigned("storage"),
      reservedMemoryBytes: _optionalUnsigned(
        response,
        "reserved_memory",
        response.body,
      ),
      reservedStorageBytes: _optionalUnsigned(
        response,
        "reserved_storage",
        response.body,
      ),
      streams: response.requiredNonNegativeInt64("streams"),
      consumers: response.requiredNonNegativeInt64("consumers"),
      limits: _decodeLimits(response, "limits", limits),
      api: _decodeApi(response, api),
      domain: response.optionalString("domain"),
      tiers: tiers,
      raw: response.body,
    );
  }

  Future<JetStreamWireResponse> _request(String subject) async {
    final message = await connection.request(
      subject,
      encodeExactJson(const <String, Object?>{}),
      timeout: timeout,
    );
    final response = JetStreamWireResponse.decode(message.payload, subject)
      ..throwApiError();
    return response;
  }
}

JetStreamAccountTier _decodeTier(
  JetStreamWireResponse response,
  String name,
  Map<String, Object?> source,
) {
  final limits = response.requiredObject("tiers.$name.limits", source: source);
  return JetStreamAccountTier(
    memoryBytes: response.requiredUnsigned(
      "tiers.$name.memory",
      source: source,
    ),
    storageBytes: response.requiredUnsigned(
      "tiers.$name.storage",
      source: source,
    ),
    reservedMemoryBytes: _optionalUnsigned(
      response,
      "tiers.$name.reserved_memory",
      source,
    ),
    reservedStorageBytes: _optionalUnsigned(
      response,
      "tiers.$name.reserved_storage",
      source,
    ),
    streams: response.requiredNonNegativeInt64(
      "tiers.$name.streams",
      source: source,
    ),
    consumers: response.requiredNonNegativeInt64(
      "tiers.$name.consumers",
      source: source,
    ),
    limits: _decodeLimits(response, "tiers.$name.limits", limits),
    raw: source,
  );
}

JetStreamAccountLimits _decodeLimits(
  JetStreamWireResponse response,
  String path,
  Map<String, Object?> source,
) {
  return JetStreamAccountLimits(
    maximumMemoryBytes: _decodeLimit(response, "$path.max_memory", source),
    maximumStorageBytes: _decodeLimit(response, "$path.max_storage", source),
    maximumStreams: _decodeLimit(response, "$path.max_streams", source),
    maximumConsumers: _decodeLimit(response, "$path.max_consumers", source),
    maximumAcknowledgementsPending: _decodeLimit(
      response,
      "$path.max_ack_pending",
      source,
    ),
    maximumMemoryStreamBytes: _decodeLimit(
      response,
      "$path.memory_max_stream_bytes",
      source,
    ),
    maximumStorageStreamBytes: _decodeLimit(
      response,
      "$path.storage_max_stream_bytes",
      source,
    ),
    maximumBytesRequired: response.optionalBoolean(
      "$path.max_bytes_required",
      source: source,
    ),
    raw: source,
  );
}

BigInt _decodeLimit(
  JetStreamWireResponse response,
  String path,
  Map<String, Object?> source,
) {
  final value = response.requiredSignedInt64(path, source: source);
  if (value < -BigInt.one) {
    throw JetStreamProtocolException(
      "Field $path must be at least minus one",
      subject: response.subject,
      fieldPath: path,
    );
  }
  return value;
}

JetStreamApiStats _decodeApi(
  JetStreamWireResponse response,
  Map<String, Object?> source,
) {
  final level = response.optionalInt("api.level", source: source) ?? 0;
  if (level < 0) {
    throw JetStreamProtocolException(
      "Field api.level must be an unsigned integer",
      subject: response.subject,
      fieldPath: "api.level",
    );
  }
  return JetStreamApiStats(
    level: level,
    total: response.requiredUnsigned("api.total", source: source),
    errors: response.requiredUnsigned("api.errors", source: source),
    inflight: _optionalUnsigned(response, "api.inflight", source),
    raw: source,
  );
}

BigInt _optionalUnsigned(
  JetStreamWireResponse response,
  String path,
  Map<String, Object?> source,
) {
  final key = path.substring(path.lastIndexOf(".") + 1);
  return source.containsKey(key)
      ? response.requiredUnsigned(path, source: source)
      : BigInt.zero;
}
