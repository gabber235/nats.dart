import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/consumer_config.dart";
import "package:nats_jetstream/src/consumer_fetch.dart";
import "package:nats_jetstream/src/consumer_models.dart";
import "package:nats_jetstream/src/consumer_push_config.dart";
import "package:nats_jetstream/src/consumer_validation.dart";
import "package:nats_jetstream/src/consumer_wire.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/identifiers.dart";
import "package:nats_jetstream/src/stream_wire.dart";
import "package:nats_jetstream/src/subject_pattern_set.dart";
import "package:nats_jetstream/src/timestamp.dart";
import "package:nats_jetstream/src/wire_integer.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// Typed consumer management over one JetStream context.
final class JetStreamConsumerManager {
  /// Creates a manager.
  const JetStreamConsumerManager(this.connection, this.apiPrefix, this.timeout);

  /// Core connection used for management requests.
  final NatsConnection connection;

  /// Domain aware JetStream API prefix.
  final String apiPrefix;

  /// Total deadline for one management request.
  final Duration timeout;

  /// Creates a pull consumer and rejects a conflicting existing definition.
  Future<JetStreamConsumerInfo> createPull(
    String stream,
    JetStreamPullConsumerConfig config,
  ) => _mutate(
    stream,
    config.name,
    config.filters,
    encodePullConsumerConfig(config),
    "create",
  );

  /// Updates an existing pull consumer.
  Future<JetStreamConsumerInfo> updatePull(
    String stream,
    JetStreamPullConsumerConfig config,
  ) => _mutate(
    stream,
    config.name,
    config.filters,
    encodePullConsumerConfig(config),
    "update",
  );

  /// Creates or updates a pull consumer.
  Future<JetStreamConsumerInfo> createOrUpdatePull(
    String stream,
    JetStreamPullConsumerConfig config,
  ) => _mutate(
    stream,
    config.name,
    config.filters,
    encodePullConsumerConfig(config),
    "",
  );

  /// Creates a push consumer.
  Future<JetStreamConsumerInfo> createPush(
    String stream,
    JetStreamPushConsumerConfig config,
  ) => _mutate(
    stream,
    config.name,
    config.filters,
    encodePushConsumerConfig(config),
    "create",
  );

  /// Updates an existing push consumer.
  Future<JetStreamConsumerInfo> updatePush(
    String stream,
    JetStreamPushConsumerConfig config,
  ) => _mutate(
    stream,
    config.name,
    config.filters,
    encodePushConsumerConfig(config),
    "update",
  );

  /// Creates or updates a push consumer.
  Future<JetStreamConsumerInfo> createOrUpdatePush(
    String stream,
    JetStreamPushConsumerConfig config,
  ) => _mutate(
    stream,
    config.name,
    config.filters,
    encodePushConsumerConfig(config),
    "",
  );

  /// Creates a strict NATS 2.14 reliable source consumer.
  Future<JetStreamConsumerInfo> createReliableSource(
    String stream,
    JetStreamReliableSourceConsumerConfig config,
  ) => _mutate(
    stream,
    config.name,
    config.filters,
    encodeReliableSourceConsumerConfig(config),
    "create",
    requiredApiLevel: 4,
  );

  /// Updates an existing strict NATS 2.14 reliable source consumer.
  Future<JetStreamConsumerInfo> updateReliableSource(
    String stream,
    JetStreamReliableSourceConsumerConfig config,
  ) => _mutate(
    stream,
    config.name,
    config.filters,
    encodeReliableSourceConsumerConfig(config),
    "update",
    requiredApiLevel: 4,
  );

  /// Creates or updates a strict NATS 2.14 reliable source consumer.
  Future<JetStreamConsumerInfo> createOrUpdateReliableSource(
    String stream,
    JetStreamReliableSourceConsumerConfig config,
  ) => _mutate(
    stream,
    config.name,
    config.filters,
    encodeReliableSourceConsumerConfig(config),
    "",
    requiredApiLevel: 4,
  );

  /// Reads current state without using a cached snapshot.
  Future<JetStreamConsumerInfo> info(String stream, String consumer) async {
    _validateIdentity(stream, consumer);
    final response = await _request(
      "$apiPrefix.CONSUMER.INFO.$stream.$consumer",
    );
    return requireConsumerIdentity(
      response,
      decodeConsumerInfo(response),
      expectedStream: stream,
      expectedConsumer: consumer,
    );
  }

  /// Opens an existing pull consumer without creating hidden server state.
  Future<JetStreamPullConsumer> pull(String stream, String consumer) async {
    final current = await info(stream, consumer);
    if (current.config.kind != JetStreamConsumerKind.pull) {
      throw ArgumentError.value(
        consumer,
        "consumer",
        "Consumer is not pull based",
      );
    }
    return JetStreamPullConsumer(connection, apiPrefix, stream, consumer);
  }

  /// Deletes a consumer and requires explicit server confirmation.
  Future<void> delete(String stream, String consumer) async {
    _validateIdentity(stream, consumer);
    final response = await _request(
      "$apiPrefix.CONSUMER.DELETE.$stream.$consumer",
    );
    if (!response.optionalBoolean("success")) {
      throw JetStreamProtocolException(
        "Consumer delete response did not confirm success",
        subject: response.subject,
        fieldPath: "success",
      );
    }
  }

  /// Reads one exact page while preserving missing and offline members.
  Future<JetStreamConsumerPage> listPage(
    String stream, {
    BigInt? offset,
  }) async {
    validateJetStreamStreamName(stream);
    final effectiveOffset = _validateOffset(offset);
    final response = await _request(
      "$apiPrefix.CONSUMER.LIST.$stream",
      body: {"offset": effectiveOffset},
    );
    return JetStreamConsumerPage(
      total: response.requiredNonNegativeInt64("total"),
      offset: response.requiredNonNegativeInt64("offset"),
      limit: response.requiredNonNegativeInt64("limit"),
      consumers: response
          .requiredObjectList("consumers")
          .map((entry) {
            final info = decodeConsumerInfo(response, source: entry);
            return requireConsumerIdentity(
              response,
              info,
              expectedStream: stream,
            );
          })
          .toList(growable: false),
      missing: response.optionalStringList("missing"),
      offline: response.optionalStringMap("offline"),
    );
  }

  /// Reads one exact page of consumer names.
  Future<JetStreamConsumerNamePage> namesPage(
    String stream, {
    BigInt? offset,
  }) async {
    validateJetStreamStreamName(stream);
    final effectiveOffset = _validateOffset(offset);
    final response = await _request(
      "$apiPrefix.CONSUMER.NAMES.$stream",
      body: {"offset": effectiveOffset},
    );
    return JetStreamConsumerNamePage(
      total: response.requiredNonNegativeInt64("total"),
      offset: response.requiredNonNegativeInt64("offset"),
      limit: response.requiredNonNegativeInt64("limit"),
      names: response.requiredStringList("consumers"),
    );
  }

  /// Iterates consumer names through every server page.
  Stream<String> names(String stream) async* {
    var offset = BigInt.zero;
    while (true) {
      final page = await namesPage(stream, offset: offset);
      if (page.offset != offset) {
        throw JetStreamProtocolException(
          "Consumer name page offset does not match the request",
          subject: "$apiPrefix.CONSUMER.NAMES.$stream",
          fieldPath: "offset",
        );
      }
      for (final name in page.names) {
        yield name;
      }
      if (page.names.isEmpty ||
          offset + BigInt.from(page.names.length) >= page.total) {
        return;
      }
      offset += BigInt.from(page.names.length);
    }
  }

  /// Pauses delivery until one exact UTC deadline.
  Future<JetStreamConsumerPauseResult> pause(
    String stream,
    String consumer,
    DateTime pauseUntil,
  ) {
    _validateIdentity(stream, consumer);
    if (!pauseUntil.isUtc) {
      throw ArgumentError.value(pauseUntil, "pauseUntil", "Must be UTC");
    }
    return _pause(stream, consumer, {
      "pause_until": serializeJetStreamRfc3339(pauseUntil, "pauseUntil"),
    });
  }

  /// Resumes delivery immediately.
  Future<JetStreamConsumerPauseResult> resume(String stream, String consumer) {
    _validateIdentity(stream, consumer);
    return _pause(stream, consumer, const {});
  }

  /// Explicitly releases one pinned priority group.
  Future<void> unpin(String stream, String consumer, String group) async {
    _validateIdentity(stream, consumer);
    validateConsumerPriorityGroup(group);
    await _request(
      "$apiPrefix.CONSUMER.UNPIN.$stream.$consumer",
      body: {"group": group},
    );
  }

  /// Resets delivery to the current acknowledgement floor.
  Future<JetStreamConsumerResetResult> reset(String stream, String consumer) =>
      _reset(stream, consumer, null);

  /// Resets delivery to one explicit positive stream sequence.
  Future<JetStreamConsumerResetResult> resetToSequence(
    String stream,
    String consumer,
    BigInt sequence,
  ) {
    validatePositiveJetStreamUint64(sequence, "sequence");
    return _reset(stream, consumer, sequence);
  }

  Future<JetStreamConsumerInfo> _mutate(
    String stream,
    String consumer,
    JetStreamConsumerFilters filters,
    Map<String, Object?> config,
    String action, {
    int? requiredApiLevel,
  }) async {
    _validateIdentity(stream, consumer);
    await _validateFiltersIntersectStream(stream, filters);
    final singleFilter = filters.subjects.length == 1
        ? filters.subjects.single
        : null;
    final filterTail =
        singleFilter != null &&
            !singleFilter.contains("*") &&
            !singleFilter.contains(">")
        ? ".$singleFilter"
        : "";
    final response = await _request(
      "$apiPrefix.CONSUMER.CREATE.$stream.$consumer$filterTail",
      body: {"stream_name": stream, "config": config, "action": action},
      requiredApiLevel: requiredApiLevel,
    );
    final info = requireConsumerIdentity(
      response,
      decodeConsumerInfo(response),
      expectedStream: stream,
      expectedConsumer: consumer,
    );
    if (!_sameStrings(info.config.filterSubjects, filters.subjects)) {
      throw JetStreamProtocolException(
        "Consumer mutation response filters do not match the request",
        subject: response.subject,
        fieldPath: "config.filter_subjects",
      );
    }
    return info;
  }

  Future<void> _validateFiltersIntersectStream(
    String stream,
    JetStreamConsumerFilters filters,
  ) async {
    if (filters.subjects.isEmpty) return;
    final response = await _request("$apiPrefix.STREAM.INFO.$stream");
    final info = requireStreamIdentity(
      response,
      decodeStreamInfo(response),
      stream,
    );
    final streamSubjects = info.config.subjects;
    if (streamSubjects.isEmpty) return;
    validateNatsSubjectPatternsIntersectAny(
      filters.subjects,
      streamSubjects,
      name: "filters",
    );
  }

  Future<JetStreamConsumerResetResult> _reset(
    String stream,
    String consumer,
    BigInt? sequence,
  ) async {
    _validateIdentity(stream, consumer);
    final response = await _request(
      "$apiPrefix.CONSUMER.RESET.$stream.$consumer",
      body: sequence == null ? null : {"seq": sequence},
      payload: sequence == null ? const [] : null,
      requiredApiLevel: 4,
    );
    return JetStreamConsumerResetResult(
      resetSequence: response.requiredUnsigned("reset_seq"),
      info: requireConsumerIdentity(
        response,
        decodeConsumerInfo(response),
        expectedStream: stream,
        expectedConsumer: consumer,
      ),
    );
  }

  Future<JetStreamConsumerPauseResult> _pause(
    String stream,
    String consumer,
    Map<String, Object?> body,
  ) async {
    final response = await _request(
      "$apiPrefix.CONSUMER.PAUSE.$stream.$consumer",
      body: body,
    );
    return JetStreamConsumerPauseResult(
      paused: response.requiredBoolean("paused"),
      pauseUntil: response.requiredRfc3339("pause_until"),
      pauseRemaining: response.optionalNanoseconds("pause_remaining"),
    );
  }

  Future<JetStreamWireResponse> _request(
    String subject, {
    Map<String, Object?>? body,
    List<int>? payload,
    int? requiredApiLevel,
  }) async {
    final headers = requiredApiLevel == null
        ? null
        : NatsHeaders(
            entries: [MapEntry("Nats-Required-Api-Level", "$requiredApiLevel")],
          );
    final message = await connection.request(
      subject,
      payload ?? (body == null ? const [] : encodeExactJson(body)),
      headers: headers,
      timeout: timeout,
    );
    return JetStreamWireResponse.decode(message.payload, subject)
      ..throwApiError();
  }

  void _validateIdentity(String stream, String consumer) {
    validateJetStreamStreamName(stream);
    validateJetStreamConsumerName(consumer);
  }

  BigInt _validateOffset(BigInt? offset) {
    final effectiveOffset = offset ?? BigInt.zero;
    validateJetStreamInt64(effectiveOffset, "offset");
    if (effectiveOffset.isNegative) {
      throw ArgumentError.value(
        effectiveOffset,
        "offset",
        "Must not be negative",
      );
    }
    return effectiveOffset;
  }
}

bool _sameStrings(List<String> left, List<String> right) {
  if (left.length != right.length) {
    return false;
  }
  for (var index = 0; index < left.length; index += 1) {
    if (left[index] != right[index]) {
      return false;
    }
  }
  return true;
}
