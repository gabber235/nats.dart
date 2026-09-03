import "dart:convert";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/direct_get.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/identifiers.dart";
import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/stream_cluster_admin.dart";
import "package:nats_jetstream/src/stream_cluster_models.dart";
import "package:nats_jetstream/src/stream_config.dart";
import "package:nats_jetstream/src/stream_models.dart";
import "package:nats_jetstream/src/stream_operations.dart";
import "package:nats_jetstream/src/stream_peer_admin.dart";
import "package:nats_jetstream/src/stream_peer_models.dart";
import "package:nats_jetstream/src/stream_restore.dart";
import "package:nats_jetstream/src/stream_snapshot.dart";
import "package:nats_jetstream/src/stream_transfer_models.dart";
import "package:nats_jetstream/src/stream_wire.dart";
import "package:nats_jetstream/src/subject_match.dart";
import "package:nats_jetstream/src/wire_integer.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// Complete stream management over one Core connection.
final class JetStreamStreamManager {
  /// Creates a stream manager.
  const JetStreamStreamManager(this.connection, this.apiPrefix, this.timeout);

  /// Core connection used for requests.
  final NatsConnection connection;

  /// Management API prefix.
  final String apiPrefix;

  /// Total deadline for one request.
  final Duration timeout;

  /// Direct replica reads for streams with direct access enabled.
  JetStreamDirectReader get direct =>
      JetStreamDirectReader(connection, apiPrefix, timeout);

  JetStreamStreamClusterAdmin get _clusterAdmin =>
      JetStreamStreamClusterAdmin(connection, apiPrefix, timeout);

  JetStreamStreamPeerAdmin get _peerAdmin =>
      JetStreamStreamPeerAdmin(connection, apiPrefix, timeout);

  /// Sends exactly one validated stream leader stepdown mutation.
  ///
  /// Server success confirms acceptance, not leader convergence. Transport and
  /// timeout failures are ambiguous because NATS may have applied the mutation.
  /// This method never retries. Use [awaitLeaderChange] with the receipt, or
  /// reconcile current stream information after an ambiguous failure.
  Future<JetStreamLeaderStepDownReceipt> requestLeaderStepDown(
    String stream, {
    JetStreamLeaderStepDownSelection selection = const JetStreamAnyLeader(),
  }) => _clusterAdmin.requestLeaderStepDown(stream, selection: selection);

  /// Waits for an accepted stream leader stepdown to converge.
  ///
  /// Read failures are retried within [maximumWait]. Failure throws
  /// [JetStreamLeaderConvergenceException] with the receipt and last topology.
  Future<JetStreamStreamInfo> awaitLeaderChange(
    JetStreamLeaderStepDownReceipt receipt, {
    Duration maximumWait = const Duration(seconds: 30),
    Duration pollInterval = const Duration(milliseconds: 100),
  }) => _clusterAdmin.awaitLeaderChange(
    receipt,
    maximumWait: maximumWait,
    pollInterval: pollInterval,
  );

  /// Requests one stream leader stepdown and waits for convergence.
  ///
  /// The mutation is sent once. Only safe topology reads repeat while waiting.
  Future<JetStreamStreamInfo> stepDownLeader(
    String stream, {
    JetStreamLeaderStepDownSelection selection = const JetStreamAnyLeader(),
    Duration maximumWait = const Duration(seconds: 30),
    Duration pollInterval = const Duration(milliseconds: 100),
  }) => _clusterAdmin.stepDownLeader(
    stream,
    selection: selection,
    maximumWait: maximumWait,
    pollInterval: pollInterval,
  );

  /// Removes one named server and returns the reconciled stream topology.
  ///
  /// The mutation is sent exactly once. Server error `10075` and Core transport
  /// failures preserve their causes because either can follow partial mutation.
  Future<JetStreamPeerRemovalResult> removePeer(
    String stream,
    String server, {
    JetStreamLeaderRemoval leaderRemoval = JetStreamLeaderRemoval.refuse,
    Duration maximumWait = const Duration(seconds: 30),
    Duration pollInterval = const Duration(milliseconds: 100),
  }) => _peerAdmin.removePeer(
    stream,
    server,
    leaderRemoval: leaderRemoval,
    maximumWait: maximumWait,
    pollInterval: pollInterval,
  );

  /// Streams one bounded snapshot into an application sink.
  Future<JetStreamSnapshotResult> snapshotTo(
    String stream,
    JetStreamSnapshotSink write, {
    required BigInt maximumBytes,
    JetStreamSnapshotOptions? options,
  }) => JetStreamSnapshotTransfer(
    connection,
    apiPrefix,
  ).snapshotTo(stream, write, maximumBytes: maximumBytes, options: options);

  /// Restores one exact snapshot from a bounded source stream.
  Future<JetStreamStreamInfo> restoreFrom(
    JetStreamSnapshotMetadata metadata,
    Stream<List<int>> source, {
    required BigInt maximumBytes,
    JetStreamRestoreOptions? options,
  }) => JetStreamRestoreTransfer(
    connection,
    apiPrefix,
    timeout,
  ).restoreFrom(metadata, source, maximumBytes: maximumBytes, options: options);

  /// Creates a stream.
  Future<JetStreamStreamInfo> create(JetStreamStreamConfig config) async {
    final response = await _request(
      "$apiPrefix.STREAM.CREATE.${config.name}",
      body: config.toJson(),
      requiredApiLevel: _requiredApiLevel(config),
    );
    return requireStreamIdentity(
      response,
      decodeStreamInfo(response),
      config.name,
    );
  }

  /// Updates a stream from a complete configuration snapshot.
  Future<JetStreamStreamInfo> update(JetStreamStreamConfig config) async {
    final response = await _request(
      "$apiPrefix.STREAM.UPDATE.${config.name}",
      body: config.toJson(),
      requiredApiLevel: _requiredApiLevel(config),
    );
    return requireStreamIdentity(
      response,
      decodeStreamInfo(response),
      config.name,
    );
  }

  /// Updates a stream, or creates it only when it is exactly absent.
  Future<JetStreamStreamInfo> createOrUpdate(
    JetStreamStreamConfig config,
  ) async {
    try {
      return await update(config);
    } on JetStreamApiException catch (error) {
      if (error.errorCode != 10059) rethrow;
      return create(config);
    }
  }

  /// Reads one stream.
  Future<JetStreamStreamInfo> info(String name) async {
    validateJetStreamStreamName(name);
    final response = await _request("$apiPrefix.STREAM.INFO.$name");
    return requireStreamIdentity(response, decodeStreamInfo(response), name);
  }

  /// Reads one exact information page.
  Future<JetStreamStreamPage> listPage({
    BigInt? offset,
    String? subject,
  }) async {
    final effectiveOffset = _offset(offset);
    if (subject != null) validateSubscriptionSubject(subject);
    final response = await _request(
      "$apiPrefix.STREAM.LIST",
      body: {"offset": effectiveOffset, "subject": ?subject},
    );
    return JetStreamStreamPage(
      total: response.requiredNonNegativeInt64("total"),
      offset: response.requiredNonNegativeInt64("offset"),
      limit: response.requiredNonNegativeInt64("limit"),
      streams: List.unmodifiable(
        response
            .requiredObjectList("streams")
            .map((entry) => decodeStreamInfo(response, source: entry)),
      ),
    );
  }

  /// Reads one exact name page.
  Future<JetStreamStreamNamePage> namesPage({
    BigInt? offset,
    String? subject,
  }) async {
    final effectiveOffset = _offset(offset);
    if (subject != null) validateSubscriptionSubject(subject);
    final response = await _request(
      "$apiPrefix.STREAM.NAMES",
      body: {"offset": effectiveOffset, "subject": ?subject},
    );
    return JetStreamStreamNamePage(
      total: response.requiredNonNegativeInt64("total"),
      offset: response.requiredNonNegativeInt64("offset"),
      limit: response.requiredNonNegativeInt64("limit"),
      names: List.unmodifiable(response.requiredStringList("streams")),
    );
  }

  /// Iterates information through every server page.
  Stream<JetStreamStreamInfo> list({String? subject}) async* {
    var offset = BigInt.zero;
    while (true) {
      final page = await listPage(offset: offset, subject: subject);
      _validatePageOffset(page.offset, offset, "LIST");
      for (final stream in page.streams) {
        yield stream;
      }
      if (page.streams.isEmpty ||
          offset + BigInt.from(page.streams.length) >= page.total) {
        return;
      }
      offset += BigInt.from(page.streams.length);
    }
  }

  /// Iterates names through every server page.
  Stream<String> names({String? subject}) async* {
    var offset = BigInt.zero;
    while (true) {
      final page = await namesPage(offset: offset, subject: subject);
      _validatePageOffset(page.offset, offset, "NAMES");
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

  /// Finds the stream bound to a literal subject.
  Future<String> nameBySubject(String subject) async {
    validatePublishSubject(subject);
    final page = await namesPage(subject: subject);
    if (page.names.isNotEmpty) return page.names.first;
    throw JetStreamApiException(
      subject: "$apiPrefix.STREAM.NAMES",
      code: 404,
      errorCode: 10059,
      description: "stream not found",
    );
  }

  /// Purges matching messages and returns the exact removal count.
  Future<JetStreamPurgeResult> purge(
    String name, {
    JetStreamPurgeOptions? options,
  }) async {
    validateJetStreamStreamName(name);
    final response = await _request(
      "$apiPrefix.STREAM.PURGE.$name",
      body: options?.toJson(),
    );
    _requireSuccess(response, "Stream purge");
    return JetStreamPurgeResult(purged: response.requiredUnsigned("purged"));
  }

  /// Retrieves one stored message by sequence.
  Future<JetStreamStoredMessage> getMessage(String stream, BigInt sequence) {
    validatePositiveJetStreamUint64(sequence, "sequence");
    return _get(stream, {"seq": sequence}, expectedSequence: sequence);
  }

  /// Retrieves the newest message for one subject pattern.
  Future<JetStreamStoredMessage> getLastMessage(String stream, String subject) {
    validateSubscriptionSubject(subject);
    return _get(stream, {
      "last_by_subj": subject,
    }, expectedSubjectPattern: subject);
  }

  /// Retrieves the first matching message at or after a sequence.
  Future<JetStreamStoredMessage> getNextMessage(
    String stream,
    String subject,
    BigInt startSequence,
  ) {
    validateSubscriptionSubject(subject);
    validatePositiveJetStreamUint64(startSequence, "startSequence");
    return _get(stream, {
      "next_by_subj": subject,
      "seq": startSequence,
    }, expectedSubjectPattern: subject);
  }

  /// Deletes one message and optionally overwrites its storage bytes.
  Future<void> deleteMessage(
    String stream,
    BigInt sequence, {
    bool secure = false,
  }) async {
    validateJetStreamStreamName(stream);
    validatePositiveJetStreamUint64(sequence, "sequence");
    final response = await _request(
      "$apiPrefix.STREAM.MSG.DELETE.$stream",
      body: {"seq": sequence, if (!secure) "no_erase": true},
    );
    _requireSuccess(response, "Message delete");
  }

  /// Deletes one stream.
  Future<void> delete(String name) async {
    validateJetStreamStreamName(name);
    final response = await _request("$apiPrefix.STREAM.DELETE.$name");
    _requireSuccess(response, "Stream delete");
  }

  Future<JetStreamStoredMessage> _get(
    String stream,
    Map<String, Object?> selector, {
    BigInt? expectedSequence,
    String? expectedSubjectPattern,
  }) async {
    validateJetStreamStreamName(stream);
    final response = await _request(
      "$apiPrefix.STREAM.MSG.GET.$stream",
      body: selector,
    );
    final message = response.requiredObject("message");
    final subject = response.requiredString("message.subject", source: message);
    final sequence = response.requiredPositiveUint64(
      "message.seq",
      source: message,
    );
    if (expectedSequence != null && sequence != expectedSequence) {
      throw JetStreamProtocolException(
        "Stored message sequence does not match the requested sequence",
        subject: response.subject,
        fieldPath: "message.seq",
      );
    }
    if (expectedSubjectPattern != null &&
        !matchesNatsSubjectPattern(expectedSubjectPattern, subject)) {
      throw JetStreamProtocolException(
        "Stored message subject does not match the requested pattern",
        subject: response.subject,
        fieldPath: "message.subject",
      );
    }
    final data = response.optionalString("message.data", source: message);
    final headerData = response.optionalString("message.hdrs", source: message);
    try {
      return JetStreamStoredMessage(
        subject: subject,
        sequence: sequence,
        payload: data == null ? const [] : base64Decode(data),
        timestamp: response.requiredRfc3339("message.time", source: message),
        headers: headerData == null
            ? null
            : NatsHeaders.parse(base64Decode(headerData)),
      );
    } on FormatException catch (error) {
      throw JetStreamProtocolException(
        "Stored message contains invalid base64",
        subject: response.subject,
        fieldPath: "message.data",
        cause: error,
      );
    } on NatsHeaderException catch (error) {
      throw JetStreamProtocolException(
        "Stored message contains invalid headers",
        subject: response.subject,
        fieldPath: "message.hdrs",
        cause: error,
      );
    }
  }

  Future<JetStreamWireResponse> _request(
    String subject, {
    Map<String, Object?>? body,
    int? requiredApiLevel,
  }) async {
    final headers = requiredApiLevel == null
        ? null
        : NatsHeaders(
            entries: [MapEntry("Nats-Required-Api-Level", "$requiredApiLevel")],
          );
    final message = await connection.request(
      subject,
      encodeExactJson(body ?? const <String, Object?>{}),
      headers: headers,
      timeout: timeout,
    );
    return JetStreamWireResponse.decode(message.payload, subject)
      ..throwApiError();
  }

  void _requireSuccess(JetStreamWireResponse response, String operation) {
    if (!response.optionalBoolean("success")) {
      throw JetStreamProtocolException(
        "$operation response did not confirm success",
        subject: response.subject,
        fieldPath: "success",
      );
    }
  }

  void _validatePageOffset(BigInt actual, BigInt expected, String operation) {
    if (actual != expected) {
      throw JetStreamProtocolException(
        "Stream $operation page offset does not match the request",
        subject: "$apiPrefix.STREAM.$operation",
        fieldPath: "offset",
      );
    }
  }
}

BigInt _offset(BigInt? value) {
  final offset = value ?? BigInt.zero;
  validateJetStreamInt64(offset, "offset");
  if (offset.isNegative) {
    throw ArgumentError.value(offset, "offset", "Must not be negative");
  }
  return offset;
}

int? _requiredApiLevel(JetStreamStreamConfig config) {
  final sources = config.raw["sources"] as List<Object?>? ?? const [];
  final hasDurableSource = sources.any(
    (source) => source is Map<String, Object?> && source["consumer"] != null,
  );
  if (config.allowFastBatchPublish || hasDurableSource) return 4;
  if (config.allowMessageCounter ||
      config.allowAtomicPublish ||
      config.allowMessageSchedules ||
      config.raw["persist_mode"] == "async") {
    return 2;
  }
  if (config.allowMessageTtl || config.subjectDeleteMarkerTtl != null) return 1;
  return null;
}
