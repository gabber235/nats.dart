import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/domain_subject.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/kv.dart";
import "package:nats_jetstream/src/kv_bucket.dart";
import "package:nats_jetstream/src/kv_config_wire.dart";
import "package:nats_jetstream/src/kv_status.dart";
import "package:nats_jetstream/src/stream_manager.dart";
import "package:nats_jetstream/src/stream_wire.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// KV bucket management entry point.
final class JetStreamKeyValueManager {
  /// Creates a manager over one JetStream context.
  const JetStreamKeyValueManager(this.connection, this.apiPrefix, this.timeout);

  /// Core connection used for all requests.
  final NatsConnection connection;

  /// Domain aware JetStream API prefix.
  final String apiPrefix;

  /// Total deadline for one request.
  final Duration timeout;

  /// Creates a bucket with bounded per key history.
  Future<JetStreamKeyValue> createBucket(
    JetStreamKeyValueBucketConfig config,
  ) async {
    final stream = keyValueStreamName(config.name);
    final response = await _request(
      "$apiPrefix.STREAM.CREATE.$stream",
      encodeKeyValueStreamConfig(config, stream),
      requiredApiLevel: config.limitMarkerTtl == null ? null : 1,
    );
    final info = requireStreamIdentity(
      response,
      decodeStreamInfo(response),
      stream,
    );
    final status = decodeKeyValueStatus(info, subject: response.subject);
    if (status.bucket != config.name ||
        status.history != config.history ||
        (config.limitMarkerTtl != null && !info.config.allowMessageTtl) ||
        (config.compression && status.compression != true)) {
      throw JetStreamProtocolException(
        "KV stream response did not preserve bucket invariants",
        subject: response.subject,
        fieldPath: "config",
      );
    }
    return _handle(status);
  }

  /// Opens an existing bucket after proving its stream identity.
  Future<JetStreamKeyValue> bucket(String name) async {
    validateJetStreamKeyValueBucket(name);
    final stream = keyValueStreamName(name);
    final response = await _request("$apiPrefix.STREAM.INFO.$stream", null);
    final info = requireStreamIdentity(
      response,
      decodeStreamInfo(response),
      stream,
    );
    final status = decodeKeyValueStatus(info, subject: response.subject);
    if (status.bucket != name) {
      throw JetStreamProtocolException(
        "Stream is not the requested KV bucket",
        subject: response.subject,
        fieldPath: "config.name",
      );
    }
    return _handle(status);
  }

  /// Updates known bucket fields while preserving unknown stream fields.
  Future<JetStreamKeyValue> update(JetStreamKeyValueBucketConfig config) async {
    final stream = keyValueStreamName(config.name);
    final current = await _request("$apiPrefix.STREAM.INFO.$stream", null);
    final currentInfo = requireStreamIdentity(
      current,
      decodeStreamInfo(current),
      stream,
    );
    final currentStatus = decodeKeyValueStatus(
      currentInfo,
      subject: current.subject,
    );
    if (currentStatus.bucket != config.name) {
      throw JetStreamProtocolException(
        "Stream is not the requested KV bucket",
        subject: current.subject,
        fieldPath: "config.name",
      );
    }
    final desired = encodeKeyValueStreamConfig(config, stream);
    final response = await _request(
      "$apiPrefix.STREAM.UPDATE.$stream",
      mergeKeyValueStreamConfig(
        currentInfo.config.raw,
        desired,
        config.metadata,
      ),
      requiredApiLevel: config.limitMarkerTtl == null ? null : 1,
    );
    final updatedStatus = decodeKeyValueStatus(
      requireStreamIdentity(response, decodeStreamInfo(response), stream),
      subject: response.subject,
    );
    if (updatedStatus.bucket != config.name) {
      throw JetStreamProtocolException(
        "KV update response has the wrong bucket identity",
        subject: response.subject,
        fieldPath: "config.name",
      );
    }
    return _handle(updatedStatus);
  }

  /// Updates a bucket, or creates it only when it is exactly absent.
  Future<JetStreamKeyValue> createOrUpdate(
    JetStreamKeyValueBucketConfig config,
  ) async {
    try {
      return await update(config);
    } on JetStreamApiException catch (error) {
      if (error.errorCode != 10059) rethrow;
      return createBucket(config);
    }
  }

  /// Lazily iterates every valid KV bucket through bounded stream pages.
  Stream<JetStreamKeyValueStatus> list() async* {
    final streams = JetStreamStreamManager(connection, apiPrefix, timeout);
    await for (final info in streams.list(subject: r"$KV.*.>")) {
      if (!info.config.name.startsWith("KV_")) continue;
      yield decodeKeyValueStatus(info, subject: "$apiPrefix.STREAM.LIST");
    }
  }

  /// Deletes a bucket stream and all of its revisions.
  Future<void> deleteBucket(String name) async {
    validateJetStreamKeyValueBucket(name);
    final response = await _request(
      "$apiPrefix.STREAM.DELETE.${keyValueStreamName(name)}",
      null,
    );
    if (!response.optionalBoolean("success")) {
      throw JetStreamProtocolException(
        "KV delete response did not confirm success",
        subject: response.subject,
        fieldPath: "success",
      );
    }
  }

  Future<JetStreamWireResponse> _request(
    String subject,
    Map<String, Object?>? body, {
    int? requiredApiLevel,
  }) async {
    final headers = requiredApiLevel == null
        ? null
        : NatsHeaders(
            entries: [MapEntry("Nats-Required-Api-Level", "$requiredApiLevel")],
          );
    final message = await connection.request(
      subject,
      body == null ? const [] : encodeExactJson(body),
      headers: headers,
      timeout: timeout,
    );
    return JetStreamWireResponse.decode(message.payload, subject)
      ..throwApiError();
  }

  Future<JetStreamKeyValue> _handle(JetStreamKeyValueStatus status) async {
    final mirror = status.mirror;
    if (mirror == null) {
      final prefix = "\$KV.${status.bucket}.";
      return JetStreamKeyValue.verified(
        connection,
        apiPrefix,
        timeout,
        status.bucket,
        allowPerKeyTtl: status.streamInfo.config.allowMessageTtl,
        backingStreamName: keyValueStreamName(status.bucket),
        readSubjectPrefix: prefix,
        editStreamName: keyValueStreamName(status.bucket),
        editSubjectPrefix: jetStreamPublicationSubject(apiPrefix, prefix),
        editApiPrefix: apiPrefix,
      );
    }
    final sourceBucket = keyValueSourceBucket(mirror.name);
    final sourcePrefix = "\$KV.$sourceBucket.";
    final externalApi = mirror.external?.apiPrefix;
    final sourceApi = externalApi ?? apiPrefix;
    final sourceResponse = await _request(
      "$sourceApi.STREAM.INFO.${keyValueStreamName(sourceBucket)}",
      null,
    );
    final sourceStatus = decodeKeyValueStatus(
      requireStreamIdentity(
        sourceResponse,
        decodeStreamInfo(sourceResponse),
        keyValueStreamName(sourceBucket),
      ),
      subject: sourceResponse.subject,
    );
    if (sourceStatus.bucket != sourceBucket) {
      throw JetStreamProtocolException(
        "KV mirror source has the wrong bucket identity",
        subject: sourceResponse.subject,
        fieldPath: "config.name",
      );
    }
    return JetStreamKeyValue.verified(
      connection,
      apiPrefix,
      timeout,
      status.bucket,
      allowPerKeyTtl: sourceStatus.streamInfo.config.allowMessageTtl,
      backingStreamName: keyValueStreamName(status.bucket),
      readSubjectPrefix: sourcePrefix,
      editStreamName: keyValueStreamName(sourceBucket),
      editSubjectPrefix: jetStreamPublicationSubject(sourceApi, sourcePrefix),
      editApiPrefix: sourceApi,
    );
  }
}

/// Backing stream name for [bucket].
String keyValueStreamName(String bucket) => "KV_$bucket";
