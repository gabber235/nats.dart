import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/object_store.dart";
import "package:nats_jetstream/src/object_store_bucket.dart";
import "package:nats_jetstream/src/object_store_stream.dart";
import "package:nats_jetstream/src/stream_manager.dart";
import "package:nats_jetstream/src/stream_wire.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// Object Store management entry point.
final class JetStreamObjectStoreManager {
  /// Creates a manager over one JetStream context.
  const JetStreamObjectStoreManager(
    this.connection,
    this.apiPrefix,
    this.timeout,
  );

  /// Core connection used for all requests.
  final NatsConnection connection;

  /// Domain aware JetStream API prefix.
  final String apiPrefix;

  /// Total deadline for one request.
  final Duration timeout;

  /// Creates a new Object Store bucket.
  Future<JetStreamObjectStore> createStore(
    JetStreamObjectStoreConfig config,
  ) async {
    final stream = objectStreamName(config.name);
    final response = await _request("$apiPrefix.STREAM.CREATE.$stream", {
      "name": stream,
      "description": ?config.description,
      "subjects": objectStreamSubjects(config.name),
      "retention": "limits",
      "max_age": ?config.maximumAge?.nanoseconds,
      "max_bytes": config.maximumBytes ?? BigInt.from(-1),
      "storage": config.storage.wireValue,
      "num_replicas": config.replicas,
      "discard": "new",
      "allow_rollup_hdrs": true,
      "allow_direct": true,
      if (config.metadata.isNotEmpty) "metadata": config.metadata,
      if (config.compression) "compression": "s2",
      "placement": ?config.placement?.toJson(),
      if (config.experimentalFastIngest) "allow_batched": true,
    });
    verifyObjectStoreIdentity(response, config.name);
    return JetStreamObjectStore(connection, apiPrefix, timeout, config.name);
  }

  /// Opens an existing bucket after proving its stream identity.
  Future<JetStreamObjectStore> store(String name) async {
    validateJetStreamObjectBucket(name);
    final response = await _request(
      "$apiPrefix.STREAM.INFO.${objectStreamName(name)}",
      null,
    );
    verifyObjectStoreIdentity(response, name);
    return JetStreamObjectStore(connection, apiPrefix, timeout, name);
  }

  /// Updates known bucket configuration while preserving unknown stream fields.
  Future<JetStreamObjectStore> update(JetStreamObjectStoreConfig config) async {
    final stream = objectStreamName(config.name);
    final current = await _request("$apiPrefix.STREAM.INFO.$stream", null);
    final currentInfo = decodeStreamInfo(current);
    verifyObjectStoreStreamInfo(
      currentInfo,
      bucket: config.name,
      subject: current.subject,
    );
    final response = await _request(
      "$apiPrefix.STREAM.UPDATE.$stream",
      _updatedConfig(currentInfo.config.raw, config),
    );
    verifyObjectStoreIdentity(response, config.name);
    return JetStreamObjectStore(connection, apiPrefix, timeout, config.name);
  }

  /// Updates a bucket, or creates it only when it is exactly absent.
  Future<JetStreamObjectStore> createOrUpdate(
    JetStreamObjectStoreConfig config,
  ) async {
    try {
      return await update(config);
    } on JetStreamApiException catch (error) {
      if (error.errorCode != 10059) rethrow;
      return createStore(config);
    }
  }

  /// Lazily iterates verified statuses through bounded server pages.
  Stream<JetStreamObjectStoreStatus> list() async* {
    final streams = JetStreamStreamManager(connection, apiPrefix, timeout);
    await for (final info in streams.list(subject: r"$O.*.C.>")) {
      if (!info.config.name.startsWith("OBJ_")) continue;
      yield decodeObjectStoreStatus(info, subject: "$apiPrefix.STREAM.LIST");
    }
  }

  /// Lazily iterates names of verified buckets through bounded server pages.
  Stream<String> names() async* {
    await for (final status in list()) {
      yield status.name;
    }
  }

  /// Deletes a bucket stream and all of its objects.
  Future<void> deleteStore(String name) async {
    validateJetStreamObjectBucket(name);
    final response = await _request(
      "$apiPrefix.STREAM.DELETE.${objectStreamName(name)}",
      null,
    );
    if (!response.optionalBoolean("success")) {
      throw JetStreamProtocolException(
        "Object Store delete response did not confirm success",
        subject: response.subject,
        fieldPath: "success",
      );
    }
  }

  Future<JetStreamWireResponse> _request(
    String subject,
    Map<String, Object?>? body,
  ) async {
    final message = await connection.request(
      subject,
      body == null ? const [] : encodeExactJson(body),
      timeout: timeout,
    );
    return JetStreamWireResponse.decode(message.payload, subject)
      ..throwApiError();
  }
}

Map<String, Object?> _updatedConfig(
  Map<String, Object?> current,
  JetStreamObjectStoreConfig desired,
) {
  final updated = Map<String, Object?>.from(current);
  if (desired.description == null) {
    updated.remove("description");
  } else {
    updated["description"] = desired.description;
  }
  updated
    ..["max_age"] = desired.maximumAge?.nanoseconds ?? BigInt.zero
    ..["max_bytes"] = desired.maximumBytes ?? BigInt.from(-1)
    ..["storage"] = desired.storage.wireValue
    ..["num_replicas"] = desired.replicas
    ..["compression"] = desired.compression ? "s2" : "none";
  final currentMetadata = current["metadata"];
  final metadata = <String, String>{
    if (currentMetadata is Map<String, Object?>)
      for (final entry in currentMetadata.entries)
        if (entry.key.startsWith("_nats.") && entry.value is String)
          entry.key: entry.value as String,
    ...desired.metadata,
  };
  if (metadata.isEmpty) {
    updated.remove("metadata");
  } else {
    updated["metadata"] = metadata;
  }
  if (desired.placement == null) {
    updated.remove("placement");
  } else {
    updated["placement"] = desired.placement!.toJson();
  }
  if (desired.experimentalFastIngest) {
    updated["allow_batched"] = true;
  } else {
    updated.remove("allow_batched");
  }
  return updated;
}
