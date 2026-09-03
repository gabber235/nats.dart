import "dart:convert";
import "dart:math";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/object_store_stream.dart";
import "package:nats_jetstream/src/wire_integer.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// Internal Object Store request boundary.
final class JetStreamObjectProtocol {
  /// Creates a request boundary for one verified bucket.
  const JetStreamObjectProtocol(
    this.connection,
    this.apiPrefix,
    this.timeout,
    this.bucket,
  );

  /// Core connection used for every request.
  final NatsConnection connection;

  /// Domain aware JetStream API prefix.
  final String apiPrefix;

  /// Deadline for each protocol request.
  final Duration timeout;

  /// Public Object Store bucket name.
  final String bucket;

  /// Publishes one chunk or metadata message with acknowledgement validation.
  Future<BigInt> publish(
    String subject,
    List<int> payload, {
    NatsHeaders? headers,
    int? generation,
  }) async {
    final expectedStreamHeaders = (headers ?? NatsHeaders()).set(
      "Nats-Expected-Stream",
      objectStreamName(bucket),
    );
    final message = generation == null
        ? await connection.request(
            subject,
            payload,
            headers: expectedStreamHeaders,
            timeout: timeout,
          )
        : await connection.requestOnGeneration(
            subject,
            payload,
            generation: generation,
            headers: expectedStreamHeaders,
            timeout: timeout,
          );
    final response = JetStreamWireResponse.decode(message.payload, subject)
      ..throwApiError();
    if (response.requiredString("stream") != objectStreamName(bucket)) {
      throw JetStreamProtocolException(
        "Object publication acknowledgement has the wrong stream",
        subject: subject,
        fieldPath: "stream",
      );
    }
    return response.requiredPositiveUint64("seq");
  }

  /// Retrieves one stored message through the JetStream API.
  Future<JetStreamStoredObjectMessage?> getStored(
    Map<String, Object?> body, {
    bool missingIsNull = false,
  }) async {
    final subject = "$apiPrefix.STREAM.MSG.GET.${objectStreamName(bucket)}";
    JetStreamWireResponse response;
    try {
      final message = await connection.request(
        subject,
        encodeExactJson(body),
        timeout: timeout,
      );
      response = JetStreamWireResponse.decode(message.payload, subject)
        ..throwApiError();
    } on JetStreamApiException catch (error) {
      if (missingIsNull && error.errorCode == 10037) {
        return null;
      }
      rethrow;
    }
    final stored = response.requiredObject("message");
    try {
      return JetStreamStoredObjectMessage(
        requestSubject: subject,
        subject: response.requiredString("message.subject", source: stored),
        sequence: response.requiredPositiveUint64(
          "message.seq",
          source: stored,
        ),
        payload: base64Decode(
          response.requiredString("message.data", source: stored),
        ),
        time: response.requiredRfc3339("message.time", source: stored),
      );
    } on FormatException catch (error) {
      throw JetStreamProtocolException(
        "Object message contains invalid base64",
        subject: subject,
        fieldPath: "message.data",
        cause: error,
      );
    }
  }

  /// Purges every stored message on one exact subject.
  Future<void> purgeSubject(String filter, {BigInt? beforeSequence}) async {
    if (beforeSequence != null) {
      validatePositiveJetStreamUint64(beforeSequence, "beforeSequence");
    }
    final subject = "$apiPrefix.STREAM.PURGE.${objectStreamName(bucket)}";
    final message = await connection.request(
      subject,
      encodeExactJson({"filter": filter, "seq": ?beforeSequence}),
      timeout: timeout,
    );
    final response = JetStreamWireResponse.decode(message.payload, subject)
      ..throwApiError();
    if (!response.optionalBoolean("success")) {
      throw JetStreamProtocolException(
        "Object chunk purge did not confirm success",
        subject: subject,
        fieldPath: "success",
      );
    }
  }

  /// Revalidates this bucket handle against its current stream identity.
  Future<void> verifyBucket() async {
    final response = await streamInfo();
    verifyObjectStoreIdentity(response, bucket);
  }

  /// Reads the full backing stream response without dropping unknown config.
  Future<JetStreamWireResponse> streamInfo() => _streamRequest(
    "$apiPrefix.STREAM.INFO.${objectStreamName(bucket)}",
    const {},
  );

  /// Replaces the backing stream config with a preserved full response copy.
  Future<JetStreamWireResponse> updateStream(Map<String, Object?> config) =>
      _streamRequest(
        "$apiPrefix.STREAM.UPDATE.${objectStreamName(bucket)}",
        config,
      );

  Future<JetStreamWireResponse> _streamRequest(
    String subject,
    Map<String, Object?> body,
  ) async {
    final message = await connection.request(
      subject,
      encodeExactJson(body),
      timeout: timeout,
    );
    return JetStreamWireResponse.decode(message.payload, subject)
      ..throwApiError();
  }

  /// Metadata subject for an unrestricted UTF 8 object name.
  String metadataSubject(String objectName) =>
      "\$O.$bucket.M.${base64Url.encode(utf8.encode(objectName))}";

  /// Chunk subject for one opaque object identifier.
  String chunkSubject(String identifier) => "\$O.$bucket.C.$identifier";
}

/// Strict stored message returned by the direct get API.
final class JetStreamStoredObjectMessage {
  /// Creates a decoded stored message.
  const JetStreamStoredObjectMessage({
    required this.requestSubject,
    required this.subject,
    required this.sequence,
    required this.payload,
    required this.time,
  });

  /// JetStream API subject used for retrieval.
  final String requestSubject;

  /// Original stored subject.
  final String subject;

  /// Exact stream sequence.
  final BigInt sequence;

  /// Decoded stored payload.
  final List<int> payload;

  /// Exact server assigned RFC 3339 timestamp.
  final String time;
}

/// Creates one cryptographically strong subject safe object identifier.
String randomObjectIdentifier() {
  const alphabet =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  final random = Random.secure();
  return List.generate(
    22,
    (_) => alphabet[random.nextInt(alphabet.length)],
    growable: false,
  ).join();
}
