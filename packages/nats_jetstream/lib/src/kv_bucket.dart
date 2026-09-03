import "dart:convert";
import "dart:typed_data";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/exact_utf8.dart";
import "package:nats_jetstream/src/kv.dart";
import "package:nats_jetstream/src/kv_codec.dart";
import "package:nats_jetstream/src/kv_codecs.dart";
import "package:nats_jetstream/src/publish_headers.dart";
import "package:nats_jetstream/src/publish_options.dart";
import "package:nats_jetstream/src/wire_integer.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// One existing KV bucket.
final class JetStreamKeyValue {
  /// Creates a handle after the manager verifies backing stream identity.
  const JetStreamKeyValue.verified(
    this.connection,
    this.apiPrefix,
    this.timeout,
    this.name, {
    required this.allowPerKeyTtl,
    required this.backingStreamName,
    required this.readSubjectPrefix,
    required this.editStreamName,
    required this.editSubjectPrefix,
    required this.editApiPrefix,
    this.codecs = const JetStreamKeyValueCodecs(),
  });

  /// Core connection used for operations.
  final NatsConnection connection;

  /// Domain aware JetStream API prefix.
  final String apiPrefix;

  /// Total deadline for one operation.
  final Duration timeout;

  /// Public bucket name.
  final String name;

  /// Whether create and purge may use the latest per message TTL protocol.
  final bool allowPerKeyTtl;

  /// Stream used for reads, history, and watches.
  final String backingStreamName;

  /// Exact subject prefix retained by the read stream.
  final String readSubjectPrefix;

  /// Source stream mutated by administrative operations.
  final String editStreamName;

  /// Exact subject prefix used by value mutations.
  final String editSubjectPrefix;

  /// JetStream API prefix used by source administrative mutations.
  final String editApiPrefix;

  /// Key and value transformations owned by this handle.
  final JetStreamKeyValueCodecs codecs;

  /// Returns an equivalent handle with transparent key and value codecs.
  ///
  /// Codecs affect future operations only. The backing stream configuration is
  /// unchanged, so every participant must use compatible codecs.
  JetStreamKeyValue withCodecs(JetStreamKeyValueCodecs codecs) {
    return JetStreamKeyValue.verified(
      connection,
      apiPrefix,
      timeout,
      name,
      allowPerKeyTtl: allowPerKeyTtl,
      backingStreamName: backingStreamName,
      readSubjectPrefix: readSubjectPrefix,
      editStreamName: editStreamName,
      editSubjectPrefix: editSubjectPrefix,
      editApiPrefix: editApiPrefix,
      codecs: codecs,
    );
  }

  /// Encodes and validates one logical key for storage.
  String encodeKey(String key) {
    final encoded = codecs.encodeKey(key);
    validateJetStreamKeyValueKey(encoded);
    return encoded;
  }

  /// Encodes and validates one logical filter for a server operation.
  String encodeFilter(String filter) {
    final encoded = codecs.encodeKey(filter);
    validateJetStreamKeyValueFilter(encoded);
    return encoded;
  }

  /// Decodes one stored key into its logical representation.
  String decodeKey(String encodedKey) => codecs.decodeKey(encodedKey);

  /// Stores a new value without a revision precondition.
  Future<BigInt> put(String key, List<int> value) => _publish(
    key,
    value,
    marker: null,
    expectedRevision: null,
    conflictOperation: "put",
  );

  /// Stores the exact UTF 8 encoding of [value].
  Future<BigInt> putString(String key, String value) =>
      put(key, encodeExactUtf8(value));

  /// Creates a key only when no prior live value exists.
  Future<BigInt> create(
    String key,
    List<int> value, {
    JetStreamKeyValueLifetime lifetime =
        const JetStreamKeyValueLifetime.inherit(),
  }) async {
    _validateLifetime(lifetime);
    try {
      return await _publish(
        key,
        value,
        marker: null,
        expectedRevision: BigInt.zero,
        conflictOperation: "create",
        ttl: lifetime.wireValue,
      );
    } on JetStreamKeyValueConflictException {
      final latest = await get(key);
      if (latest == null ||
          latest.operation == JetStreamKeyValueOperation.put) {
        rethrow;
      }
      return _publish(
        key,
        value,
        marker: null,
        expectedRevision: latest.revision,
        conflictOperation: "create",
        ttl: lifetime.wireValue,
      );
    }
  }

  /// Replaces a key only when [expectedRevision] is current.
  Future<BigInt> update(String key, List<int> value, BigInt expectedRevision) {
    validatePositiveJetStreamUint64(expectedRevision, "expectedRevision");
    return _publish(
      key,
      value,
      marker: null,
      expectedRevision: expectedRevision,
      conflictOperation: "update",
    );
  }

  /// Stores a delete marker, optionally guarded by a revision.
  Future<BigInt> delete(String key, {BigInt? expectedRevision}) => _publish(
    key,
    const [],
    marker: "DEL",
    expectedRevision: expectedRevision,
    conflictOperation: "delete",
  );

  /// Replaces key history with one purge marker.
  Future<BigInt> purge(
    String key, {
    BigInt? expectedRevision,
    JetStreamKeyValueLifetime markerLifetime =
        const JetStreamKeyValueLifetime.inherit(),
  }) {
    _validateLifetime(markerLifetime);
    return _publish(
      key,
      const [],
      marker: "PURGE",
      expectedRevision: expectedRevision,
      conflictOperation: "purge",
      purge: true,
      ttl: markerLifetime.wireValue,
    );
  }

  /// Reads the latest stored revision, or `null` when the key never existed.
  Future<JetStreamKeyValueEntry?> get(String key) async {
    final encodedKey = encodeKey(key);
    final subject = _readSubject(encodedKey);
    final apiSubject = "$apiPrefix.STREAM.MSG.GET.$backingStreamName";
    JetStreamWireResponse response;
    try {
      final message = await connection.request(
        apiSubject,
        encodeExactJson({"last_by_subj": subject}),
        timeout: timeout,
      );
      response = JetStreamWireResponse.decode(message.payload, apiSubject)
        ..throwApiError();
    } on JetStreamApiException catch (error) {
      if (error.errorCode == 10037) {
        return null;
      }
      rethrow;
    }
    final stored = response.requiredObject("message");
    if (response.requiredString("message.subject", source: stored) != subject) {
      throw JetStreamProtocolException(
        "KV response contains the wrong key subject",
        subject: apiSubject,
        fieldPath: "message.subject",
      );
    }
    return decodeStoredKeyValueEntry(
      response,
      stored,
      bucket: name,
      encodedKey: encodedKey,
      codecs: codecs,
    );
  }

  /// Reads a value as strict UTF 8, or `null` when the key is absent.
  ///
  /// Malformed UTF 8 throws [FormatException].
  Future<String?> getString(String key) async {
    final entry = await get(key);
    return entry == null
        ? null
        : utf8.decode(entry.value, allowMalformed: false);
  }

  Future<BigInt> _publish(
    String key,
    List<int> value, {
    required String? marker,
    required BigInt? expectedRevision,
    required String conflictOperation,
    bool purge = false,
    String? ttl,
  }) async {
    final encodedKey = encodeKey(key);
    if (expectedRevision != null) {
      validateJetStreamUint64(expectedRevision, "expectedRevision");
    }
    final subject = _editSubject(encodedKey);
    final snapshot = marker == null
        ? codecs.encodeValue(value)
        : Uint8List(0).asUnmodifiableView();
    NatsHeaders? headers;
    if (marker != null) {
      headers = setJetStreamManagedHeader(headers, "KV-Operation", marker);
    }
    if (purge) {
      headers = setJetStreamManagedHeader(headers, "Nats-Rollup", "sub");
    }
    if (ttl != null) {
      headers = setJetStreamManagedHeader(headers, "Nats-TTL", ttl);
    }
    headers = applyJetStreamPublishOptions(
      headers,
      expectedRevision == null
          ? null
          : JetStreamPublishOptions(
              expectedLastSubjectSequence: expectedRevision,
              expectedLastSubjectSequenceSubject: _readSubject(encodedKey),
            ),
    );
    headers = setJetStreamManagedHeader(
      headers,
      "Nats-Expected-Stream",
      editStreamName,
    );
    try {
      final message = await connection.request(
        subject,
        snapshot,
        headers: headers,
        timeout: timeout,
      );
      final response = JetStreamWireResponse.decode(message.payload, subject)
        ..throwApiError();
      if (response.requiredString("stream") != editStreamName) {
        throw JetStreamProtocolException(
          "KV acknowledgement has the wrong stream identity",
          subject: subject,
          fieldPath: "stream",
        );
      }
      return response.requiredPositiveUint64("seq");
    } on JetStreamApiException catch (error) {
      if (expectedRevision != null &&
          (error.errorCode == 10071 || error.errorCode == 10164)) {
        throw JetStreamKeyValueConflictException(
          bucket: name,
          key: key,
          operation: conflictOperation,
          expectedRevision: expectedRevision,
          cause: error,
        );
      }
      rethrow;
    }
  }

  String _readSubject(String key) => "$readSubjectPrefix$key";

  String _editSubject(String key) => "$editSubjectPrefix$key";

  void _validateLifetime(JetStreamKeyValueLifetime lifetime) {
    if (lifetime.wireValue != null && !allowPerKeyTtl) {
      throw StateError("KV bucket does not enable per key TTL");
    }
  }
}
