import "dart:math";

import "package:nats_jetstream/src/consumer_config.dart";
import "package:nats_jetstream/src/consumer_delivery.dart";
import "package:nats_jetstream/src/consumer_fetch.dart";
import "package:nats_jetstream/src/consumer_management.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/kv.dart";
import "package:nats_jetstream/src/kv_bucket.dart";
import "package:nats_jetstream/src/kv_codec.dart";
import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/wire_integer.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// Finite history and key snapshots for one KV bucket.
extension JetStreamKeyValueSnapshots on JetStreamKeyValue {
  /// Reads retained revisions for one key in ascending stream order.
  Future<List<JetStreamKeyValueEntry>> history(
    String key, {
    int maximumEntries = 100000,
    int maximumBytes = 64 * 1024 * 1024,
  }) {
    encodeKey(key);
    return historyMatching(
      [key],
      maximumEntries: maximumEntries,
      maximumBytes: maximumBytes,
    );
  }

  /// Reads retained revisions matching any relative key filter.
  Future<List<JetStreamKeyValueEntry>> historyMatching(
    Iterable<String> filters, {
    int maximumEntries = 100000,
    int maximumBytes = 64 * 1024 * 1024,
  }) async {
    final capturedFilters = _captureFilters(this, filters);
    final deliveries = await _snapshot(
      start: const JetStreamConsumerStart.all(),
      filters: capturedFilters,
      maximumEntries: maximumEntries,
      maximumBytes: maximumBytes,
      headersOnly: false,
    );
    return List.unmodifiable(
      deliveries.map(
        (delivery) => decodeKeyValueDelivery(
          delivery,
          bucket: name,
          subjectPrefix: readSubjectPrefix,
          codecs: codecs,
        ),
      ),
    );
  }

  /// Reads one exact stream revision when it belongs to [key].
  Future<JetStreamKeyValueEntry?> getRevision(
    String key,
    BigInt revision,
  ) async {
    final encodedKey = encodeKey(key);
    validatePositiveJetStreamUint64(revision, "revision");
    final apiSubject = "$apiPrefix.STREAM.MSG.GET.$backingStreamName";
    JetStreamWireResponse response;
    try {
      final message = await connection.request(
        apiSubject,
        encodeExactJson({"seq": revision}),
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
    final storedRevision = response.requiredPositiveUint64(
      "message.seq",
      source: stored,
    );
    if (storedRevision != revision) {
      throw JetStreamProtocolException(
        "KV response revision does not match the requested revision",
        subject: response.subject,
        fieldPath: "message.seq",
      );
    }
    if (response.requiredString("message.subject", source: stored) !=
        _subject(encodedKey)) {
      return null;
    }
    return decodeStoredKeyValueEntry(
      response,
      stored,
      bucket: name,
      encodedKey: encodedKey,
      codecs: codecs,
    );
  }

  /// Returns the sorted current live key set.
  Future<List<String>> keys({
    Iterable<String> filters = const [">"],
    int maximumKeys = 100000,
    int maximumBytes = 16 * 1024 * 1024,
  }) async {
    final capturedFilters = _captureFilters(this, filters);
    final deliveries = await _snapshot(
      start: const JetStreamConsumerStart.lastPerSubject(),
      filters: capturedFilters,
      maximumEntries: maximumKeys,
      maximumBytes: maximumBytes,
      headersOnly: true,
    );
    final keys = <String>{};
    for (final delivery in deliveries) {
      final entry = decodeKeyValueDelivery(
        delivery,
        bucket: name,
        subjectPrefix: readSubjectPrefix,
        codecs: codecs,
        decodeValue: false,
      );
      if (entry.operation == JetStreamKeyValueOperation.put) {
        keys.add(entry.key);
      }
    }
    final result = keys.toList()..sort();
    return List.unmodifiable(result);
  }

  Future<List<JetStreamDelivery>> _snapshot({
    required JetStreamConsumerStart start,
    required List<String> filters,
    required int maximumEntries,
    required int maximumBytes,
    required bool headersOnly,
  }) async {
    if (maximumEntries < 1 || maximumEntries > 1000000) {
      throw ArgumentError.value(
        maximumEntries,
        "maximumEntries",
        "Must be between 1 and 1000000",
      );
    }
    if (maximumBytes < 1) {
      throw ArgumentError.value(
        maximumBytes,
        "maximumBytes",
        "Must be positive",
      );
    }
    final stream = backingStreamName;
    final consumer = "K${_randomToken(21)}";
    final manager = JetStreamConsumerManager(connection, apiPrefix, timeout);
    final info = await manager.createPull(
      stream,
      JetStreamPullConsumerConfig(
        name: consumer,
        durable: false,
        start: start,
        filters: _consumerFilters(readSubjectPrefix, filters),
        ackPolicy: JetStreamConsumerAckPolicy.none,
        maximumDeliveries: 1,
        maximumBatch: maximumEntries,
        inactiveThreshold: JetStreamNanoseconds.fromDuration(
          const Duration(seconds: 30),
        ),
        headersOnly: headersOnly,
      ),
    );
    try {
      if (info.pending == BigInt.zero) {
        return const [];
      }
      if (info.pending > BigInt.from(maximumEntries)) {
        throw StateError("KV snapshot exceeds the configured entry bound");
      }
      final count = info.pending.toInt();
      final result =
          await JetStreamPullConsumer(
            connection,
            apiPrefix,
            stream,
            consumer,
          ).fetch(
            JetStreamFetchRequest(
              maximumMessages: count,
              maximumBytes: maximumBytes,
              expires: timeout,
              idleHeartbeat: timeout >= const Duration(seconds: 2)
                  ? const Duration(seconds: 1)
                  : null,
            ),
          );
      if (result.messages.length != count ||
          result.completion != JetStreamFetchCompletion.batchComplete) {
        throw StateError("KV snapshot exceeded its byte bound");
      }
      return result.messages;
    } finally {
      await manager.delete(stream, consumer);
    }
  }

  String _subject(String key) => "$readSubjectPrefix$key";
}

List<String> _captureFilters(
  JetStreamKeyValue bucket,
  Iterable<String> filters,
) {
  final captured = List<String>.unmodifiable(filters);
  if (captured.isEmpty) {
    throw ArgumentError.value(filters, "filters", "Must not be empty");
  }
  if (captured.toSet().length != captured.length) {
    throw ArgumentError.value(filters, "filters", "Must be unique");
  }
  return List.unmodifiable(captured.map(bucket.encodeFilter));
}

JetStreamConsumerFilters _consumerFilters(String prefix, List<String> filters) {
  final subjects = filters
      .map((filter) => "$prefix$filter")
      .toList(growable: false);
  return subjects.length == 1
      ? JetStreamConsumerFilters.single(subjects.single)
      : JetStreamConsumerFilters.multiple(subjects);
}

String _randomToken(int length) {
  const alphabet =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  final random = Random.secure();
  return List.generate(
    length,
    (_) => alphabet[random.nextInt(alphabet.length)],
    growable: false,
  ).join();
}
