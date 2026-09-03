import "dart:async";
import "dart:convert";
import "dart:math";
import "dart:typed_data";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/atomic_batch.dart";
import "package:nats_jetstream/src/atomic_batch_advisory.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/identifiers.dart";
import "package:nats_jetstream/src/publish_headers.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// Internal lifecycle owner for one atomic batch operation.
final class JetStreamAtomicBatchPublisher {
  /// Creates a publisher over one Core connection.
  const JetStreamAtomicBatchPublisher(this.connection, this.timeout);

  /// Core connection used for every staged request.
  final NatsConnection connection;

  /// Total deadline for staging and commit.
  final Duration timeout;

  /// Stages [messages] and commits them with the API level four EOB path.
  Future<JetStreamAtomicBatchAck> publish(
    Iterable<JetStreamAtomicMessage> messages, {
    String? batchId,
  }) {
    return _publish(_prepare(messages, batchId));
  }

  /// Publishes while observing definitive abandonment for one exact stream.
  Future<JetStreamAtomicBatchAck> publishConfirmed(
    String stream,
    Iterable<JetStreamAtomicMessage> messages, {
    String? batchId,
    required Duration confirmationTimeout,
  }) {
    validateJetStreamStreamName(stream);
    if (confirmationTimeout <= Duration.zero) {
      throw ArgumentError.value(
        confirmationTimeout,
        "confirmationTimeout",
        "Must be positive",
      );
    }
    final prepared = _prepare(messages, batchId);
    return _publishConfirmed(
      stream,
      prepared,
      confirmationTimeout: confirmationTimeout,
    );
  }

  Future<JetStreamAtomicBatchAck> _publishConfirmed(
    String stream,
    _PreparedAtomicBatch prepared, {
    required Duration confirmationTimeout,
  }) async {
    final watcher = await JetStreamAtomicBatchAdvisoryWatcher.connect(
      connection,
      stream: stream,
      timeout: timeout,
    );
    final matching = Completer<JetStreamAtomicBatchAbandonedAdvisory>();
    JetStreamAtomicBatchAbandonedAdvisory? matchingAdvisory;
    Object? observationCause;
    StackTrace? observationCauseStackTrace;
    final listener = watcher.advisories.listen(
      (advisory) {
        if (!matching.isCompleted &&
            advisory.matches(stream: stream, batchId: prepared.batchId)) {
          matchingAdvisory = advisory;
          matching.complete(advisory);
        }
      },
      onError: (Object error, StackTrace stackTrace) {
        observationCause ??= error;
        observationCauseStackTrace ??= stackTrace;
      },
    );
    try {
      return await _publish(prepared, expectedStream: stream);
    } on Object catch (error, stackTrace) {
      if (matchingAdvisory != null) {
        throw JetStreamAtomicBatchAbandonedException(
          advisory: matchingAdvisory!,
          cause: error,
          causeStackTrace: stackTrace,
        );
      }
      if (error is JetStreamApiException ||
          error is JetStreamProtocolException && error.fieldPath == "stream") {
        rethrow;
      }
      late final JetStreamAtomicBatchAbandonedAdvisory advisory;
      try {
        advisory = await matching.future.timeout(confirmationTimeout);
      } on TimeoutException {
        throw JetStreamAtomicBatchAmbiguousException(
          stream: stream,
          batchId: prepared.batchId,
          confirmationTimeout: confirmationTimeout,
          cause: error,
          causeStackTrace: stackTrace,
          observationCause: observationCause,
          observationCauseStackTrace: observationCauseStackTrace,
        );
      }
      throw JetStreamAtomicBatchAbandonedException(
        advisory: advisory,
        cause: error,
        causeStackTrace: stackTrace,
      );
    } finally {
      await listener.cancel();
      await watcher.close();
    }
  }

  _PreparedAtomicBatch _prepare(
    Iterable<JetStreamAtomicMessage> messages,
    String? batchId,
  ) {
    final captured = List<JetStreamAtomicMessage>.unmodifiable(messages);
    if (captured.isEmpty || captured.length > 1000) {
      throw ArgumentError.value(
        captured.length,
        "messages",
        "Must contain between 1 and 1000 messages",
      );
    }
    final effectiveBatchId = batchId ?? _newBatchId();
    _validateBatchId(effectiveBatchId);
    _validateOptions(captured);
    return _PreparedAtomicBatch(captured, effectiveBatchId);
  }

  Future<JetStreamAtomicBatchAck> _publish(
    _PreparedAtomicBatch prepared, {
    String? expectedStream,
  }) async {
    final captured = prepared.messages;
    final effectiveBatchId = prepared.batchId;
    final elapsed = Stopwatch()..start();
    for (var index = 0; index < captured.length; index += 1) {
      final message = captured[index];
      var headers = applyJetStreamPublishOptions(
        message.headers,
        message.options,
      );
      headers = _batchHeaders(headers, effectiveBatchId, index + 1);
      final reply = await connection.request(
        message.subject,
        message.payload,
        headers: headers,
        timeout: _remaining(elapsed),
      );
      if (reply.payload.isNotEmpty) {
        final response = JetStreamWireResponse.decode(
          reply.payload,
          message.subject,
        )..throwApiError();
        throw JetStreamProtocolException(
          "Atomic batch staging returned an unexpected response",
          subject: response.subject,
        );
      }
    }

    var commitHeaders = _batchHeaders(
      null,
      effectiveBatchId,
      captured.length + 1,
    );
    commitHeaders = setJetStreamManagedHeader(
      commitHeaders,
      "Nats-Batch-Commit",
      "eob",
    );
    final finalMessage = captured.last;
    final reply = await connection.request(
      finalMessage.subject,
      const [],
      headers: commitHeaders,
      timeout: _remaining(elapsed),
    );
    final response = JetStreamWireResponse.decode(
      reply.payload,
      finalMessage.subject,
    )..throwApiError();
    final acknowledgedBatchId = response.requiredString("batch");
    if (acknowledgedBatchId != effectiveBatchId) {
      throw JetStreamProtocolException(
        "Atomic batch acknowledgement has the wrong batch identity",
        subject: response.subject,
        fieldPath: "batch",
      );
    }
    final acknowledgedCount = response.requiredUnsigned("count");
    if (acknowledgedCount != BigInt.from(captured.length)) {
      throw JetStreamProtocolException(
        "Atomic batch acknowledgement has the wrong message count",
        subject: response.subject,
        fieldPath: "count",
      );
    }
    final acknowledgedStream = response.requiredString("stream");
    if (expectedStream != null && acknowledgedStream != expectedStream) {
      throw JetStreamProtocolException(
        "Atomic batch acknowledgement has the wrong stream identity",
        subject: response.subject,
        fieldPath: "stream",
      );
    }
    return JetStreamAtomicBatchAck(
      stream: acknowledgedStream,
      sequence: response.requiredPositiveUint64("seq"),
      batchId: acknowledgedBatchId,
      messageCount: acknowledgedCount,
      duplicate: response.optionalBoolean("duplicate"),
      domain: response.optionalString("domain"),
    );
  }

  NatsHeaders _batchHeaders(
    NatsHeaders? headers,
    String batchId,
    int sequence,
  ) {
    var result = setJetStreamManagedHeader(headers, "Nats-Batch-Id", batchId);
    result = setJetStreamManagedHeader(
      result,
      "Nats-Batch-Sequence",
      "$sequence",
    );
    return setJetStreamManagedHeader(result, "Nats-Required-Api-Level", "4");
  }

  Duration _remaining(Stopwatch elapsed) {
    final result = timeout - elapsed.elapsed;
    if (result <= Duration.zero) {
      throw TimeoutException(
        "Atomic batch exceeded its total deadline",
        timeout,
      );
    }
    return result;
  }
}

void _validateOptions(List<JetStreamAtomicMessage> messages) {
  for (var index = 0; index < messages.length; index += 1) {
    final headers = messages[index].headers;
    for (final name in _atomicBatchManagedHeaders) {
      if (headers?.first(name) != null) {
        throw ArgumentError(
          "Header $name is managed by the atomic batch operation",
        );
      }
    }
    final options = messages[index].options;
    if (options?.expectedLastMessageId != null) {
      throw ArgumentError.value(
        options?.expectedLastMessageId,
        "expectedLastMessageId",
        "Atomic batches do not support this expectation",
      );
    }
    if (index != 0 && options?.expectedLastSequence != null) {
      throw ArgumentError.value(
        options?.expectedLastSequence,
        "expectedLastSequence",
        "Only the first atomic message may set this expectation",
      );
    }
  }
}

const _atomicBatchManagedHeaders = [
  "Nats-Batch-Commit",
  "Nats-Batch-Id",
  "Nats-Batch-Sequence",
  "Nats-Required-Api-Level",
];

void _validateBatchId(String batchId) {
  final byteLength = utf8.encode(batchId).length;
  if (byteLength < 1 || byteLength > 64) {
    throw ArgumentError.value(
      batchId,
      "batchId",
      "Must contain between 1 and 64 UTF 8 bytes",
    );
  }
  NatsHeaders(entries: [MapEntry("Validation", batchId)]);
}

final class _PreparedAtomicBatch {
  const _PreparedAtomicBatch(this.messages, this.batchId);

  final List<JetStreamAtomicMessage> messages;
  final String batchId;
}

String _newBatchId() {
  final random = Random.secure();
  final bytes = Uint8List.fromList(
    List<int>.generate(18, (_) => random.nextInt(256)),
  );
  return base64UrlEncode(bytes).replaceAll("=", "");
}
