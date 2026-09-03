import "dart:async";
import "dart:math";
import "dart:typed_data";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/direct_get_models.dart";
import "package:nats_jetstream/src/direct_get_wire.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/identifiers.dart";
import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/wire_integer.dart";

/// Generation scoped direct reads from stream replicas.
final class JetStreamDirectReader {
  /// Creates a direct reader.
  const JetStreamDirectReader(this.connection, this.apiPrefix, this.timeout);

  /// Core connection used for requests.
  final NatsConnection connection;

  /// Domain aware JetStream API prefix.
  final String apiPrefix;

  /// Total deadline for one direct read.
  final Duration timeout;

  /// Reads one exact sequence from any eligible stream replica.
  Future<JetStreamStoredMessage> get(String stream, BigInt sequence) {
    validatePositiveJetStreamUint64(sequence, "sequence");
    return _single(stream, {"seq": sequence}, expectedSequence: sequence);
  }

  /// Reads one exact sequence and returns only its payload.
  Future<Uint8List> getPayload(String stream, BigInt sequence) {
    validatePositiveJetStreamUint64(sequence, "sequence");
    return _singlePayload(stream, {
      "seq": sequence,
    }, expectedSequence: sequence);
  }

  /// Reads the newest stored message matching one subject pattern.
  Future<JetStreamStoredMessage> getLast(String stream, String subject) {
    validateSubscriptionSubject(subject);
    return _single(
      stream,
      {"last_by_subj": subject},
      expectedSubjectPatterns: [subject],
    );
  }

  /// Reads the newest matching message and returns only its payload.
  Future<Uint8List> getLastPayload(String stream, String subject) {
    validateSubscriptionSubject(subject);
    return _singlePayload(
      stream,
      {"last_by_subj": subject},
      expectedSubjectPatterns: [subject],
    );
  }

  /// Reads the newest message using the permission friendly subject endpoint.
  Future<JetStreamStoredMessage> getLastBySubject(
    String stream,
    String subject,
  ) {
    validateJetStreamStreamName(stream);
    validatePublishSubject(subject);
    return _requestSingle(
      "$apiPrefix.DIRECT.GET.$stream.$subject",
      const {},
      expectedSubjectPatterns: [subject],
    );
  }

  /// Reads the newest message through the subject endpoint and returns payload.
  Future<Uint8List> getLastPayloadBySubject(String stream, String subject) {
    validateJetStreamStreamName(stream);
    validatePublishSubject(subject);
    return _requestPayload(
      "$apiPrefix.DIRECT.GET.$stream.$subject",
      const {},
      expectedSubjectPatterns: [subject],
    );
  }

  /// Reads the first matching message at or after one sequence.
  Future<JetStreamStoredMessage> getNext(
    String stream,
    String subject, {
    BigInt? startSequence,
  }) {
    validateSubscriptionSubject(subject);
    if (startSequence != null) {
      validatePositiveJetStreamUint64(startSequence, "startSequence");
    }
    return _single(
      stream,
      {"next_by_subj": subject, "seq": ?startSequence},
      minimumSequence: startSequence,
      expectedSubjectPatterns: [subject],
    );
  }

  /// Reads the first matching message and returns only its payload.
  Future<Uint8List> getNextPayload(
    String stream,
    String subject, {
    BigInt? startSequence,
  }) {
    validateSubscriptionSubject(subject);
    if (startSequence != null) {
      validatePositiveJetStreamUint64(startSequence, "startSequence");
    }
    return _singlePayload(
      stream,
      {"next_by_subj": subject, "seq": ?startSequence},
      minimumSequence: startSequence,
      expectedSubjectPatterns: [subject],
    );
  }

  /// Reads one bounded page and requires the server end marker.
  Future<JetStreamDirectBatchResult> batch(
    String stream,
    JetStreamDirectBatchRequest request,
  ) async {
    validateJetStreamStreamName(stream);
    if (request.nextBySubject case final subject?) {
      validateSubscriptionSubject(subject);
    }
    for (final subject in request.multipleLastSubjects ?? const <String>[]) {
      validateSubscriptionSubject(subject);
    }
    final subject = "$apiPrefix.DIRECT.GET.$stream";
    final generation = connection.generation;
    final inbox = "_INBOX.${_randomToken(22)}";
    NatsSubscription? subscription;
    StreamIterator<NatsMessage>? iterator;
    final messages = <JetStreamStoredMessage>[];
    final expectedSubjectPatterns = request.nextBySubject == null
        ? request.multipleLastSubjects ?? const <String>[]
        : [request.nextBySubject!];
    var previousSequence = BigInt.zero;
    var retainedBytes = 0;
    final clock = Stopwatch()..start();
    try {
      subscription = await connection.subscribe(
        inbox,
        options: NatsSubscriptionOptions(
          maximumMessages: request.maximumMessages + 1,
          maximumBytes: request.maximumBufferedBytes,
          reconnect: NatsSubscriptionReconnectPolicy.close,
        ),
      );
      iterator = StreamIterator(subscription.messages);
      await connection.flush(timeout: timeout);
      if (!connection.isConnected || connection.generation != generation) {
        throw const NatsConnectionException(
          "Connection generation changed before the direct batch request",
        );
      }
      await connection.publishOnGeneration(
        subject,
        encodeExactJson(request.toJson()),
        generation: generation,
        replySubject: inbox,
      );
      while (true) {
        final remaining = timeout - clock.elapsed;
        if (remaining <= Duration.zero) {
          throw NatsTimeoutException("JetStream direct batch");
        }
        late final bool hasMessage;
        try {
          hasMessage = await iterator.moveNext().timeout(remaining);
        } on TimeoutException {
          throw NatsTimeoutException("JetStream direct batch");
        } on NatsSlowConsumerException {
          throw const JetStreamDirectBatchLimitException(
            "subscription queue exceeded maximumBufferedBytes",
          );
        }
        if (!hasMessage) {
          throw const NatsConnectionException(
            "Direct batch reply inbox closed",
          );
        }
        final message = iterator.current;
        retainedBytes += directMessageRetainedBytes(message);
        if (retainedBytes > request.maximumBufferedBytes) {
          throw const JetStreamDirectBatchLimitException(
            "response bytes exceeded maximumBufferedBytes",
          );
        }
        if (message.headers?.statusCode case final status?) {
          if (status != 204) {
            decodeDirectStoredMessage(message, subject);
          }
          final end = decodeDirectBatchEnd(message, subject);
          return JetStreamDirectBatchResult(
            messages: List.unmodifiable(messages),
            pending: end.pending,
            lastSequence: end.lastSequence,
            upToSequence: end.upToSequence,
          );
        }
        if (messages.length >= request.maximumMessages) {
          throw const JetStreamDirectBatchLimitException(
            "response count exceeded maximumMessages before the end marker",
          );
        }
        final decoded = decodeDirectBatchStoredMessage(
          message,
          subject,
          previousSequence,
          minimumSequence: request.startSequence,
          expectedSubjectPatterns: expectedSubjectPatterns,
        );
        previousSequence = decoded.sequence;
        messages.add(decoded);
      }
    } on JetStreamDirectBatchException {
      rethrow;
    } on Object catch (error, stackTrace) {
      Error.throwWithStackTrace(
        JetStreamDirectBatchException(partialMessages: messages, cause: error),
        stackTrace,
      );
    } finally {
      await iterator?.cancel();
      await subscription?.unsubscribe();
    }
  }

  Future<JetStreamStoredMessage> _single(
    String stream,
    Map<String, Object?> selector, {
    BigInt? expectedSequence,
    BigInt? minimumSequence,
    List<String> expectedSubjectPatterns = const [],
  }) {
    validateJetStreamStreamName(stream);
    return _requestSingle(
      "$apiPrefix.DIRECT.GET.$stream",
      selector,
      expectedSequence: expectedSequence,
      minimumSequence: minimumSequence,
      expectedSubjectPatterns: expectedSubjectPatterns,
    );
  }

  Future<Uint8List> _singlePayload(
    String stream,
    Map<String, Object?> selector, {
    BigInt? expectedSequence,
    BigInt? minimumSequence,
    List<String> expectedSubjectPatterns = const [],
  }) {
    validateJetStreamStreamName(stream);
    return _requestPayload(
      "$apiPrefix.DIRECT.GET.$stream",
      selector,
      expectedSequence: expectedSequence,
      minimumSequence: minimumSequence,
      expectedSubjectPatterns: expectedSubjectPatterns,
    );
  }

  Future<JetStreamStoredMessage> _requestSingle(
    String subject,
    Map<String, Object?> selector, {
    BigInt? expectedSequence,
    BigInt? minimumSequence,
    List<String> expectedSubjectPatterns = const [],
  }) async {
    final message = await connection.request(
      subject,
      encodeExactJson(selector),
      timeout: timeout,
    );
    return decodeDirectStoredMessage(
      message,
      subject,
      expectedSequence: expectedSequence,
      minimumSequence: minimumSequence,
      expectedSubjectPatterns: expectedSubjectPatterns,
    );
  }

  Future<Uint8List> _requestPayload(
    String subject,
    Map<String, Object?> selector, {
    BigInt? expectedSequence,
    BigInt? minimumSequence,
    List<String> expectedSubjectPatterns = const [],
  }) async {
    final message = await connection.request(
      subject,
      encodeExactJson(selector),
      timeout: timeout,
    );
    return Uint8List.fromList(
      decodeDirectPayload(
        message,
        subject,
        expectedSequence: expectedSequence,
        minimumSequence: minimumSequence,
        expectedSubjectPatterns: expectedSubjectPatterns,
      ),
    ).asUnmodifiableView();
  }
}

String _randomToken(int length) {
  const alphabet =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  final random = Random.secure();
  return String.fromCharCodes(
    List.generate(
      length,
      (_) => alphabet.codeUnitAt(random.nextInt(alphabet.length)),
    ),
  );
}
