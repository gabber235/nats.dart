import "dart:async";
import "dart:convert";
import "dart:math";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/consumer_config.dart";
import "package:nats_jetstream/src/consumer_delivery.dart";
import "package:nats_jetstream/src/consumer_duration.dart";
import "package:nats_jetstream/src/consumer_runtime.dart";
import "package:nats_jetstream/src/consumer_validation.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/wire_integer.dart";

const _finitePullControlAllowanceBytes = 1024;
const _maximumSafeInteger = 9007199254740991;
const _maximumFinitePullBufferedBytes =
    _maximumSafeInteger - _finitePullControlAllowanceBytes;

/// Policy specific fields for one priority pull request.
final class JetStreamFetchPriority {
  /// Creates an overflow request with optional positive eligibility thresholds.
  factory JetStreamFetchPriority.overflow({
    required String group,
    BigInt? minimumPending,
    BigInt? minimumAcknowledgementsPending,
  }) {
    validateConsumerPriorityGroup(group);
    _validateThreshold(minimumPending, "minimumPending");
    _validateThreshold(
      minimumAcknowledgementsPending,
      "minimumAcknowledgementsPending",
    );
    return JetStreamFetchPriority._(
      group,
      minimumPending,
      minimumAcknowledgementsPending,
      null,
      null,
    );
  }

  /// Creates a pinned client request, optionally renewing an existing pin.
  factory JetStreamFetchPriority.pinned({
    required String group,
    String? pinId,
  }) {
    validateConsumerPriorityGroup(group);
    if (pinId != null) {
      if (pinId.isEmpty ||
          pinId.codeUnits.any((unit) => unit <= 32 || unit == 127)) {
        throw ArgumentError.value(pinId, "pinId", "Invalid pinned client ID");
      }
      validateConsumerUtf8Field(pinId, "pinId", 255);
    }
    return JetStreamFetchPriority._(group, null, null, pinId, null);
  }

  /// Creates a prioritized request where zero is the highest priority.
  factory JetStreamFetchPriority.prioritized({
    required String group,
    required int priority,
  }) {
    validateConsumerPriorityGroup(group);
    if (priority < 0 || priority > 9) {
      throw ArgumentError.value(priority, "priority", "Must be from 0 to 9");
    }
    return JetStreamFetchPriority._(group, null, null, null, priority);
  }

  const JetStreamFetchPriority._(
    this.group,
    this.minimumPending,
    this.minimumAcknowledgementsPending,
    this.pinId,
    this.priority,
  );

  /// Configured consumer priority group.
  final String group;

  /// Minimum available messages for overflow eligibility.
  final BigInt? minimumPending;

  /// Minimum unacknowledged messages for overflow eligibility.
  final BigInt? minimumAcknowledgementsPending;

  /// Existing pinned client identifier to renew.
  final String? pinId;

  /// Prioritized selection value from zero through nine.
  final int? priority;
}

/// Bounded finite pull request.
final class JetStreamFetchRequest {
  /// Creates a validated request.
  factory JetStreamFetchRequest({
    required int maximumMessages,
    int? maximumBytes,
    int maximumBufferedBytes = 8 * 1024 * 1024,
    Duration expires = const Duration(seconds: 5),
    Duration? idleHeartbeat,
    bool noWait = false,
    JetStreamFetchPriority? priority,
  }) {
    if (maximumMessages < 1 || maximumMessages > 1000000) {
      throw ArgumentError.value(
        maximumMessages,
        "maximumMessages",
        "Must be between 1 and 1000000",
      );
    }
    if (maximumBytes != null && maximumBytes < 1) {
      throw ArgumentError.value(
        maximumBytes,
        "maximumBytes",
        "Must be positive",
      );
    }
    if (maximumBufferedBytes < 1) {
      throw ArgumentError.value(
        maximumBufferedBytes,
        "maximumBufferedBytes",
        "Must be positive",
      );
    }
    if (maximumBufferedBytes > _maximumFinitePullBufferedBytes) {
      throw ArgumentError.value(
        maximumBufferedBytes,
        "maximumBufferedBytes",
        "Must not exceed $_maximumFinitePullBufferedBytes",
      );
    }
    if (expires <= Duration.zero) {
      throw ArgumentError.value(expires, "expires", "Must be positive");
    }
    consumerDurationNanoseconds(expires, "expires");
    if (idleHeartbeat != null &&
        (idleHeartbeat <= Duration.zero ||
            idleHeartbeat.inMicroseconds > expires.inMicroseconds ~/ 2)) {
      throw ArgumentError.value(
        idleHeartbeat,
        "idleHeartbeat",
        "Must be positive and no more than half the expiry",
      );
    }
    if (idleHeartbeat != null) {
      consumerDurationNanoseconds(idleHeartbeat, "idleHeartbeat");
    }
    if (noWait && idleHeartbeat != null) {
      throw ArgumentError("Immediate pulls cannot request heartbeats");
    }
    return JetStreamFetchRequest._(
      maximumMessages,
      maximumBytes,
      maximumBufferedBytes,
      expires,
      idleHeartbeat,
      noWait,
      priority,
    );
  }

  const JetStreamFetchRequest._(
    this.maximumMessages,
    this.maximumBytes,
    this.maximumBufferedBytes,
    this.expires,
    this.idleHeartbeat,
    this.noWait,
    this.priority,
  );

  /// Maximum user messages returned.
  final int maximumMessages;

  /// Maximum server response bytes, when bounded.
  final int? maximumBytes;

  /// Maximum total delivery bytes retained in the returned result.
  final int maximumBufferedBytes;

  /// Server pull request expiry.
  final Duration expires;

  /// Optional server heartbeat interval.
  final Duration? idleHeartbeat;

  /// Whether the server should return immediately when empty.
  final bool noWait;

  /// Optional policy specific priority request fields.
  final JetStreamFetchPriority? priority;
}

/// Stable pull consumer handle with no cached server state.
final class JetStreamPullConsumer {
  /// Creates a handle after management type discrimination.
  const JetStreamPullConsumer(
    this.connection,
    this.apiPrefix,
    this.stream,
    this.name,
  );

  /// Core connection used for delivery and acknowledgements.
  final NatsConnection connection;

  /// Domain aware JetStream API prefix.
  final String apiPrefix;

  /// Owning stream name.
  final String stream;

  /// Stable consumer name.
  final String name;

  /// Executes one finite generation scoped pull.
  Future<JetStreamFetchResult> fetch(JetStreamFetchRequest request) async {
    final generation = connection.generation;
    final inbox = "_INBOX.${_randomToken(22)}";
    final received = <JetStreamDelivery>[];
    var receivedBytes = 0;
    var requestPublished = false;
    String? pinId;
    NatsSubscription? subscription;
    StreamIterator<NatsMessage>? iterator;
    try {
      final ackPolicy = await readPullConsumerAckPolicy(
        connection,
        apiPrefix,
        stream,
        name,
        request.expires,
      );
      if (!connection.isConnected || connection.generation != generation) {
        throw const NatsConnectionException(
          "Connection generation changed while validating the pull consumer",
        );
      }
      subscription = await connection.subscribe(
        inbox,
        options: NatsSubscriptionOptions(
          maximumMessages: request.maximumMessages + 16,
          maximumBytes:
              request.maximumBufferedBytes + _finitePullControlAllowanceBytes,
          reconnect: NatsSubscriptionReconnectPolicy.close,
        ),
      );
      iterator = StreamIterator(subscription.messages);
      await connection.flush(timeout: request.expires);
      if (!connection.isConnected || connection.generation != generation) {
        throw const NatsConnectionException(
          "Connection generation changed before the pull request",
        );
      }
      final requestClock = Stopwatch()..start();
      await connection.publishOnGeneration(
        "$apiPrefix.CONSUMER.MSG.NEXT.$stream.$name",
        encodeExactJson(_encodeFetchRequest(request)),
        generation: generation,
        replySubject: inbox,
      );
      requestPublished = true;

      while (true) {
        final overallLimit = request.noWait
            ? const Duration(seconds: 5)
            : request.expires + const Duration(seconds: 1);
        final overallRemaining = overallLimit - requestClock.elapsed;
        if (overallRemaining <= Duration.zero) {
          throw NatsTimeoutException("JetStream finite pull");
        }
        final heartbeatLimit = request.idleHeartbeat == null
            ? null
            : request.idleHeartbeat! * 2;
        final wait = heartbeatLimit != null && heartbeatLimit < overallRemaining
            ? heartbeatLimit
            : overallRemaining;
        late final bool hasMessage;
        try {
          hasMessage = await iterator.moveNext().timeout(wait);
        } on TimeoutException {
          if (heartbeatLimit != null && heartbeatLimit <= overallRemaining) {
            throw JetStreamConsumerHeartbeatException(request.idleHeartbeat!);
          }
          throw NatsTimeoutException("JetStream finite pull");
        }
        if (!hasMessage) {
          throw const NatsConnectionException("Pull delivery inbox closed");
        }
        final message = iterator.current;
        final status = message.headers?.statusCode;
        if (status == null) {
          final messagePinId = message.headers?.first("Nats-Pin-Id");
          if (messagePinId != null) {
            if (pinId != null && pinId != messagePinId) {
              throw JetStreamProtocolException(
                "Pinned client ID changed within one pull request",
                subject: "$apiPrefix.CONSUMER.MSG.NEXT.$stream.$name",
                fieldPath: "Nats-Pin-Id",
              );
            }
            pinId = messagePinId;
          }
          final delivery = requireConsumerDeliveryIdentity(
            ackPolicy == JetStreamConsumerAckPolicy.none
                ? JetStreamDelivery.withoutAcknowledgement(message)
                : JetStreamDelivery.fromMessage(
                    connection,
                    generation,
                    message,
                  ),
            expectedStream: stream,
            expectedConsumer: name,
            operation: "Finite pull",
          );
          final deliveryBytes = _retainedDeliveryBytes(message);
          if (receivedBytes + deliveryBytes > request.maximumBufferedBytes) {
            throw NatsSlowConsumerException(
              "Finite pull exceeded ${request.maximumBufferedBytes} retained "
              "bytes",
            );
          }
          received.add(delivery);
          receivedBytes += deliveryBytes;
          if (received.length == request.maximumMessages) {
            return JetStreamFetchResult(
              messages: received,
              completion: JetStreamFetchCompletion.batchComplete,
              pinId: pinId,
            );
          }
          continue;
        }
        if (status == 100) {
          final reply = message.replySubject;
          if (reply != null) {
            await connection.publishOnGeneration(
              reply,
              const [],
              generation: generation,
            );
          }
          continue;
        }
        final completion = _normalCompletion(message.headers!);
        if (completion != null) {
          return JetStreamFetchResult(
            messages: received,
            completion: completion,
            pinId: pinId,
          );
        }
        throw JetStreamConsumerStatusException(
          code: status,
          description: message.headers?.statusDescription ?? "",
          headers: message.headers!,
        );
      }
    } on JetStreamFetchException {
      rethrow;
    } on Object catch (error, stackTrace) {
      final failure = JetStreamFetchException(
        message: "Finite pull did not complete",
        partialMessages: received,
        cause: error,
        ambiguous: requestPublished,
      );
      Error.throwWithStackTrace(failure, stackTrace);
    } finally {
      await iterator?.cancel();
      await subscription?.unsubscribe();
    }
  }
}

Map<String, Object?> _encodeFetchRequest(JetStreamFetchRequest request) => {
  "batch": request.maximumMessages,
  "max_bytes": request.maximumBytes == null
      ? request.maximumBufferedBytes
      : min(request.maximumBytes!, request.maximumBufferedBytes),
  if (!request.noWait)
    "expires": consumerDurationNanoseconds(request.expires, "expires"),
  if (request.noWait) "no_wait": true,
  "idle_heartbeat": ?_nanoseconds(request.idleHeartbeat),
  if (request.priority case final priority?) ...{
    "group": priority.group,
    "min_pending": ?priority.minimumPending,
    "min_ack_pending": ?priority.minimumAcknowledgementsPending,
    "id": ?priority.pinId,
    "priority": ?priority.priority,
  },
};

int _retainedDeliveryBytes(NatsMessage message) {
  return utf8.encode(message.subject).length +
      utf8.encode(message.replySubject ?? "").length +
      message.payload.length +
      (message.headers?.encode().length ?? 0);
}

void _validateThreshold(BigInt? value, String name) {
  if (value != null) {
    validatePositiveJetStreamDuration(value, name);
  }
}

BigInt? _nanoseconds(Duration? value) =>
    value == null ? null : consumerDurationNanoseconds(value, "idleHeartbeat");

JetStreamFetchCompletion? _normalCompletion(NatsHeaders headers) {
  final description = (headers.statusDescription ?? "").toLowerCase();
  return switch (headers.statusCode) {
    404 => JetStreamFetchCompletion.noMessages,
    408 => JetStreamFetchCompletion.expired,
    409 when description.contains("batch completed") =>
      JetStreamFetchCompletion.batchComplete,
    409 when description.contains("message size exceeds") =>
      JetStreamFetchCompletion.byteLimit,
    _ => null,
  };
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
