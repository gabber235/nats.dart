import "dart:convert";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/consumer_ack_subject.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/wire_integer.dart";

/// Normal completion reason for one finite pull request.
enum JetStreamFetchCompletion {
  /// Requested message count arrived.
  batchComplete,

  /// Requested byte budget was exhausted.
  byteLimit,

  /// No message was immediately available.
  noMessages,

  /// The server side pull request expired.
  expired,
}

/// Immutable delivery with exact acknowledgement metadata.
final class JetStreamDelivery {
  /// Creates a typed delivery from one Core message.
  factory JetStreamDelivery.fromMessage(
    NatsConnection connection,
    int generation,
    NatsMessage message,
  ) {
    final reply = message.replySubject;
    final metadata = reply == null
        ? null
        : JetStreamConsumerAckSubject.parse(reply);
    return JetStreamDelivery._(
      message,
      metadata,
      reply == null
          ? null
          : JetStreamAcknowledger._(connection, generation, reply, metadata!),
    );
  }

  /// Creates an inspectable AckNone delivery without acknowledgement methods.
  ///
  /// NATS retains the reply subject for sequence metadata even when the
  /// consumer rejects acknowledgement publications.
  factory JetStreamDelivery.withoutAcknowledgement(NatsMessage message) {
    final reply = message.replySubject;
    return JetStreamDelivery._(
      message,
      reply == null ? null : JetStreamConsumerAckSubject.parse(reply),
      null,
    );
  }

  const JetStreamDelivery._(this.message, this.metadata, this.acknowledger);

  /// Immutable Core message contents.
  final NatsMessage message;

  /// Parsed v1 or v2 acknowledgement metadata.
  final JetStreamConsumerAckSubject? metadata;

  /// Acknowledgement operations, absent for `AckNone` delivery.
  final JetStreamAcknowledger? acknowledger;
}

/// Requires one delivery to belong to its requested stream and consumer.
JetStreamDelivery requireConsumerDeliveryIdentity(
  JetStreamDelivery delivery, {
  required String expectedStream,
  required String expectedConsumer,
  required String operation,
}) {
  final metadata = delivery.metadata;
  if (metadata == null) {
    throw JetStreamProtocolException(
      "$operation delivery is missing acknowledgement metadata",
      subject: delivery.message.replySubject,
      fieldPath: "replySubject",
    );
  }
  if (metadata.stream != expectedStream) {
    throw JetStreamProtocolException(
      "$operation delivery has the wrong stream identity",
      subject: delivery.message.replySubject,
      fieldPath: "stream",
    );
  }
  if (metadata.consumer != expectedConsumer) {
    throw JetStreamProtocolException(
      "$operation delivery has the wrong consumer identity",
      subject: delivery.message.replySubject,
      fieldPath: "consumer",
    );
  }
  return delivery;
}

/// Complete bounded result of one finite pull.
final class JetStreamFetchResult {
  /// Creates an immutable result.
  JetStreamFetchResult({
    required List<JetStreamDelivery> messages,
    required this.completion,
    this.pinId,
  }) : messages = List.unmodifiable(messages);

  /// Delivered messages in server order.
  final List<JetStreamDelivery> messages;

  /// Normal terminal reason.
  final JetStreamFetchCompletion completion;

  /// Pinned client identifier assigned or confirmed by the server.
  final String? pinId;
}

/// A finite pull missed two requested server heartbeat intervals.
final class JetStreamConsumerHeartbeatException implements Exception {
  /// Creates a typed heartbeat failure.
  const JetStreamConsumerHeartbeatException(this.interval);

  /// Heartbeat interval requested from the server.
  final Duration interval;

  @override
  String toString() =>
      "JetStreamConsumerHeartbeatException: missed two $interval intervals";
}

/// A finite pull failed after possibly delivering a partial prefix.
final class JetStreamFetchException implements Exception {
  /// Creates an inspectable partial failure.
  JetStreamFetchException({
    required this.message,
    required List<JetStreamDelivery> partialMessages,
    required this.cause,
    required this.ambiguous,
  }) : partialMessages = List.unmodifiable(partialMessages);

  /// Safe diagnostic.
  final String message;

  /// Messages delivered before failure.
  final List<JetStreamDelivery> partialMessages;

  /// Typed underlying failure.
  final Object cause;

  /// Whether the server may still have delivered more messages.
  final bool ambiguous;

  @override
  String toString() => "JetStreamFetchException: $message";
}

/// An unknown pull status returned by the server.
final class JetStreamConsumerStatusException implements Exception {
  /// Creates an inspectable status failure.
  const JetStreamConsumerStatusException({
    required this.code,
    required this.description,
    required this.headers,
  });

  /// Three digit status code.
  final int code;

  /// Bounded server status description.
  final String description;

  /// Ordered response headers.
  final NatsHeaders headers;

  @override
  String toString() => "JetStreamConsumerStatusException($code): $description";
}

/// Serialized acknowledgements for one delivered message.
final class JetStreamAcknowledger {
  JetStreamAcknowledger._(
    this._connection,
    this._generation,
    this._subject,
    this.metadata,
  );

  final NatsConnection _connection;
  final int _generation;
  final String _subject;
  Future<void>? _terminal;
  String? _terminalPayload;
  var _terminalConfirmed = false;

  /// Exact delivery metadata.
  final JetStreamConsumerAckSubject metadata;

  /// Positively acknowledges this delivery.
  Future<void> ack() => _terminalOperation("+ACK");

  /// Positively acknowledges and waits for server confirmation.
  Future<void> ackConfirmed({Duration timeout = const Duration(seconds: 5)}) {
    final current = _terminal;
    if (current != null) {
      if (_terminalPayload != "+ACK") {
        throw StateError("Delivery already has another terminal action");
      }
      if (!_terminalConfirmed) {
        throw StateError(
          "An unconfirmed acknowledgement cannot become confirmed",
        );
      }
      return current;
    }
    final operation = _connection
        .requestOnGeneration(
          _subject,
          utf8.encode("+ACK"),
          generation: _generation,
          timeout: timeout,
        )
        .then<void>((_) {});
    _terminal = operation;
    _terminalPayload = "+ACK";
    _terminalConfirmed = true;
    return operation;
  }

  /// Requests immediate redelivery.
  Future<void> nak() => _terminalOperation("-NAK");

  /// Requests redelivery after an exact delay.
  Future<void> nakAfter(JetStreamNanoseconds delay) {
    validatePositiveJetStreamDuration(delay.nanoseconds, "delay");
    return _terminalOperation('-NAK {"delay":${delay.nanoseconds}}');
  }

  /// Extends the acknowledgement deadline without completing delivery.
  Future<void> inProgress() {
    if (_terminal != null) {
      throw StateError("Delivery already has a terminal acknowledgement");
    }
    return _publish("+WPI");
  }

  /// Terminates redelivery.
  Future<void> term() => _terminalOperation("+TERM");

  /// Terminates redelivery with one safe diagnostic reason.
  Future<void> termWithReason(String reason) {
    if (reason.isEmpty || reason.length > 1024 || reason.contains("\r")) {
      throw ArgumentError.value(reason, "reason", "Invalid term reason");
    }
    return _terminalOperation("+TERM $reason");
  }

  Future<void> _terminalOperation(String payload) {
    final current = _terminal;
    if (current != null) {
      if (_terminalPayload != payload) {
        throw StateError("Delivery already has another terminal action");
      }
      return current;
    }
    final operation = _publish(payload);
    _terminal = operation;
    _terminalPayload = payload;
    return operation;
  }

  Future<void> _publish(String payload) => _connection.publishOnGeneration(
    _subject,
    utf8.encode(payload),
    generation: _generation,
  );
}
