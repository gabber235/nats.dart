import "dart:typed_data";

import "package:nats_core/src/headers.dart";

/// A decoded operation sent by NATS Server.
sealed class ServerOperation {
  const ServerOperation();
}

/// An INFO operation containing forward compatible server fields.
final class ServerInfoOperation extends ServerOperation {
  /// Creates an INFO operation.
  const ServerInfoOperation(this.fields);

  /// Parsed INFO JSON fields.
  final Map<String, Object?> fields;
}

/// A message delivered to a subscription.
final class ServerMessageOperation extends ServerOperation {
  /// Creates a decoded message.
  const ServerMessageOperation({
    required this.subject,
    required this.subscriptionId,
    required this.payload,
    this.replySubject,
    this.headers,
  });

  /// Published subject.
  final String subject;

  /// Local subscription identifier supplied by the client.
  final int subscriptionId;

  /// Optional reply subject.
  final String? replySubject;

  /// Immutable message headers, when present.
  final NatsHeaders? headers;

  /// Message payload bytes.
  final Uint8List payload;
}

/// A server PING that requires a PONG.
final class ServerPingOperation extends ServerOperation {
  /// Creates a PING operation.
  const ServerPingOperation();
}

/// A server PONG satisfying one queued ping waiter.
final class ServerPongOperation extends ServerOperation {
  /// Creates a PONG operation.
  const ServerPongOperation();
}

/// A positive acknowledgment emitted in verbose mode.
final class ServerOkOperation extends ServerOperation {
  /// Creates a positive acknowledgment.
  const ServerOkOperation();
}

/// A server protocol or authorization error.
final class ServerErrorOperation extends ServerOperation {
  /// Creates a server error with its normalized [message].
  const ServerErrorOperation(this.message);

  /// Error text without matching surrounding quotes.
  final String message;
}
