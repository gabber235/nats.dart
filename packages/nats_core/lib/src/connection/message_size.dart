import "dart:convert";

import "package:nats_core/src/message.dart";

/// Bytes retained when one message waits for application delivery.
int retainedMessageBytes(NatsMessage message) {
  return message.payload.length +
      utf8.encode(message.subject).length +
      utf8.encode(message.replySubject ?? "").length +
      (message.headers?.encode().length ?? 0);
}
