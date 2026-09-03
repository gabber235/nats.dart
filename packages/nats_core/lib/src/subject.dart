import "dart:convert";

import "package:nats_core/src/error.dart";

/// Conservative client default matching the normal NATS control line limit.
const defaultMaximumNatsControlLineBytes = 4096;

/// Validates a publish or request [subject].
void validatePublishSubject(
  String subject, {
  int maximumBytes = defaultMaximumNatsControlLineBytes,
}) {
  final tokens = _validateCommon(subject, maximumBytes);
  if (tokens.any((token) => token == "*" || token == ">")) {
    throw const NatsSubjectException(
      "Publish subjects must not contain wildcards",
    );
  }
}

/// Validates a subscription [subject].
void validateSubscriptionSubject(
  String subject, {
  int maximumBytes = defaultMaximumNatsControlLineBytes,
}) {
  final tokens = _validateCommon(subject, maximumBytes);
  for (var index = 0; index < tokens.length; index += 1) {
    final token = tokens[index];
    if (token.contains("*") && token != "*") {
      throw const NatsSubjectException("Wildcard * must occupy a full token");
    }
    if (token.contains(">") && (token != ">" || index != tokens.length - 1)) {
      throw const NatsSubjectException(
        "Wildcard > must occupy the final full token",
      );
    }
  }
}

/// Validates a queue group [queue].
void validateQueueGroup(
  String queue, {
  int maximumBytes = defaultMaximumNatsControlLineBytes,
}) {
  if (queue.isEmpty) {
    throw const NatsSubjectException("Queue group must not be empty");
  }
  for (final unit in queue.codeUnits) {
    if (unit <= 32 || unit == 127 || unit == 42 || unit == 62) {
      throw const NatsSubjectException("Queue group contains a forbidden byte");
    }
  }
  _validateWireEncoding(queue, maximumBytes, "Queue group");
}

List<String> _validateCommon(String subject, int maximumBytes) {
  if (subject.isEmpty) {
    throw const NatsSubjectException("Subject must not be empty");
  }
  for (final unit in subject.codeUnits) {
    if (unit <= 32 || unit == 127) {
      throw const NatsSubjectException(
        "Subject contains whitespace or control",
      );
    }
  }
  final tokens = subject.split(".");
  if (tokens.any((token) => token.isEmpty)) {
    throw const NatsSubjectException("Subject contains an empty token");
  }
  _validateWireEncoding(subject, maximumBytes, "Subject");
  return tokens;
}

void _validateWireEncoding(String value, int maximumBytes, String label) {
  if (maximumBytes < 1 || value.length > maximumBytes) {
    throw NatsSubjectException("$label exceeds $maximumBytes UTF8 bytes");
  }
  final encoded = utf8.encode(value);
  if (encoded.length > maximumBytes) {
    throw NatsSubjectException("$label exceeds $maximumBytes UTF8 bytes");
  }
  if (utf8.decode(encoded) != value) {
    throw NatsSubjectException("$label is not valid Unicode");
  }
}
