import "dart:convert";

import "package:nats_core/nats_core.dart";

/// Validates a JetStream stream [name] before it enters an API subject.
void validateJetStreamStreamName(String name) {
  if (name.isEmpty) {
    throw ArgumentError.value(name, "name", "Must not be empty");
  }
  if (utf8.encode(name).length > 255) {
    throw ArgumentError.value(name, "name", "Must not exceed 255 UTF8 bytes");
  }
  for (final unit in name.codeUnits) {
    if (unit <= 32 ||
        unit == 127 ||
        unit == 42 ||
        unit == 46 ||
        unit == 47 ||
        unit == 62 ||
        unit == 92) {
      throw ArgumentError.value(
        name,
        "name",
        "Contains a character forbidden in a stream name",
      );
    }
  }
}

/// Validates a management [prefix] used to construct API subjects.
void validateJetStreamApiPrefix(String prefix) {
  validatePublishSubject("$prefix.INFO");
}
