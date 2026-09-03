import "dart:convert";

import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/timestamp.dart";
import "package:nats_jetstream/src/user_metadata.dart";
import "package:nats_jetstream/src/wire_integer.dart";

/// Validates a consumer name before using it in an API subject.
void validateJetStreamConsumerName(String name) {
  if (name.isEmpty || name.length > 255) {
    throw ArgumentError.value(name, "name", "Must contain 1 through 255 bytes");
  }
  for (final unit in name.codeUnits) {
    if (unit <= 32 ||
        unit == 127 ||
        unit == 42 ||
        unit == 46 ||
        unit == 47 ||
        unit == 62 ||
        unit == 92) {
      throw ArgumentError.value(name, "name", "Contains a forbidden unit");
    }
  }
  validateConsumerUtf8Field(name, "name", 255);
}

/// Validates policy fields shared by pull and ordinary push consumers.
void validateConsumerCommon(
  String? description,
  bool flowControlAckPolicy,
  JetStreamNanoseconds? ackWait,
  int? maximumDeliveries,
  List<JetStreamNanoseconds> backoff,
  int? maximumAckPending,
) {
  if (description != null) {
    validateConsumerUtf8Field(description, "description", 4096);
  }
  if (flowControlAckPolicy) {
    throw ArgumentError.value(
      flowControlAckPolicy,
      "ackPolicy",
      "Use a reliable source consumer",
    );
  }
  if (ackWait != null && ackWait.nanoseconds <= BigInt.zero) {
    throw ArgumentError.value(ackWait, "ackWait", "Must be positive");
  }
  if (ackWait != null) {
    validatePositiveJetStreamDuration(ackWait.nanoseconds, "ackWait");
  }
  if (maximumDeliveries != null && maximumDeliveries != -1) {
    validatePositiveConsumerOption(maximumDeliveries, "maximumDeliveries");
  }
  if (backoff.any((value) => value.nanoseconds <= BigInt.zero)) {
    throw ArgumentError.value(backoff, "backoff", "Must be positive");
  }
  for (final value in backoff) {
    validatePositiveJetStreamDuration(value.nanoseconds, "backoff");
  }
  validatePositiveOrMinusOneConsumerOption(
    maximumAckPending,
    "maximumAckPending",
  );
}

/// Validates an exact Unicode field against one server UTF8 byte ceiling.
void validateConsumerUtf8Field(String value, String name, int maximumBytes) {
  if (value.length > maximumBytes) {
    throw ArgumentError.value(value, name, "Exceeds $maximumBytes UTF8 bytes");
  }
  final encoded = utf8.encode(value);
  if (encoded.length > maximumBytes || utf8.decode(encoded) != value) {
    throw ArgumentError.value(
      value,
      name,
      "Must be valid Unicode within $maximumBytes UTF8 bytes",
    );
  }
}

/// Requires a positive value when [value] is present.
void validatePositiveConsumerOption(int? value, String name) {
  if (value != null && value < 1) {
    throw ArgumentError.value(value, name, "Must be positive");
  }
}

/// Requires a positive value or minus one when [value] is present.
void validatePositiveOrMinusOneConsumerOption(int? value, String name) {
  if (value != null && value != -1 && value < 1) {
    throw ArgumentError.value(value, name, "Must be positive or minus one");
  }
}

/// Captures metadata after enforcing the server byte limit.
Map<String, String> captureConsumerMetadata(Map<String, String> metadata) {
  return captureJetStreamUserMetadata(metadata);
}

/// Validates fields shared by server normalized consumer storage.
void validateConsumerStorage(int replicas) {
  if (replicas < 0 || replicas > 5) {
    throw ArgumentError.value(replicas, "replicas", "Must be between 0 and 5");
  }
}

/// Requires an exact UTC pause deadline.
void validateConsumerPause(DateTime? pauseUntil) {
  if (pauseUntil == null) {
    return;
  }
  if (!pauseUntil.isUtc) {
    throw ArgumentError.value(pauseUntil, "pauseUntil", "Must be UTC");
  }
  serializeJetStreamRfc3339(pauseUntil, "pauseUntil");
}

/// Validates an optional acknowledgement sampling percentage.
void validateConsumerSampling(int? samplePercentage) {
  if (samplePercentage != null &&
      (samplePercentage < 0 || samplePercentage > 100)) {
    throw ArgumentError.value(
      samplePercentage,
      "samplePercentage",
      "Must be between 0 and 100",
    );
  }
}

/// Validates one consumer priority group using the server grammar.
void validateConsumerPriorityGroup(String group) {
  if (!RegExp(r"^[a-zA-Z0-9/_=-]{1,16}$").hasMatch(group)) {
    throw ArgumentError.value(group, "group", "Invalid priority group name");
  }
}
