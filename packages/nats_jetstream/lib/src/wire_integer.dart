/// Largest integer accepted by an unsigned 64 bit NATS wire field.
final BigInt maximumJetStreamUint64 = (BigInt.one << 64) - BigInt.one;

/// Largest integer accepted by a signed 64 bit NATS wire field.
final BigInt maximumJetStreamInt64 = (BigInt.one << 63) - BigInt.one;

/// Smallest integer accepted by a signed 64 bit NATS wire field.
final BigInt minimumJetStreamInt64 = -(BigInt.one << 63);

/// Largest integer accepted by a signed 32 bit NATS wire field.
final BigInt maximumJetStreamInt32 = (BigInt.one << 31) - BigInt.one;

/// Smallest integer accepted by a signed 32 bit NATS wire field.
final BigInt minimumJetStreamInt32 = -(BigInt.one << 31);

/// Requires one exact signed 64 bit wire integer.
BigInt validateJetStreamInt64(BigInt value, String name) {
  if (value < minimumJetStreamInt64 || value > maximumJetStreamInt64) {
    throw ArgumentError.value(value, name, "Must fit signed 64 bit integer");
  }
  return value;
}

/// Requires one exact unsigned 64 bit wire integer.
BigInt validateJetStreamUint64(BigInt value, String name) {
  if (value.isNegative || value > maximumJetStreamUint64) {
    throw ArgumentError.value(value, name, "Must fit unsigned 64 bit integer");
  }
  return value;
}

/// Requires one positive unsigned 64 bit wire integer.
BigInt validatePositiveJetStreamUint64(BigInt value, String name) {
  validateJetStreamUint64(value, name);
  if (value == BigInt.zero) {
    throw ArgumentError.value(value, name, "Must be positive");
  }
  return value;
}

/// Requires one signed 64 bit limit using minus one as unlimited.
BigInt validateJetStreamLimit64(BigInt value, String name) {
  validateJetStreamInt64(value, name);
  if (value < -BigInt.one) {
    throw ArgumentError.value(value, name, "Must be minus one or nonnegative");
  }
  return value;
}

/// Requires one positive signed 64 bit duration in nanoseconds.
BigInt validatePositiveJetStreamDuration(BigInt value, String name) {
  validateJetStreamInt64(value, name);
  if (value <= BigInt.zero) {
    throw ArgumentError.value(value, name, "Must be positive");
  }
  return value;
}

/// Requires one nonnegative signed 64 bit duration in nanoseconds.
BigInt validateJetStreamDuration(BigInt value, String name) {
  validateJetStreamInt64(value, name);
  if (value.isNegative) {
    throw ArgumentError.value(value, name, "Must not be negative");
  }
  return value;
}

/// Requires one signed 32 bit message size using minus one as unlimited.
BigInt validateJetStreamMessageSize(BigInt value, String name) {
  if (value < -BigInt.one || value > maximumJetStreamInt32) {
    throw ArgumentError.value(
      value,
      name,
      "Must be minus one or fit nonnegative signed 32 bit integer",
    );
  }
  return value;
}
