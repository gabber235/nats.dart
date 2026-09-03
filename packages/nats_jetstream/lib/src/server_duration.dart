import "package:nats_jetstream/src/wire_integer.dart";

/// Returns a positive duration in nanoseconds within Go time.Duration range.
BigInt jetStreamServerDurationNanoseconds(Duration value, String name) {
  final nanoseconds = BigInt.from(value.inMicroseconds) * BigInt.from(1000);
  return validatePositiveJetStreamDuration(nanoseconds, name);
}
