import "package:nats_jetstream/src/server_duration.dart";

/// Encodes a positive consumer duration within the server int64 schema.
BigInt consumerDurationNanoseconds(Duration value, String name) {
  return jetStreamServerDurationNanoseconds(value, name);
}
