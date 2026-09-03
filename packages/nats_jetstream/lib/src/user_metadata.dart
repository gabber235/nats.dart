import "package:nats_jetstream/src/exact_utf8.dart";

/// Captures user metadata after enforcing shared server constraints.
Map<String, String> captureJetStreamUserMetadata(Map<String, String> metadata) {
  final captured = Map<String, String>.unmodifiable(metadata);
  if (captured.keys.any((key) => key.startsWith("_nats."))) {
    throw ArgumentError.value(
      metadata,
      "metadata",
      "Keys starting with _nats. are reserved by the server",
    );
  }
  final bytes = captured.entries.fold<int>(
    0,
    (total, entry) =>
        total +
        encodeExactUtf8(entry.key, name: "metadata").length +
        encodeExactUtf8(entry.value, name: "metadata").length,
  );
  if (bytes > 128 * 1024) {
    throw ArgumentError.value(metadata, "metadata", "Exceeds 128 KiB");
  }
  return captured;
}
