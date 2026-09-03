import "package:nats_jetstream/src/timestamp.dart";
import "package:test/test.dart";

void main() {
  for (final value in [
    "0001-01-01T00:00:00Z",
    "2000-02-29T23:59:59Z",
    "2026-09-03T12:34:56+23:59",
    "2026-09-03T12:34:56-00:00",
    "2026-09-03T12:34:56.123456789Z",
  ]) {
    test("accepts and preserves $value", () {
      expect(validateJetStreamRfc3339(value, "timestamp"), same(value));
    });
  }

  test("serializes only four digit DateTime years", () {
    expect(
      serializeJetStreamRfc3339(DateTime.utc(1), "time"),
      "0001-01-01T00:00:00.000Z",
    );
    expect(
      serializeJetStreamRfc3339(DateTime.utc(9999), "time"),
      "9999-01-01T00:00:00.000Z",
    );
    expect(
      () => serializeJetStreamRfc3339(DateTime.utc(-1), "time"),
      throwsArgumentError,
    );
    expect(
      () => serializeJetStreamRfc3339(DateTime.utc(10000), "time"),
      throwsArgumentError,
    );
  });

  for (final value in [
    "",
    "2026-09-42T00:00:00Z",
    "2025-02-29T00:00:00Z",
    "0000-01-01T00:00:00Z",
    "2026-09-03T24:00:00Z",
    "2026-09-03T00:60:00Z",
    "2026-09-03T00:00:60Z",
    "2026-09-03T00:00:00+24:00",
    "2026-09-03T00:00:00+00:60",
    "2026-09-03T00:00:00.Z",
    "2026-09-03T00:00:00.1234567890Z",
  ]) {
    test("rejects $value", () {
      expect(
        () => validateJetStreamRfc3339(value, "timestamp"),
        throwsArgumentError,
      );
    });
  }
}
