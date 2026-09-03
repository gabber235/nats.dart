import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  test("asynchronous publisher separates attempt and total deadlines", () {
    final derived = JetStreamAsyncPublisherOptions(
      acknowledgementTimeout: const Duration(seconds: 8),
      maximumAttempts: 4,
    );
    final explicit = JetStreamAsyncPublisherOptions(
      acknowledgementTimeout: const Duration(seconds: 8),
      attemptTimeout: const Duration(seconds: 1),
      maximumAttempts: 4,
    );

    expect(derived.attemptTimeout, const Duration(seconds: 2));
    expect(explicit.attemptTimeout, const Duration(seconds: 1));
    expect(
      () => JetStreamAsyncPublisherOptions(
        acknowledgementTimeout: const Duration(seconds: 1),
        attemptTimeout: const Duration(seconds: 2),
      ),
      throwsArgumentError,
    );
  });
}
