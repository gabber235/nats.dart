import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  test("publish options preserve exact expectation values", () {
    final sequence = BigInt.parse("18446744073709551615");
    final options = JetStreamPublishOptions(
      messageId: "message-1",
      expectedStream: "ORDERS",
      expectedLastSequence: sequence,
      expectedLastSubjectSequence: sequence,
      expectedLastSubjectSequenceSubject: "orders.customer.*",
      expectedLastMessageId: "message-0",
      lifetime: JetStreamMessageLifetime.expiresAfter(
        const Duration(seconds: 12),
      ),
    );

    expect(options.expectedLastSequence, sequence);
    expect(options.expectedLastSubjectSequence, sequence);
    expect(options.expectedLastSubjectSequenceSubject, "orders.customer.*");
    expect(options.lifetime.wireValue, "12s");
  });

  test("publish options reject invalid values", () {
    expect(
      () => JetStreamPublishOptions(expectedLastSequence: BigInt.from(-1)),
      throwsArgumentError,
    );
    expect(
      () => JetStreamPublishOptions(
        expectedLastSequence: BigInt.parse("18446744073709551616"),
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamPublishOptions(
        expectedLastSubjectSequence: BigInt.parse("18446744073709551616"),
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamPublishOptions(
        expectedLastSubjectSequenceSubject: "orders.1",
      ),
      throwsArgumentError,
    );
    expect(() => JetStreamPublishOptions(messageId: ""), throwsArgumentError);
    expect(
      () => JetStreamPublishOptions(messageId: "line\nbreak"),
      throwsA(anything),
    );
    expect(
      () => JetStreamPublishOptions(expectedStream: "ORDERS.BAD"),
      throwsArgumentError,
    );
    for (final subject in ["orders..one", "orders.>.one", "orders.cust*"]) {
      expect(
        () => JetStreamPublishOptions(
          expectedLastSubjectSequence: BigInt.zero,
          expectedLastSubjectSequenceSubject: subject,
        ),
        throwsA(isA<NatsSubjectException>()),
      );
    }
    for (final duration in [
      Duration.zero,
      const Duration(seconds: -1),
      const Duration(milliseconds: 500),
      const Duration(milliseconds: 1500),
    ]) {
      expect(
        () => JetStreamMessageLifetime.expiresAfter(duration),
        throwsArgumentError,
      );
    }
  });

  test("message lifetime reuses KV lifetime semantics", () {
    expect(const JetStreamMessageLifetime.inherit().wireValue, isNull);
    expect(const JetStreamMessageLifetime.never().wireValue, "never");
    expect(const JetStreamKeyValueLifetime.never().wireValue, "never");
    expect(
      JetStreamKeyValueLifetime.expiresAfter(
        const Duration(seconds: 2),
      ).wireValue,
      "2s",
    );
  });

  test("message lifetime fits the server signed nanosecond range", () {
    const maximum = Duration(seconds: 9223372036);
    const overflow = Duration(seconds: 9223372037);
    expect(
      JetStreamMessageLifetime.expiresAfter(maximum).wireValue,
      "9223372036s",
    );
    expect(
      () => JetStreamMessageLifetime.expiresAfter(overflow),
      throwsArgumentError,
    );
  });
}
