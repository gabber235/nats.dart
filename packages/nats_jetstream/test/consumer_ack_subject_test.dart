import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  test("parses exact v1 acknowledgement metadata", () {
    final metadata = JetStreamConsumerAckSubject.parse(
      r"$JS.ACK.ORDERS.WORKER.2.18446744073709551615.7."
      "1234567890123456789.42",
    );

    expect(metadata.version, 1);
    expect(metadata.domain, isNull);
    expect(metadata.stream, "ORDERS");
    expect(metadata.consumer, "WORKER");
    expect(metadata.delivered, BigInt.two);
    expect(metadata.streamSequence, BigInt.parse("18446744073709551615"));
    expect(metadata.pending, BigInt.from(42));
  });

  test("parses v2 domain, account, and opaque extensions", () {
    final metadata = JetStreamConsumerAckSubject.parse(
      r"$JS.ACK._.A1B2.ORDERS.WORKER.1.2.3.4.5.random.more",
    );

    expect(metadata.version, 2);
    expect(metadata.domain, isNull);
    expect(metadata.accountHash, "A1B2");
    expect(metadata.extensions, ["random", "more"]);
  });

  test("rejects mixed layouts and malformed numeric tokens", () {
    for (final subject in [
      r"$JS.ACK.ORDERS.WORKER.1.2.3.4",
      r"$JS.ACK._.HASH.ORDERS.WORKER.1.2.3.4",
      r"$JS.ACK.ORDERS.WORKER.one.2.3.4.5",
      List.filled(4097, "X").join(),
    ]) {
      expect(
        () => JetStreamConsumerAckSubject.parse(subject),
        throwsA(isA<JetStreamProtocolException>()),
      );
    }
  });

  test("enforces acknowledgement field wire ranges", () {
    for (final subject in [
      r"$JS.ACK.EVENTS.WORKER.0.2.3.4.5",
      r"$JS.ACK.EVENTS.WORKER.1.0.3.4.5",
      r"$JS.ACK.EVENTS.WORKER.1.2.0.4.5",
      r"$JS.ACK.EVENTS.WORKER.1.2.3.0.5",
      r"$JS.ACK.EVENTS.WORKER.18446744073709551616.2.3.4.5",
      r"$JS.ACK.EVENTS.WORKER.1.18446744073709551616.3.4.5",
      r"$JS.ACK.EVENTS.WORKER.1.2.18446744073709551616.4.5",
      r"$JS.ACK.EVENTS.WORKER.1.2.3.9223372036854775808.5",
      r"$JS.ACK.EVENTS.WORKER.1.2.3.4.18446744073709551616",
    ]) {
      expect(
        () => JetStreamConsumerAckSubject.parse(subject),
        throwsA(isA<JetStreamProtocolException>()),
      );
    }

    final maximum = JetStreamConsumerAckSubject.parse(
      r"$JS.ACK.EVENTS.WORKER.18446744073709551615."
      "18446744073709551615.18446744073709551615."
      "9223372036854775807.18446744073709551615",
    );
    expect(maximum.delivered, BigInt.parse("18446744073709551615"));
    expect(maximum.timestampNanoseconds, BigInt.parse("9223372036854775807"));
    expect(maximum.pending, BigInt.parse("18446744073709551615"));
  });

  test("rejects invalid acknowledgement metadata before delivery exposure", () {
    expect(
      () => JetStreamDelivery.withoutAcknowledgement(
        NatsMessage(
          subject: "inbox",
          payload: const [],
          replySubject: r"$JS.ACK.EVENTS.WORKER.1.0.3.4.5",
        ),
      ),
      throwsA(isA<JetStreamProtocolException>()),
    );
  });

  test("delivery identity requires exact stream and consumer metadata", () {
    JetStreamDelivery delivery(String? reply) =>
        JetStreamDelivery.withoutAcknowledgement(
          NatsMessage(subject: "inbox", payload: const [], replySubject: reply),
        );

    for (final scenario in [
      (reply: null, field: "replySubject"),
      (reply: r"$JS.ACK.OTHER.WORKER.1.2.3.4.5", field: "stream"),
      (reply: r"$JS.ACK.EVENTS.OTHER.1.2.3.4.5", field: "consumer"),
    ]) {
      expect(
        () => requireConsumerDeliveryIdentity(
          delivery(scenario.reply),
          expectedStream: "EVENTS",
          expectedConsumer: "WORKER",
          operation: "Test",
        ),
        throwsA(
          isA<JetStreamProtocolException>().having(
            (error) => error.fieldPath,
            "fieldPath",
            scenario.field,
          ),
        ),
      );
    }

    final valid = delivery(r"$JS.ACK.EVENTS.WORKER.1.2.3.4.5");
    expect(
      requireConsumerDeliveryIdentity(
        valid,
        expectedStream: "EVENTS",
        expectedConsumer: "WORKER",
        operation: "Test",
      ),
      same(valid),
    );
  });
}
