import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/ordered_consumer_cursor.dart";
import "package:nats_jetstream/src/ordered_consumer_recovery.dart";
import "package:test/test.dart";

void main() {
  test("AckNone delivery preserves metadata without an acknowledger", () {
    final delivery = JetStreamDelivery.withoutAcknowledgement(
      NatsMessage(
        subject: "events",
        payload: const [1],
        replySubject: r"$JS.ACK.STREAM.ORDERED.1.2.1.3.0",
      ),
    );

    expect(delivery.metadata?.stream, "STREAM");
    expect(delivery.metadata?.consumer, "ORDERED");
    expect(delivery.metadata?.streamSequence, BigInt.two);
    expect(delivery.metadata?.consumerSequence, BigInt.one);
    expect(delivery.acknowledger, isNull);
  });

  test("ordered options validate recovery bounds and capture metadata", () {
    final metadata = {"owner": "reader"};
    final options = JetStreamOrderedConsumerOptions(
      inactiveThreshold: const Duration(seconds: 2),
      recoveryTimeout: const Duration(seconds: 3),
      headersOnly: true,
      metadata: metadata,
    );

    metadata["owner"] = "changed";
    expect(options.metadata, {"owner": "reader"});
    expect(options.headersOnly, isTrue);
    expect(
      () => JetStreamOrderedConsumerOptions(
        inactiveThreshold: const Duration(milliseconds: 999),
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamOrderedConsumerOptions(recoveryTimeout: Duration.zero),
      throwsArgumentError,
    );
  });

  test("cursor accepts forward stream holes but reports them", () {
    final cursor = JetStreamOrderedConsumerCursor()..beginConsumer(BigInt.one);

    final first = cursor.inspect(
      consumer: "ORDERED",
      consumerSequence: BigInt.one,
      streamSequence: BigInt.one,
    );
    final afterDeletedSequence = cursor.inspect(
      consumer: "ORDERED",
      consumerSequence: BigInt.two,
      streamSequence: BigInt.from(3),
    );

    expect(first.accepted, isTrue);
    expect(first.gap, isNull);
    expect(afterDeletedSequence.accepted, isTrue);
    expect(
      afterDeletedSequence.gap?.kind,
      JetStreamOrderedConsumerGapKind.streamForward,
    );
    expect(afterDeletedSequence.gap?.expected, BigInt.two);
    expect(afterDeletedSequence.gap?.received, BigInt.from(3));
    expect(afterDeletedSequence.gap?.causesRecreation, isFalse);
    expect(cursor.lastStreamSequence, BigInt.from(3));
  });

  test("cursor rejects consumer gaps without advancing stream position", () {
    final cursor = JetStreamOrderedConsumerCursor()
      ..beginConsumer(BigInt.one)
      ..inspect(
        consumer: "ORDERED",
        consumerSequence: BigInt.one,
        streamSequence: BigInt.one,
      );

    final gap = cursor.inspect(
      consumer: "ORDERED",
      consumerSequence: BigInt.from(3),
      streamSequence: BigInt.from(3),
    );

    expect(gap.accepted, isFalse);
    expect(gap.gap?.kind, JetStreamOrderedConsumerGapKind.consumer);
    expect(
      gap.recreationReason,
      JetStreamOrderedConsumerRecreationReason.consumerSequenceGap,
    );
    expect(cursor.lastStreamSequence, BigInt.one);
  });

  test("cursor rejects repeated stream sequence after recreation", () {
    final cursor = JetStreamOrderedConsumerCursor()
      ..beginConsumer(BigInt.one)
      ..inspect(
        consumer: "FIRST",
        consumerSequence: BigInt.one,
        streamSequence: BigInt.one,
      )
      ..beginConsumer(BigInt.two);

    final regression = cursor.inspect(
      consumer: "SECOND",
      consumerSequence: BigInt.one,
      streamSequence: BigInt.one,
    );

    expect(regression.accepted, isFalse);
    expect(
      regression.gap?.kind,
      JetStreamOrderedConsumerGapKind.streamRegression,
    );
    expect(
      regression.recreationReason,
      JetStreamOrderedConsumerRecreationReason.streamSequenceRegression,
    );
  });

  test("recovery classification recognizes exact server control states", () {
    final headers = NatsHeaders();

    expect(
      orderedConsumerRecoveryReason(
        JetStreamConsumerStatusException(
          code: 409,
          description: "consumer deleted",
          headers: headers,
        ),
      ),
      JetStreamOrderedConsumerRecreationReason.consumerDeleted,
    );
    expect(
      orderedConsumerRecoveryReason(
        JetStreamConsumerStatusException(
          code: 409,
          description: "Consumer Leadership Change",
          headers: headers,
        ),
      ),
      JetStreamOrderedConsumerRecreationReason.leadershipChanged,
    );
    expect(
      orderedConsumerRecoveryReason(
        JetStreamConsumerStatusException(
          code: 409,
          description: "batch completed",
          headers: headers,
        ),
      ),
      isNull,
    );
    expect(
      orderedConsumerRecoveryReason(
        const JetStreamConsumerHeartbeatException(Duration(seconds: 1)),
      ),
      JetStreamOrderedConsumerRecreationReason.heartbeatMissed,
    );
    expect(
      orderedConsumerRecoveryReason(
        JetStreamConsumerStatusException(
          code: 503,
          description: "",
          headers: headers,
        ),
      ),
      JetStreamOrderedConsumerRecreationReason.noResponders,
    );
    expect(
      orderedConsumerRecoveryReason(
        JetStreamConsumerStatusException(
          code: 503,
          description: "temporarily unavailable",
          headers: headers,
        ),
      ),
      JetStreamOrderedConsumerRecreationReason.noResponders,
    );
    for (final status in [502, 504]) {
      expect(
        orderedConsumerRecoveryReason(
          JetStreamConsumerStatusException(
            code: status,
            description: "gateway failure",
            headers: headers,
          ),
        ),
        isNull,
      );
    }
  });
}
