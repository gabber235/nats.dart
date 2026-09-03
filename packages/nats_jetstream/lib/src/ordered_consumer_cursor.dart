// ignore_for_file: public_member_api_docs

import "package:nats_jetstream/src/ordered_consumer_models.dart";

final class JetStreamOrderedConsumerCursor {
  BigInt? _lastStreamSequence;
  var _expectedConsumerSequence = BigInt.one;
  BigInt? _expectedStartSequence;

  BigInt? get lastStreamSequence => _lastStreamSequence;

  void beginConsumer(BigInt? expectedStartSequence) {
    _expectedConsumerSequence = BigInt.one;
    _expectedStartSequence = expectedStartSequence;
  }

  JetStreamOrderedCursorInspection inspect({
    required String consumer,
    required BigInt consumerSequence,
    required BigInt streamSequence,
  }) {
    if (consumerSequence != _expectedConsumerSequence) {
      return JetStreamOrderedCursorInspection.recreate(
        JetStreamOrderedConsumerGap(
          kind: JetStreamOrderedConsumerGapKind.consumer,
          consumer: consumer,
          expected: _expectedConsumerSequence,
          received: consumerSequence,
          previousStreamSequence: _lastStreamSequence,
          receivedStreamSequence: streamSequence,
          causesRecreation: true,
        ),
        JetStreamOrderedConsumerRecreationReason.consumerSequenceGap,
      );
    }
    final previous = _lastStreamSequence;
    final expectedStream = previous == null
        ? _expectedStartSequence
        : previous + BigInt.one;
    if (previous != null && streamSequence <= previous) {
      return JetStreamOrderedCursorInspection.recreate(
        JetStreamOrderedConsumerGap(
          kind: JetStreamOrderedConsumerGapKind.streamRegression,
          consumer: consumer,
          expected: previous + BigInt.one,
          received: streamSequence,
          previousStreamSequence: previous,
          receivedStreamSequence: streamSequence,
          causesRecreation: true,
        ),
        JetStreamOrderedConsumerRecreationReason.streamSequenceRegression,
      );
    }
    final forwardGap = expectedStream != null && streamSequence > expectedStream
        ? JetStreamOrderedConsumerGap(
            kind: JetStreamOrderedConsumerGapKind.streamForward,
            consumer: consumer,
            expected: expectedStream,
            received: streamSequence,
            previousStreamSequence: previous,
            receivedStreamSequence: streamSequence,
            causesRecreation: false,
          )
        : null;
    _expectedConsumerSequence += BigInt.one;
    _lastStreamSequence = streamSequence;
    return JetStreamOrderedCursorInspection.accept(forwardGap);
  }
}

final class JetStreamOrderedCursorInspection {
  const JetStreamOrderedCursorInspection.accept(this.gap)
    : recreationReason = null;

  const JetStreamOrderedCursorInspection.recreate(
    this.gap,
    this.recreationReason,
  );

  final JetStreamOrderedConsumerGap? gap;
  final JetStreamOrderedConsumerRecreationReason? recreationReason;

  bool get accepted => recreationReason == null;
}
