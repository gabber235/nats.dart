import "package:nats_jetstream/src/continuous_watch_cursor.dart";
import "package:test/test.dart";

void main() {
  test("cursor rejects replay and advances only after commit", () {
    final cursor = JetStreamContinuousWatchCursor();

    expect(
      cursor.inspect(
        streamSequence: BigInt.from(7),
        consumerSequence: BigInt.one,
      ),
      JetStreamContinuousWatchCursorDecision.accept,
    );
    expect(cursor.lastRevision, isNull);
    cursor.commit(BigInt.from(7));
    expect(cursor.lastRevision, BigInt.from(7));

    expect(
      cursor.inspect(
        streamSequence: BigInt.from(7),
        consumerSequence: BigInt.two,
      ),
      JetStreamContinuousWatchCursorDecision.duplicate,
    );
    expect(
      cursor.inspect(
        streamSequence: BigInt.from(6),
        consumerSequence: BigInt.from(3),
      ),
      JetStreamContinuousWatchCursorDecision.duplicate,
    );
  });

  test("cursor reports consumer gaps within one generation", () {
    final cursor = JetStreamContinuousWatchCursor();
    expect(
      cursor.inspect(streamSequence: BigInt.one, consumerSequence: BigInt.one),
      JetStreamContinuousWatchCursorDecision.accept,
    );
    cursor.commit(BigInt.one);

    expect(
      cursor.inspect(
        streamSequence: BigInt.two,
        consumerSequence: BigInt.from(3),
      ),
      JetStreamContinuousWatchCursorDecision.gap,
    );
  });

  test("new generation resets consumer sequence but preserves revision", () {
    final cursor = JetStreamContinuousWatchCursor();
    expect(
      cursor.inspect(
        streamSequence: BigInt.from(10),
        consumerSequence: BigInt.from(4),
      ),
      JetStreamContinuousWatchCursorDecision.accept,
    );
    cursor
      ..commit(BigInt.from(10))
      ..beginGeneration();
    expect(
      cursor.inspect(
        streamSequence: BigInt.from(10),
        consumerSequence: BigInt.one,
      ),
      JetStreamContinuousWatchCursorDecision.duplicate,
    );
    expect(
      cursor.inspect(
        streamSequence: BigInt.from(11),
        consumerSequence: BigInt.two,
      ),
      JetStreamContinuousWatchCursorDecision.accept,
    );
    cursor.commit(BigInt.from(11));
    expect(cursor.lastRevision, BigInt.from(11));
  });
}
