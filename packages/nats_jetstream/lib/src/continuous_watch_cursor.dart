// ignore_for_file: public_member_api_docs

enum JetStreamContinuousWatchCursorDecision { accept, duplicate, gap }

/// Pure generation and stream cursor for one continuous watch.
final class JetStreamContinuousWatchCursor {
  BigInt? _lastRevision;
  BigInt? _consumerSequence;

  BigInt? get lastRevision => _lastRevision;

  void beginGeneration() {
    _consumerSequence = null;
  }

  JetStreamContinuousWatchCursorDecision inspect({
    required BigInt streamSequence,
    required BigInt consumerSequence,
  }) {
    final previousConsumerSequence = _consumerSequence;
    if (previousConsumerSequence != null &&
        consumerSequence != previousConsumerSequence + BigInt.one) {
      return JetStreamContinuousWatchCursorDecision.gap;
    }
    _consumerSequence = consumerSequence;
    final previousRevision = _lastRevision;
    if (previousRevision != null && streamSequence <= previousRevision) {
      return JetStreamContinuousWatchCursorDecision.duplicate;
    }
    return JetStreamContinuousWatchCursorDecision.accept;
  }

  void commit(BigInt streamSequence) {
    final previousRevision = _lastRevision;
    if (previousRevision != null && streamSequence <= previousRevision) {
      throw StateError("Watch cursor commit must advance the stream revision");
    }
    _lastRevision = streamSequence;
  }
}
