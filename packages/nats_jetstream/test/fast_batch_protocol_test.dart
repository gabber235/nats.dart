@TestOn("vm")
library;

import "dart:convert";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

import "support/scripted_nats_peer.dart";

void main() {
  test("future gap sequence is rejected even when gaps are allowed", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final result = harness.publish(
      gapPolicy: JetStreamFastBatchGapPolicy.allow,
    );
    final failureResult = _failure(result);
    final first = await harness.nextPublication();

    await harness.sendControl(first, '{"type":"gap","last_seq":1,"seq":2}');

    final failure = await failureResult;
    expect(
      failure.cause,
      isA<NatsProtocolException>().having(
        (error) => error.message,
        "message",
        contains("gap exceeds the sent sequence"),
      ),
    );
  });

  test(
    "future error sequence is rejected even when gaps are allowed",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final result = harness.publish(
        gapPolicy: JetStreamFastBatchGapPolicy.allow,
      );
      final failureResult = _failure(result);
      final first = await harness.nextPublication();

      await harness.sendControl(first, _errorControl(sequence: 2));

      final failure = await failureResult;
      expect(
        failure.cause,
        isA<NatsProtocolException>().having(
          (error) => error.message,
          "message",
          contains("error exceeds the sent sequence"),
        ),
      );
    },
  );

  test("initial operation rejection is definitive", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final result = harness.publish();
    final failureResult = _failure(result);
    final first = await harness.nextPublication();
    expect(first.replySubject, endsWith(r".0.$FI"));

    await harness.sendControl(first, _errorControl(sequence: 1));

    final failure = await failureResult;
    expect(failure.sentSequence, BigInt.one);
    expect(failure.acknowledgedSequence, BigInt.zero);
    expect(failure.ambiguous, isFalse);
    expect(
      failure.cause,
      isA<JetStreamApiException>().having(
        (error) => error.errorCode,
        "errorCode",
        10205,
      ),
    );
  });

  test("initial top level API rejection is definitive", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final result = harness.publish();
    final failureResult = _failure(result);
    final first = await harness.nextPublication();
    expect(first.replySubject, endsWith(r".0.$FI"));

    await harness.sendControl(
      first,
      '{"error":{"code":400,"err_code":10205,'
      '"description":"disabled"}}',
    );

    final failure = await failureResult;
    expect(failure.sentSequence, BigInt.one);
    expect(failure.acknowledgedSequence, BigInt.zero);
    expect(failure.ambiguous, isFalse);
    expect(
      failure.cause,
      isA<JetStreamApiException>().having(
        (error) => error.errorCode,
        "errorCode",
        10205,
      ),
    );
  });

  test("rejection after initial acknowledgement remains ambiguous", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final result = harness.publish();
    final failureResult = _failure(result);
    final first = await harness.nextPublication();
    await harness.sendControl(first, '{"type":"ack","seq":1,"msgs":1}');
    final finalMessage = await harness.nextPublication();

    await harness.sendControl(finalMessage, _errorControl(sequence: 2));

    final failure = await failureResult;
    expect(failure.sentSequence, BigInt.from(2));
    expect(failure.acknowledgedSequence, BigInt.one);
    expect(failure.ambiguous, isTrue);
    expect(failure.cause, isA<JetStreamApiException>());
  });

  for (final count in [1, 2]) {
    test(
      "final acknowledgement accepts persisted prefix count $count",
      () async {
        final harness = await _Harness.open();
        addTearDown(harness.close);
        final result = harness.publish();
        final first = await harness.nextPublication();
        await harness.sendControl(first, '{"type":"ack","seq":1,"msgs":1}');
        final finalMessage = await harness.nextPublication();

        await harness.sendControl(
          finalMessage,
          '{"stream":"EVENTS","seq":2,"batch":"batch","count":$count}',
        );

        final acknowledgement = await result;
        expect(acknowledgement.messageCount, BigInt.from(count));
        expect(acknowledgement.acknowledgedBatchSequence, BigInt.one);
        expect(acknowledgement.gaps, isEmpty);
        expect(acknowledgement.errors, isEmpty);
      },
    );
  }
}

String _errorControl({required int sequence}) {
  return '{"type":"err","seq":$sequence,"error":'
      '{"code":400,"err_code":10205,"description":"disabled"}}';
}

Future<JetStreamFastBatchException> _failure(
  Future<JetStreamFastBatchAck> publication,
) async {
  try {
    await publication;
  } on JetStreamFastBatchException catch (error) {
    return error;
  }
  throw StateError("Fast batch unexpectedly completed");
}

final class _Harness {
  const _Harness(this.peer, this.connection);

  static Future<_Harness> open() async {
    final peer = await ScriptedNatsPeer.start();
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [NatsServer.parse(peer.uri.toString())],
        reconnect: NatsReconnectPolicy(maximumAttempts: 0),
        pingInterval: const Duration(minutes: 10),
      ),
    );
    return _Harness(peer, connection);
  }

  final ScriptedNatsPeer peer;
  final NatsConnection connection;

  Future<JetStreamFastBatchAck> publish({
    JetStreamFastBatchGapPolicy gapPolicy = JetStreamFastBatchGapPolicy.fail,
  }) {
    return JetStreamContext(connection).publishFastBatchMessages(
      [
        JetStreamFastBatchMessage("events", const [1]),
        JetStreamFastBatchMessage("events", const [2]),
      ],
      options: JetStreamFastBatchOptions(
        batchId: "batch",
        initialFlow: 1,
        gapPolicy: gapPolicy,
      ),
    );
  }

  Future<ScriptedNatsPublication> nextPublication() {
    return peer.nextPublication("events").timeout(const Duration(seconds: 2));
  }

  Future<void> sendControl(
    ScriptedNatsPublication publication,
    String payload,
  ) {
    return peer.sendMessage(publication.replySubject!, utf8.encode(payload));
  }

  Future<void> close() async {
    await connection.close();
    await peer.close();
  }
}
