@TestOn("vm")
library;

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:test/test.dart";

import "support/scripted_nats_peer.dart";

void main() {
  for (final header in [
    "Nats-Batch-Commit",
    "Nats-Batch-Id",
    "Nats-Batch-Sequence",
    "Nats-Required-Api-Level",
  ]) {
    test("atomic batch rejects caller owned $header before wire use", () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final message = JetStreamAtomicMessage("events", const [
        1,
      ], headers: NatsHeaders(entries: [MapEntry(header, "caller")]));

      expect(
        () => JetStreamContext(
          harness.connection,
        ).publishAtomicBatch([message], batchId: "expected"),
        throwsArgumentError,
      );
      expect(harness.peer.publications, isEmpty);
    });
  }

  test(
    "confirmed atomic batch rejects owned headers before watching",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final message = JetStreamAtomicMessage(
        "events",
        const [1],
        headers: NatsHeaders(
          entries: const [MapEntry("Nats-Batch-Commit", "1")],
        ),
      );

      expect(
        () => JetStreamContext(
          harness.connection,
        ).publishAtomicBatchConfirmed("EVENTS", [message], batchId: "expected"),
        throwsArgumentError,
      );
      expect(harness.peer.publications, isEmpty);
    },
  );

  for (final scenario in [
    const _Scenario("wrong batch identity", "other", 2, "batch"),
    const _Scenario("count below expected", "expected", 1, "count"),
    const _Scenario("count above expected", "expected", 3, "count"),
  ]) {
    test("atomic acknowledgement rejects ${scenario.name}", () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);

      await harness.publish(
        acknowledgedBatch: scenario.batch,
        acknowledgedCount: scenario.count,
        expectedFailure: isA<JetStreamProtocolException>().having(
          (error) => error.fieldPath,
          "fieldPath",
          scenario.fieldPath,
        ),
      );
    });
  }

  test("atomic acknowledgement accepts the exact operation identity", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);

    final result = (await harness.publish(
      acknowledgedBatch: "expected",
      acknowledgedCount: 2,
    ))!;

    expect(result.batchId, "expected");
    expect(result.messageCount, BigInt.from(2));
  });

  test("atomic acknowledgement rejects stream sequence zero", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);

    await harness.publish(
      acknowledgedBatch: "expected",
      acknowledgedCount: 2,
      acknowledgedSequence: BigInt.zero,
      expectedFailure: isA<JetStreamProtocolException>().having(
        (error) => error.fieldPath,
        "fieldPath",
        "seq",
      ),
    );
  });
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

  Future<JetStreamAtomicBatchAck?> publish({
    required String acknowledgedBatch,
    required int acknowledgedCount,
    BigInt? acknowledgedSequence,
    Matcher? expectedFailure,
  }) async {
    final pending = JetStreamContext(connection).publishAtomicBatch([
      JetStreamAtomicMessage("events", const [1]),
      JetStreamAtomicMessage("events", const [2]),
    ], batchId: "expected");
    final expectation = expectedFailure == null
        ? null
        : expectLater(
            pending.timeout(const Duration(seconds: 2)),
            throwsA(expectedFailure),
          );
    for (var index = 0; index < 2; index += 1) {
      final staged = await peer
          .nextPublication("events")
          .timeout(const Duration(seconds: 2));
      await peer.sendMessage(staged.replySubject!, const []);
    }
    final commit = await peer
        .nextPublication("events")
        .timeout(const Duration(seconds: 2));
    await peer.sendMessage(
      commit.replySubject!,
      encodeExactJson({
        "stream": "EVENTS",
        "seq": acknowledgedSequence ?? BigInt.from(2),
        "batch": acknowledgedBatch,
        "count": BigInt.from(acknowledgedCount),
      }),
    );
    if (expectation != null) {
      await expectation;
      return null;
    }
    return pending.timeout(const Duration(seconds: 2));
  }

  Future<void> close() async {
    await connection.close();
    await peer.close();
  }
}

final class _Scenario {
  const _Scenario(this.name, this.batch, this.count, this.fieldPath);

  final String name;
  final String batch;
  final int count;
  final String fieldPath;
}
