@TestOn("vm")
library;

import "dart:convert";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

import "support/scripted_nats_peer.dart";

void main() {
  test("first message cannot exceed the local retained byte bound", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);

    final failure = await _failure(
      JetStreamContext(harness.connection).publishFastBatchMessages(
        [JetStreamFastBatchMessage("events", List.filled(256, 1))],
        options: JetStreamFastBatchOptions(
          batchId: "first",
          maximumOutstandingBytes: 128,
        ),
      ),
    ).timeout(const Duration(seconds: 2));

    expect(failure.message, contains("maximumOutstandingBytes"));
    expect(failure.ambiguous, isFalse);
    expect(failure.cause, isA<ArgumentError>());
    expect(harness.peer.publications, isEmpty);
  });

  test("message after an acknowledgement still obeys the byte bound", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final publication = JetStreamContext(harness.connection)
        .publishFastBatchMessages(
          [
            JetStreamFastBatchMessage("events", const []),
            JetStreamFastBatchMessage("events", List.filled(256, 2)),
          ],
          options: JetStreamFastBatchOptions(
            batchId: "after-ack",
            initialFlow: 1,
            maximumOutstandingBytes: 128,
          ),
        );
    final failureResult = _failure(publication);

    final first = await harness.peer
        .nextPublication("events")
        .timeout(const Duration(seconds: 2));
    await harness.peer.sendMessage(
      first.replySubject!,
      utf8.encode('{"type":"ack","seq":1,"msgs":1}'),
    );
    final failure = await failureResult.timeout(const Duration(seconds: 2));

    expect(failure.message, contains("maximumOutstandingBytes"));
    expect(failure.ambiguous, isFalse);
    expect(failure.acknowledgedSequence, BigInt.one);
    expect(harness.peer.publications, hasLength(1));
  });
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

  Future<void> close() async {
    await connection.close();
    await peer.close();
  }
}
