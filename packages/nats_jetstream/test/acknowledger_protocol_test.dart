@TestOn("vm")
library;

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

import "support/scripted_nats_peer.dart";

const _ackSubject = r"$JS.ACK.EVENTS.WORKER.1.2.3.4.5";

void main() {
  test(
    "unconfirmed acknowledgement cannot report later confirmation",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final acknowledger = harness.acknowledger();

      await acknowledger.ack();
      await harness.peer.nextPublication(_ackSubject);

      expect(acknowledger.ackConfirmed, throwsStateError);
    },
  );

  test(
    "confirmed acknowledgement is idempotent and rejects other actions",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final acknowledger = harness.acknowledger();

      final confirmed = acknowledger.ackConfirmed();
      final request = await harness.peer.nextPublication(_ackSubject);
      await harness.peer.sendMessage(request.replySubject!, const []);
      await confirmed;

      expect(acknowledger.ack(), same(confirmed));
      expect(acknowledger.ackConfirmed(), same(confirmed));
      expect(acknowledger.nak, throwsStateError);
    },
  );

  test("different terminal acknowledgement actions never alias", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final actions =
        <({String name, Future<void> Function(JetStreamAcknowledger) run})>[
          (name: "ack", run: (value) => value.ack()),
          (name: "nak", run: (value) => value.nak()),
          (
            name: "nakAfter",
            run: (value) =>
                value.nakAfter(JetStreamNanoseconds(BigInt.from(1000000))),
          ),
          (name: "term", run: (value) => value.term()),
          (name: "termReason", run: (value) => value.termWithReason("invalid")),
        ];

    for (final first in actions) {
      for (final second in actions) {
        if (first.name == second.name) continue;
        final acknowledger = harness.acknowledger();
        await first.run(acknowledger);
        await harness.peer.nextPublication(_ackSubject);
        expect(
          () => second.run(acknowledger),
          throwsStateError,
          reason: "${first.name} then ${second.name}",
        );
      }
    }
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

  JetStreamAcknowledger acknowledger() {
    return JetStreamDelivery.fromMessage(
      connection,
      connection.generation,
      NatsMessage(
        subject: "delivery",
        payload: const [],
        replySubject: _ackSubject,
      ),
    ).acknowledger!;
  }

  Future<void> close() async {
    await connection.close();
    await peer.close();
  }
}
