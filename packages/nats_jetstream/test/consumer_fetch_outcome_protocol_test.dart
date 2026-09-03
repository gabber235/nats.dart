@TestOn("vm")
library;

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:test/test.dart";

import "support/scripted_nats_peer.dart";

const _pullSubject = r"$JS.API.CONSUMER.MSG.NEXT.EVENTS.WORKER";

void main() {
  for (final serverMaximumBytes in <int?>[null, 100]) {
    test("finite delivery enforces cumulative local bytes with server max "
        "$serverMaximumBytes", () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final result = harness.consumer.fetch(
        JetStreamFetchRequest(
          maximumMessages: 3,
          maximumBytes: serverMaximumBytes,
          maximumBufferedBytes: 100,
          expires: const Duration(seconds: 1),
        ),
      );
      await harness.respondToInfo();
      final pull = await harness.peer.nextPublication(_pullSubject);
      final request = decodeExactJsonObject(pull.body);
      expect(request["max_bytes"], BigInt.from(100));
      await harness.peer.sendMessage(pull.replySubject!, const [
        1,
      ], replySubject: r"$JS.ACK.EVENTS.WORKER.1.1.1.1.2");
      await harness.peer.sendMessage(pull.replySubject!, const [
        2,
      ], replySubject: r"$JS.ACK.EVENTS.WORKER.1.2.2.2.1");

      final failure = await _failure(result);
      expect(failure.partialMessages, hasLength(1));
      expect(failure.cause, isA<NatsSlowConsumerException>());
      expect(failure.ambiguous, isTrue);
    });
  }

  test(
    "finite pull queue rejects a flood when the server ignores max bytes",
    () async {
      const maximumBufferedBytes = 4096;
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final result = harness.consumer.fetch(
        JetStreamFetchRequest(
          maximumMessages: 1000,
          maximumBytes: 64 * 1024 * 1024,
          maximumBufferedBytes: maximumBufferedBytes,
          expires: const Duration(seconds: 1),
        ),
      );
      await harness.respondToInfo();
      final pull = await harness.peer.nextPublication(_pullSubject);
      final request = decodeExactJsonObject(pull.body);
      expect(request["max_bytes"], BigInt.from(maximumBufferedBytes));

      final payload = List<int>.filled(1024, 7);
      await harness.peer.sendMessageFlood(
        pull.replySubject!,
        payload,
        count: 32,
        replySubject: (index) =>
            "\$JS.ACK.EVENTS.WORKER.1.${index + 1}.${index + 1}.1.1000",
      );

      final failure = await _failure(result);
      expect(failure.partialMessages, isEmpty);
      expect(
        failure.cause,
        isA<NatsSlowConsumerException>().having(
          (error) => error.message,
          "message",
          contains("5120 bytes"),
        ),
      );
      expect(failure.ambiguous, isTrue);
    },
  );

  test("AckNone finite delivery preserves metadata without ack API", () async {
    final harness = await _Harness.open(
      ackPolicy: JetStreamConsumerAckPolicy.none,
    );
    addTearDown(harness.close);
    final result = harness.consumer.fetch(
      JetStreamFetchRequest(
        maximumMessages: 1,
        expires: const Duration(seconds: 1),
      ),
    );
    await harness.respondToInfo();
    final pull = await harness.peer.nextPublication(_pullSubject);
    await harness.peer.sendMessage(pull.replySubject!, const [
      7,
    ], replySubject: r"$JS.ACK.EVENTS.WORKER.1.4.3.2.0");

    final delivery = (await result).messages.single;
    expect(delivery.message.payload, const [7]);
    expect(delivery.metadata?.streamSequence, BigInt.from(4));
    expect(delivery.acknowledger, isNull);
    await harness.connection.flush();
    expect(
      harness.peer.publications.where(
        (publication) => publication.subject.startsWith(r"$JS.ACK."),
      ),
      isEmpty,
    );
  });

  test("AckExplicit finite delivery retains its ack API", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final result = harness.consumer.fetch(
      JetStreamFetchRequest(
        maximumMessages: 1,
        expires: const Duration(seconds: 1),
      ),
    );
    await harness.respondToInfo();
    final pull = await harness.peer.nextPublication(_pullSubject);
    await harness.peer.sendMessage(pull.replySubject!, const [
      8,
    ], replySubject: r"$JS.ACK.EVENTS.WORKER.1.4.3.2.0");

    final delivery = (await result).messages.single;
    expect(delivery.acknowledger, isNotNull);
  });

  test(
    "wrong delivery identity fails before exposure or acknowledgement",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final result = harness.consumer.fetch(
        JetStreamFetchRequest(
          maximumMessages: 1,
          expires: const Duration(seconds: 1),
        ),
      );
      await harness.respondToInfo();
      final pull = await harness.peer.nextPublication(_pullSubject);
      await harness.peer.sendMessage(pull.replySubject!, const [
        1,
      ], replySubject: r"$JS.ACK.OTHER.WORKER.1.1.1.1.0");

      final failure = await _failure(result);
      expect(failure.partialMessages, isEmpty);
      expect(failure.ambiguous, isTrue);
      expect(
        failure.cause,
        isA<JetStreamProtocolException>().having(
          (error) => error.fieldPath,
          "fieldPath",
          "stream",
        ),
      );
      expect(
        harness.peer.publications.where(
          (publication) => publication.subject.startsWith(r"$JS.ACK."),
        ),
        isEmpty,
      );
    },
  );

  test("disconnect after pull publication has an ambiguous outcome", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final result = harness.consumer.fetch(
      JetStreamFetchRequest(
        maximumMessages: 1,
        expires: const Duration(seconds: 1),
      ),
    );

    await harness.respondToInfo();
    await harness.peer.nextPublication(_pullSubject);
    await harness.peer.disconnectClient();

    final failure = await _failure(result);
    expect(failure.partialMessages, isEmpty);
    expect(failure.ambiguous, isTrue);
    expect(failure.cause, isA<NatsConnectionException>());
  });

  test("generation loss before pull publication is definitive", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    harness.peer.suspendPingResponses();
    final result = harness.consumer.fetch(
      JetStreamFetchRequest(
        maximumMessages: 1,
        expires: const Duration(seconds: 1),
      ),
    );

    await harness.respondToInfo();
    await harness.peer.nextSubscription("_INBOX.*");
    await harness.peer.disconnectClient();

    final failure = await _failure(result);
    expect(failure.partialMessages, isEmpty);
    expect(failure.ambiguous, isFalse);
    expect(failure.cause, isA<NatsConnectionException>());
    expect(
      harness.peer.publications.where(
        (publication) => publication.subject == _pullSubject,
      ),
      isEmpty,
    );
  });
}

Future<JetStreamFetchException> _failure(
  Future<JetStreamFetchResult> result,
) async {
  try {
    await result.timeout(const Duration(seconds: 2));
  } on JetStreamFetchException catch (error) {
    return error;
  }
  throw StateError("Finite pull unexpectedly completed");
}

final class _Harness {
  const _Harness(this.peer, this.connection, this.consumer, this.ackPolicy);

  static Future<_Harness> open({
    JetStreamConsumerAckPolicy ackPolicy = JetStreamConsumerAckPolicy.explicit,
  }) async {
    final peer = await ScriptedNatsPeer.start();
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [NatsServer.parse(peer.uri.toString())],
        reconnect: NatsReconnectPolicy(maximumAttempts: 0),
        pingInterval: const Duration(minutes: 10),
      ),
    );
    return _Harness(
      peer,
      connection,
      JetStreamPullConsumer(connection, r"$JS.API", "EVENTS", "WORKER"),
      ackPolicy,
    );
  }

  final ScriptedNatsPeer peer;
  final NatsConnection connection;
  final JetStreamPullConsumer consumer;
  final JetStreamConsumerAckPolicy ackPolicy;

  Future<void> respondToInfo() async {
    final request = await peer.nextPublication(
      r"$JS.API.CONSUMER.INFO.EVENTS.WORKER",
    );
    await peer.sendMessage(
      request.replySubject!,
      encodeExactJson(_consumerInfo(ackPolicy)),
    );
  }

  Future<void> close() async {
    await connection.close();
    await peer.close();
  }
}

Map<String, Object?> _consumerInfo(JetStreamConsumerAckPolicy ackPolicy) => {
  "stream_name": "EVENTS",
  "name": "WORKER",
  "created": "2026-09-02T00:00:00Z",
  "config": {
    "name": "WORKER",
    "deliver_policy": "all",
    "ack_policy": ackPolicy.wireValue,
    "replay_policy": "instant",
  },
  "delivered": {"consumer_seq": BigInt.zero, "stream_seq": BigInt.zero},
  "ack_floor": {"consumer_seq": BigInt.zero, "stream_seq": BigInt.zero},
  "num_ack_pending": BigInt.zero,
  "num_redelivered": BigInt.zero,
  "num_waiting": BigInt.zero,
  "num_pending": BigInt.one,
  "ts": "2026-09-02T00:00:00Z",
};
