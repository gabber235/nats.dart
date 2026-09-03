@TestOn("vm")
library;

import "dart:async";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:test/test.dart";

import "support/scripted_nats_peer.dart";

const _pullSubject = r"$JS.API.CONSUMER.MSG.NEXT.EVENTS.WORKER";
const _deadline = Duration(seconds: 2);

void main() {
  test(
    "AckNone continuous delivery preserves metadata without ack API",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final stream = harness.consume();
      final deliveryFuture = stream.messages.first;
      await harness.respondToInfo(JetStreamConsumerAckPolicy.none);
      final pull = await harness.peer.nextPublication(_pullSubject);
      await harness.peer.sendMessage(pull.replySubject!, const [
        7,
      ], replySubject: r"$JS.ACK.EVENTS.WORKER.1.4.3.2.0");

      final delivery = await deliveryFuture;
      expect(delivery.metadata?.streamSequence, BigInt.from(4));
      expect(delivery.acknowledger, isNull);
      await stream.stop().timeout(_deadline);
      await harness.connection.flush();
      expect(
        harness.peer.publications.where(
          (publication) => publication.subject.startsWith(r"$JS.ACK."),
        ),
        isEmpty,
      );
    },
  );

  test("continuous pull rejects another consumer delivery", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final stream = harness.consume();
    final doneExpectation = expectLater(
      stream.done,
      throwsA(
        isA<JetStreamProtocolException>().having(
          (error) => error.fieldPath,
          "fieldPath",
          "consumer",
        ),
      ),
    );
    final messages = <JetStreamDelivery>[];
    final errors = <Object>[];
    final listener = stream.messages.listen(messages.add, onError: errors.add);
    addTearDown(listener.cancel);
    await harness.respondToInfo(JetStreamConsumerAckPolicy.explicit);
    final pull = await harness.peer.nextPublication(_pullSubject);
    await harness.peer.sendMessage(pull.replySubject!, const [
      1,
    ], replySubject: r"$JS.ACK.EVENTS.OTHER.1.1.1.1.0");

    await doneExpectation;
    expect(messages, isEmpty);
    expect(errors, [isA<JetStreamProtocolException>()]);
    expect(
      harness.peer.publications.where(
        (publication) => publication.subject.startsWith(r"$JS.ACK."),
      ),
      isEmpty,
    );
  });

  test("immediate pause defers the initial pull until resume", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final errors = <Object>[];
    final stream = harness.consume();
    final listener = stream.messages.listen((_) {}, onError: errors.add)
      ..pause();

    await harness.respondToInfo(JetStreamConsumerAckPolicy.explicit);
    final inbox = await harness.peer.nextSubscription("_INBOX.*");
    await harness.connection.flush();

    expect(
      harness.peer.publications.where(
        (publication) => publication.subject == _pullSubject,
      ),
      isEmpty,
    );

    listener.resume();
    final pull = await harness.peer.nextPublication(_pullSubject);
    expect(pull.replySubject, inbox);
    expect(decodeExactJsonObject(pull.body), {
      "batch": BigInt.from(2),
      "max_bytes": BigInt.from(4096),
      "expires": BigInt.from(5000000000),
      "idle_heartbeat": BigInt.from(1000000000),
    });
    await harness.connection.flush();
    expect(_pullCount(harness.peer), 1);

    await stream.stop().timeout(_deadline);
    await listener.cancel();
    expect(errors, isEmpty);
  });

  test("pause at a batch boundary prevents refill", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final deferred = await _pauseAtBatchBoundary(harness);

    await harness.connection.flush();

    expect(_pullCount(harness.peer), 1);
    await deferred.stream.stop().timeout(_deadline);
    await deferred.listener.cancel();
    expect(deferred.errors, isEmpty);
  });

  test("resume after a batch boundary emits exactly one refill", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final deferred = await _pauseAtBatchBoundary(harness);

    deferred.listener.resume();
    final refill = await harness.peer.nextPublication(_pullSubject);
    expect(refill.replySubject, deferred.inbox);
    await harness.connection.flush();

    expect(_pullCount(harness.peer), 2);
    await deferred.stream.stop().timeout(_deadline);
    await deferred.listener.cancel();
    expect(deferred.errors, isEmpty);
  });

  test("stop while refill is deferred completes without refill", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final deferred = await _pauseAtBatchBoundary(harness);

    await deferred.stream.stop().timeout(_deadline);
    await deferred.stream.done.timeout(_deadline);
    await harness.connection.flush();

    expect(_pullCount(harness.peer), 1);
    await deferred.listener.cancel();
    expect(deferred.errors, isEmpty);
  });

  test("drain while refill is deferred completes without refill", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final deferred = await _pauseAtBatchBoundary(harness);

    await deferred.stream.drain(timeout: _deadline);
    await deferred.stream.done.timeout(_deadline);
    await harness.connection.flush();

    expect(_pullCount(harness.peer), 1);
    await deferred.listener.cancel();
    expect(deferred.errors, isEmpty);
  });

  test("connection close while refill is deferred settles done", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final deferred = await _pauseAtBatchBoundary(harness);
    final doneExpectation = expectLater(
      deferred.stream.done.timeout(_deadline),
      throwsA(isA<NatsClosedException>()),
    );

    await harness.connection.close();

    await doneExpectation;
    deferred.listener.resume();
    await deferred.closed.future.timeout(_deadline);
    expect(deferred.errors, [isA<NatsClosedException>()]);
    expect(_pullCount(harness.peer), 1);
    await deferred.listener.cancel();
  });

  test("paused continuous pull has one Core queue byte owner", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final stream = harness.consume(
      maximumPendingMessages: 8,
      maximumPendingBytes: 1024,
    );
    final doneExpectation = expectLater(
      stream.done.timeout(_deadline),
      throwsA(
        isA<NatsSlowConsumerException>().having(
          (error) => error.message,
          "message",
          contains("3072 bytes"),
        ),
      ),
    );
    final delivered = <JetStreamDelivery>[];
    final errors = <Object>[];
    final closed = Completer<void>();
    final paused = Completer<void>();
    late final StreamSubscription<JetStreamDelivery> listener;
    listener = stream.messages.listen(
      (delivery) {
        delivered.add(delivery);
        if (!paused.isCompleted) {
          listener.pause();
          paused.complete();
        }
      },
      onError: errors.add,
      onDone: closed.complete,
    );
    addTearDown(listener.cancel);
    await harness.respondToInfo(JetStreamConsumerAckPolicy.explicit);
    final pull = await harness.peer.nextPublication(_pullSubject);
    await harness.peer.sendMessage(pull.replySubject!, const [
      1,
    ], replySubject: r"$JS.ACK.EVENTS.WORKER.1.1.1.1.1000");
    await paused.future.timeout(_deadline);

    await harness.peer.sendMessageFlood(
      pull.replySubject!,
      List<int>.filled(256, 7),
      count: 32,
      replySubject: (index) =>
          "\$JS.ACK.EVENTS.WORKER.1.${index + 1}.${index + 1}.1.1000",
    );

    await doneExpectation;
    expect(_pullCount(harness.peer), 1);
    expect(delivered, hasLength(1));
    final statistics = harness.connection.statistics;
    expect(statistics.activeSubscriptions, 0);
    expect(statistics.subscriptionQueuedMessages, 0);
    expect(statistics.subscriptionQueuedBytes, 0);

    listener.resume();
    await closed.future.timeout(_deadline);
    expect(errors, [isA<NatsSlowConsumerException>()]);
  });
}

Future<_DeferredPull> _pauseAtBatchBoundary(_Harness harness) async {
  final errors = <Object>[];
  final closed = Completer<void>();
  final paused = Completer<void>();
  final stream = harness.consume();
  var received = 0;
  // ignore: cancel_subscriptions
  late final StreamSubscription<JetStreamDelivery> listener;
  listener = stream.messages.listen(
    (_) {
      received += 1;
      if (received == 2) {
        listener.pause();
        paused.complete();
      }
    },
    onError: errors.add,
    onDone: closed.complete,
  );
  await harness.respondToInfo(JetStreamConsumerAckPolicy.explicit);
  final pull = await harness.peer.nextPublication(_pullSubject);
  final inbox = pull.replySubject!;

  await harness.peer.sendMessage(inbox, const [
    1,
  ], replySubject: r"$JS.ACK.EVENTS.WORKER.1.1.1.1.1");
  await harness.peer.sendMessage(inbox, const [
    2,
  ], replySubject: r"$JS.ACK.EVENTS.WORKER.1.2.2.2.0");
  await paused.future.timeout(_deadline);

  return _DeferredPull(
    stream: stream,
    listener: listener,
    inbox: inbox,
    errors: errors,
    closed: closed,
  );
}

int _pullCount(ScriptedNatsPeer peer) => peer.publications
    .where((publication) => publication.subject == _pullSubject)
    .length;

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

  JetStreamConsumerStream consume({
    int maximumPendingMessages = 2,
    int maximumPendingBytes = 4096,
  }) {
    return JetStreamPullConsumer(
      connection,
      r"$JS.API",
      "EVENTS",
      "WORKER",
    ).consume(
      options: JetStreamConsumeOptions(
        batchSize: 2,
        expires: const Duration(seconds: 5),
        idleHeartbeat: const Duration(seconds: 1),
        maximumPendingMessages: maximumPendingMessages,
        maximumPendingBytes: maximumPendingBytes,
      ),
    );
  }

  Future<void> respondToInfo(JetStreamConsumerAckPolicy ackPolicy) async {
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

final class _DeferredPull {
  const _DeferredPull({
    required this.stream,
    required this.listener,
    required this.inbox,
    required this.errors,
    required this.closed,
  });

  final JetStreamConsumerStream stream;
  final StreamSubscription<JetStreamDelivery> listener;
  final String inbox;
  final List<Object> errors;
  final Completer<void> closed;
}
