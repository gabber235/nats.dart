@TestOn("vm")
library;

import "dart:async";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/continuous_watch.dart";
import "package:nats_jetstream/src/continuous_watch_generation.dart";
import "package:nats_jetstream/src/continuous_watch_session.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/kv_watch_session.dart";
import "package:nats_jetstream/src/object_store_watch_session.dart";
import "package:test/test.dart";

import "support/scripted_nats_peer.dart";

const _deadline = Duration(seconds: 2);

void main() {
  test(
    "ordered recovery deletes a consumer whose create response arrives late",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final jetStream = JetStreamContext(
        harness.connection,
        timeout: const Duration(seconds: 1),
      );
      final ordered = jetStream.consumers
          .ordered(
            "EVENTS",
            options: JetStreamOrderedConsumerOptions(
              recoveryTimeout: const Duration(milliseconds: 100),
            ),
          )
          .consume();
      final errors = <Object>[];
      final listener = ordered.messages.listen((_) {}, onError: errors.add);
      final doneFailure = expectLater(
        ordered.done.timeout(_deadline),
        throwsA(
          isA<JetStreamOrderedConsumerRecoveryException>().having(
            (error) => error.cause,
            "cause",
            isA<NatsTimeoutException>(),
          ),
        ),
      );

      final create = await harness.peer.nextPublicationMatching(
        r"$JS.API.CONSUMER.CREATE.EVENTS.*",
      );
      final consumer = create.subject.split(".").last;
      final serverConsumers = {consumer};
      final delete = await harness.peer.nextPublication(
        "\$JS.API.CONSUMER.DELETE.EVENTS.$consumer",
      );

      await harness.peer.sendMessage(
        create.replySubject!,
        _consumerInfoResponse(create),
      );
      await harness.peer.sendMessage(
        delete.replySubject!,
        encodeExactJson({"success": true}),
      );
      serverConsumers.remove(consumer);

      await doneFailure;
      await listener.cancel();
      expect(serverConsumers, isEmpty);
      expect(
        harness.peer.publications.where(
          (publication) => publication.subject.startsWith(
            r"$JS.API.CONSUMER.CREATE.EVENTS.",
          ),
        ),
        hasLength(1),
      );
      expect(errors, [isA<JetStreamOrderedConsumerRecoveryException>()]);
    },
  );

  test("permanent close during retry delay settles publication", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final publisher = JetStreamContext(harness.connection).createAsyncPublisher(
      options: JetStreamAsyncPublisherOptions(
        acknowledgementTimeout: const Duration(seconds: 10),
        attemptTimeout: const Duration(seconds: 2),
        retryPolicy: JetStreamAsyncPublishRetryPolicy.deduplicated,
        maximumAttempts: 2,
      ),
    );
    addTearDown(() async {
      try {
        await publisher.close();
      } on Object {
        return;
      }
    });

    final pending = publisher.publish("events", const [
      1,
    ], options: JetStreamPublishOptions(messageId: "stable"));
    final acknowledgementFailure = expectLater(
      pending.acknowledgement.timeout(_deadline),
      throwsA(
        isA<JetStreamAsyncPublishException>().having(
          (error) => error.cause,
          "cause",
          isA<NatsClosedException>(),
        ),
      ),
    );
    final first = await harness.peer.nextPublication("events");
    await harness.peer.sendMessage(
      first.replySubject!,
      encodeExactJson({
        "error": {
          "code": 409,
          "err_code": 10158,
          "description": "duplicate message id is in process",
        },
      }),
    );
    await _waitForState(pending, JetStreamAsyncPublishState.waitingToRetry);

    await harness.connection.close();

    await acknowledgementFailure;
    expect(pending.attempts, 1);
  });

  test("watch cancellation waits for consumer deletion and done", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final watch = JetStreamContinuousWatchSession<int, int>(
      connection: harness.connection,
      apiPrefix: r"$JS.API",
      timeout: const Duration(seconds: 1),
      options: JetStreamContinuousWatchOptions(
        stream: "WATCH",
        filters: const JetStreamConsumerFilters.all(),
        start: const JetStreamContinuousWatchStart(
          JetStreamContinuousWatchStartKind.current,
        ),
        maximumPendingMessages: 8,
        maximumPendingBytes: 1024,
        heartbeat: const Duration(seconds: 1),
        label: "test watch",
      ),
      decode: (delivery) => delivery.message.payload.single,
      include: (_) => true,
      changedEvent: (value) => value,
      initialCompleteEvent: 0,
    );
    final ready = Completer<void>();
    final listener = watch.events.listen((_) {
      if (!ready.isCompleted) ready.complete();
    });
    final create = await harness.peer.nextPublicationMatching(
      r"$JS.API.CONSUMER.CREATE.WATCH.*",
    );
    final consumer = create.subject.split(".").last;
    await harness.peer.sendMessage(
      create.replySubject!,
      _consumerInfoResponse(create),
    );
    await ready.future.timeout(_deadline);

    var cancellationCompleted = false;
    final cancellation = listener.cancel().then((_) {
      cancellationCompleted = true;
    });
    final delete = await harness.peer.nextPublication(
      "\$JS.API.CONSUMER.DELETE.WATCH.$consumer",
    );
    expect(cancellationCompleted, isFalse);

    await harness.peer.sendMessage(
      delete.replySubject!,
      encodeExactJson({"success": true}),
    );
    await cancellation.timeout(_deadline);
    await watch.done.timeout(_deadline);
    expect(cancellationCompleted, isTrue);
  });

  for (final adapter in ["KV", "Object Store"]) {
    test("$adapter watch pause has one Core queue byte owner", () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      late final Stream<Object?> events;
      late final Future<void> done;
      if (adapter == "KV") {
        final watch = JetStreamKeyValueWatchSession(
          harness.connection,
          r"$JS.API",
          const Duration(seconds: 1),
          "BUCKET",
          "KV_BUCKET",
          r"$KV.BUCKET.",
          const JetStreamKeyValueCodecs(),
          JetStreamKeyValueWatchOptions(
            maximumPendingMessages: 8,
            maximumPendingBytes: 1024,
            heartbeat: const Duration(seconds: 1),
          ),
        );
        events = watch.events;
        done = watch.done;
      } else {
        final watch = JetStreamObjectWatchSession(
          harness.connection,
          r"$JS.API",
          const Duration(seconds: 1),
          "BUCKET",
          JetStreamObjectWatchOptions(
            maximumPendingMessages: 8,
            maximumPendingBytes: 1024,
            heartbeat: const Duration(seconds: 1),
          ),
        );
        events = watch.events;
        done = watch.done;
      }
      final delivered = <Object?>[];
      final errors = <Object>[];
      final paused = Completer<void>();
      final closed = Completer<void>();
      late final StreamSubscription<Object?> listener;
      listener = events.listen(
        (event) {
          delivered.add(event);
          if (!paused.isCompleted) {
            listener.pause();
            paused.complete();
          }
        },
        onError: errors.add,
        onDone: closed.complete,
      );
      addTearDown(listener.cancel);

      final stream = adapter == "KV" ? "KV_BUCKET" : "OBJ_BUCKET";
      final storedSubject = adapter == "KV"
          ? r"$KV.BUCKET.>"
          : r"$O.BUCKET.M.>";
      final info = await harness.peer.nextPublication(
        "\$JS.API.STREAM.INFO.$stream",
      );
      await harness.peer.sendMessage(
        info.replySubject!,
        encodeExactJson(_streamInfoResponse(stream, storedSubject)),
      );
      final inbox = await harness.peer.nextSubscription("_INBOX.*");
      final create = await harness.peer.nextPublicationMatching(
        r"$JS.API.CONSUMER.CREATE.*.*",
      );
      final request = decodeExactJsonObject(create.body);
      expect(request["stream_name"], stream);
      final consumer = create.subject.split(".").last;
      await harness.peer.sendMessage(
        create.replySubject!,
        _consumerInfoResponse(create),
      );
      await paused.future.timeout(_deadline);

      await harness.peer.sendMessageFlood(
        inbox,
        List<int>.filled(256, 7),
        count: 32,
        replySubject: (index) =>
            "\$JS.ACK.$stream.$consumer.1.${index + 1}.${index + 1}.1.1000",
      );
      final delete = await harness.peer.nextPublication(
        "\$JS.API.CONSUMER.DELETE.$stream.$consumer",
      );
      await harness.peer.sendMessage(
        delete.replySubject!,
        encodeExactJson({"success": true}),
      );

      await expectLater(
        done.timeout(_deadline),
        throwsA(
          isA<NatsSlowConsumerException>().having(
            (error) => error.message,
            "message",
            contains("3072 bytes"),
          ),
        ),
      );
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

  test(
    "watch retains immediate delivery emitted during consumer creation",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final watch = JetStreamContinuousWatchSession<int, int>(
        connection: harness.connection,
        apiPrefix: r"$JS.API",
        timeout: const Duration(seconds: 1),
        options: JetStreamContinuousWatchOptions(
          stream: "WATCH",
          filters: const JetStreamConsumerFilters.all(),
          start: const JetStreamContinuousWatchStart(
            JetStreamContinuousWatchStartKind.current,
          ),
          maximumPendingMessages: 8,
          maximumPendingBytes: 1024,
          heartbeat: const Duration(seconds: 1),
          label: "test watch",
        ),
        decode: (delivery) => delivery.message.payload.single,
        include: (_) => true,
        changedEvent: (value) => value,
        initialCompleteEvent: 0,
      );
      final events = StreamIterator(watch.events);
      final firstEvent = events.moveNext();

      final inbox = await harness.peer
          .nextSubscription("_INBOX.*")
          .timeout(_deadline);
      final create = await harness.peer
          .nextPublicationMatching(r"$JS.API.CONSUMER.CREATE.WATCH.*")
          .timeout(_deadline);
      final request = decodeExactJsonObject(create.body);
      final config = request["config"]! as Map<String, Object?>;
      expect(config["deliver_subject"], inbox);
      final consumer = create.subject.split(".").last;
      const acknowledgementPrefix = r"$JS.ACK.WATCH.";

      await harness.peer.sendMessage(inbox, const [
        7,
      ], replySubject: "$acknowledgementPrefix$consumer.1.1.1.1.0");
      await harness.peer.sendMessage(
        create.replySubject!,
        _consumerInfoResponse(create, pending: BigInt.one),
      );

      expect(await firstEvent.timeout(_deadline), isTrue);
      expect(events.current, 7);
      expect(await events.moveNext().timeout(_deadline), isTrue);
      expect(events.current, 0);

      final cancellation = events.cancel();
      final delete = await harness.peer.nextPublication(
        "\$JS.API.CONSUMER.DELETE.WATCH.$consumer",
      );
      await harness.peer.sendMessage(
        delete.replySubject!,
        encodeExactJson({"success": true}),
      );
      await cancellation.timeout(_deadline);
      await watch.done.timeout(_deadline);
    },
  );

  test(
    "watch terminates when an initial snapshot generation is lost",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final watch = JetStreamContinuousWatchSession<int, int>(
        connection: harness.connection,
        apiPrefix: r"$JS.API",
        timeout: const Duration(seconds: 1),
        options: JetStreamContinuousWatchOptions(
          stream: "WATCH",
          filters: const JetStreamConsumerFilters.all(),
          start: const JetStreamContinuousWatchStart(
            JetStreamContinuousWatchStartKind.current,
          ),
          maximumPendingMessages: 8,
          maximumPendingBytes: 1024,
          heartbeat: const Duration(seconds: 1),
          label: "test watch",
        ),
        decode: (delivery) => delivery.message.payload.single,
        include: (_) => true,
        changedEvent: (value) => value,
        initialCompleteEvent: 0,
      );
      final events = StreamIterator(watch.events);
      final firstEvent = events.moveNext();
      final doneFailure = expectLater(
        watch.done,
        throwsA(
          isA<JetStreamInitialSnapshotInterruptedException>()
              .having((error) => error.operation, "operation", "test watch")
              .having(
                (error) => error.lastRevision,
                "lastRevision",
                BigInt.one,
              ),
        ),
      );
      final inbox = await harness.peer.nextSubscription("_INBOX.*");
      final create = await harness.peer.nextPublicationMatching(
        r"$JS.API.CONSUMER.CREATE.WATCH.*",
      );
      final consumer = create.subject.split(".").last;
      await harness.peer.sendMessage(
        create.replySubject!,
        _consumerInfoResponse(create, pending: BigInt.two),
      );
      await harness.peer.sendMessage(inbox, const [
        7,
      ], replySubject: "\$JS.ACK.WATCH.$consumer.1.1.1.1.1");
      expect(await firstEvent.timeout(_deadline), isTrue);
      expect(events.current, 7);

      await harness.peer.disconnectClient();

      await doneFailure;
      await events.cancel();
    },
  );

  test("watch rejects another consumer delivery before decoding", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final watch = JetStreamContinuousWatchSession<int, int>(
      connection: harness.connection,
      apiPrefix: r"$JS.API",
      timeout: const Duration(seconds: 1),
      options: JetStreamContinuousWatchOptions(
        stream: "WATCH",
        filters: const JetStreamConsumerFilters.all(),
        start: const JetStreamContinuousWatchStart(
          JetStreamContinuousWatchStartKind.current,
        ),
        maximumPendingMessages: 8,
        maximumPendingBytes: 1024,
        heartbeat: const Duration(seconds: 1),
        label: "test watch",
      ),
      decode: (delivery) => delivery.message.payload.single,
      include: (_) => true,
      changedEvent: (value) => value,
      initialCompleteEvent: 0,
    );
    final delivered = <int>[];
    final errors = <Object>[];
    final listener = watch.events.listen(delivered.add, onError: errors.add);
    final doneFailure = expectLater(
      watch.done,
      throwsA(
        isA<JetStreamProtocolException>().having(
          (error) => error.fieldPath,
          "fieldPath",
          "consumer",
        ),
      ),
    );
    addTearDown(listener.cancel);
    final inbox = await harness.peer.nextSubscription("_INBOX.*");
    final create = await harness.peer.nextPublicationMatching(
      r"$JS.API.CONSUMER.CREATE.WATCH.*",
    );
    final consumer = create.subject.split(".").last;
    await harness.peer.sendMessage(
      create.replySubject!,
      _consumerInfoResponse(create, pending: BigInt.one),
    );
    await harness.peer.sendMessage(inbox, const [
      7,
    ], replySubject: r"$JS.ACK.WATCH.OTHER.1.1.1.1.0");
    final delete = await harness.peer.nextPublication(
      "\$JS.API.CONSUMER.DELETE.WATCH.$consumer",
    );
    await harness.peer.sendMessage(
      delete.replySubject!,
      encodeExactJson({"success": true}),
    );

    await doneFailure;
    expect(delivered, isEmpty);
    expect(errors, [isA<JetStreamProtocolException>()]);
    expect(
      harness.peer.publications.where(
        (publication) => publication.subject.startsWith(r"$JS.ACK."),
      ),
      isEmpty,
    );
  });

  test("watch create failure releases delivery subscription", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final owner = JetStreamContinuousWatchGenerationOwner(
      harness.connection,
      r"$JS.API",
      const Duration(seconds: 1),
      JetStreamContinuousWatchOptions(
        stream: "WATCH",
        filters: const JetStreamConsumerFilters.all(),
        start: const JetStreamContinuousWatchStart(
          JetStreamContinuousWatchStartKind.current,
        ),
        maximumPendingMessages: 8,
        maximumPendingBytes: 1024,
        heartbeat: const Duration(seconds: 1),
        label: "test watch",
      ),
    );
    addTearDown(owner.close);
    final opening = owner.open(const JetStreamConsumerStart.all());
    final openingFailure = expectLater(
      opening,
      throwsA(isA<JetStreamApiException>()),
    );

    final inbox = await harness.peer
        .nextSubscription("_INBOX.*")
        .timeout(_deadline);
    final create = await harness.peer
        .nextPublicationMatching(r"$JS.API.CONSUMER.CREATE.WATCH.*")
        .timeout(_deadline);
    final consumer = create.subject.split(".").last;
    await harness.peer.sendMessage(
      create.replySubject!,
      encodeExactJson({
        "error": {
          "code": 400,
          "err_code": 10000,
          "description": "consumer rejected",
        },
      }),
    );
    final delete = await harness.peer.nextPublication(
      "\$JS.API.CONSUMER.DELETE.WATCH.$consumer",
    );
    await harness.peer.sendMessage(
      delete.replySubject!,
      encodeExactJson({"success": true}),
    );

    await openingFailure;
    await expectLater(
      harness.peer.sendMessage(inbox, const [1]),
      throwsA(isA<StateError>()),
    );
  });

  test(
    "continuous pull stop settles during unlimited reconnect outage",
    () async {
      final harness = await _Harness.open(maximumReconnectAttempts: -1);
      addTearDown(harness.close);
      final disconnected = harness.connection.events.firstWhere(
        (event) => event is NatsDisconnected,
      );
      await harness.peer.disconnectClient();
      await disconnected.timeout(_deadline);
      final stream = JetStreamPullConsumer(
        harness.connection,
        r"$JS.API",
        "EVENTS",
        "WORKER",
      ).consume();
      final listener = stream.messages.listen((_) {});

      await stream.stop().timeout(_deadline);

      await stream.done.timeout(_deadline);
      await listener.cancel();
    },
  );

  test("KV watch close settles during unlimited reconnect outage", () async {
    final harness = await _Harness.open(maximumReconnectAttempts: -1);
    addTearDown(harness.close);
    final disconnected = harness.connection.events.firstWhere(
      (event) => event is NatsDisconnected,
    );
    await harness.peer.disconnectClient();
    await disconnected.timeout(_deadline);
    final watch = JetStreamKeyValueWatchSession(
      harness.connection,
      r"$JS.API",
      const Duration(seconds: 30),
      "BUCKET",
      "KV_BUCKET",
      r"$KV.BUCKET.",
      const JetStreamKeyValueCodecs(),
      JetStreamKeyValueWatchOptions(),
    );
    final listener = watch.events.listen((_) {});

    await watch.close().timeout(_deadline);

    await watch.done.timeout(_deadline);
    await listener.cancel();
  });

  test(
    "Object Store watch close settles during unlimited reconnect outage",
    () async {
      final harness = await _Harness.open(maximumReconnectAttempts: -1);
      addTearDown(harness.close);
      final disconnected = harness.connection.events.firstWhere(
        (event) => event is NatsDisconnected,
      );
      await harness.peer.disconnectClient();
      await disconnected.timeout(_deadline);
      final watch = JetStreamObjectWatchSession(
        harness.connection,
        r"$JS.API",
        const Duration(seconds: 30),
        "BUCKET",
        JetStreamObjectWatchOptions(),
      );
      final listener = watch.events.listen((_) {});

      await watch.close().timeout(_deadline);

      await watch.done.timeout(_deadline);
      await listener.cancel();
    },
  );

  test("connection close racing watch listener attachment settles", () async {
    final harness = await _Harness.open(maximumReconnectAttempts: -1);
    addTearDown(harness.close);
    final watch = JetStreamContinuousWatchSession<int, int>(
      connection: harness.connection,
      apiPrefix: r"$JS.API",
      timeout: const Duration(seconds: 30),
      options: JetStreamContinuousWatchOptions(
        stream: "WATCH",
        filters: const JetStreamConsumerFilters.all(),
        start: const JetStreamContinuousWatchStart(
          JetStreamContinuousWatchStartKind.current,
        ),
        maximumPendingMessages: 8,
        maximumPendingBytes: 1024,
        heartbeat: const Duration(seconds: 1),
        label: "race watch",
      ),
      decode: (delivery) => delivery.message.payload.single,
      include: (_) => true,
      changedEvent: (value) => value,
      initialCompleteEvent: 0,
    );
    final errors = <Object>[];
    final listener = watch.events.listen((_) {}, onError: errors.add);
    final doneFailure = expectLater(
      watch.done.timeout(_deadline),
      throwsA(isA<NatsClosedException>()),
    );

    final close = harness.connection.close();

    await close.timeout(_deadline);
    await doneFailure;
    expect(errors, [isA<NatsClosedException>()]);
    await listener.cancel();
  });
}

List<int> _consumerInfoResponse(
  ScriptedNatsPublication create, {
  BigInt? pending,
}) {
  final request = decodeExactJsonObject(create.body);
  final config = request["config"]! as Map<String, Object?>;
  final consumer = create.subject.split(".").last;
  return encodeExactJson({
    "stream_name": request["stream_name"]!,
    "name": consumer,
    "created": "2026-09-02T00:00:00Z",
    "config": config,
    "delivered": {"consumer_seq": BigInt.zero, "stream_seq": BigInt.zero},
    "ack_floor": {"consumer_seq": BigInt.zero, "stream_seq": BigInt.zero},
    "num_ack_pending": BigInt.zero,
    "num_redelivered": BigInt.zero,
    "num_waiting": BigInt.zero,
    "num_pending": pending ?? BigInt.zero,
    "ts": "2026-09-02T00:00:00Z",
  });
}

Map<String, Object?> _streamInfoResponse(String stream, String subject) => {
  "config": {
    "name": stream,
    "subjects": [subject],
    "retention": "limits",
    "storage": "memory",
    "discard": "old",
  },
  "created": "2026-09-02T00:00:00Z",
  "state": {
    "messages": BigInt.zero,
    "bytes": BigInt.zero,
    "first_seq": BigInt.zero,
    "first_ts": "0001-01-01T00:00:00Z",
    "last_seq": BigInt.zero,
    "last_ts": "0001-01-01T00:00:00Z",
    "consumer_count": BigInt.zero,
  },
  "ts": "2026-09-02T00:00:00Z",
};

Future<void> _waitForState(
  JetStreamPendingPublish pending,
  JetStreamAsyncPublishState expected,
) async {
  final deadline = DateTime.timestamp().add(_deadline);
  while (pending.state != expected) {
    if (DateTime.timestamp().isAfter(deadline)) {
      fail("Publication remained in ${pending.state}");
    }
    await Future<void>.delayed(Duration.zero);
  }
}

final class _Harness {
  const _Harness(this.peer, this.connection);

  static Future<_Harness> open({int maximumReconnectAttempts = 0}) async {
    final peer = await ScriptedNatsPeer.start();
    final connection = await NatsConnection.connect(
      NatsOptions(
        servers: [NatsServer.parse(peer.uri.toString())],
        reconnect: NatsReconnectPolicy(
          maximumAttempts: maximumReconnectAttempts,
          initialDelay: const Duration(milliseconds: 10),
          baseDelay: const Duration(milliseconds: 10),
          maximumDelay: const Duration(milliseconds: 10),
          jitter: 0,
        ),
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
