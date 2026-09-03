import "dart:async";

import "package:nats_core/nats_core.dart";
import "package:nats_core/src/connection/subscription_state.dart";
import "package:test/test.dart";

void main() {
  test("message bound produces one terminal slow consumer error", () async {
    var overflows = 0;
    late final SubscriptionState state;
    state = SubscriptionState(
      id: 7,
      subject: "events",
      queue: null,
      maximumMessages: 1,
      maximumBytes: 1024,
      replayOnReconnect: true,
      onCancel: () async {},
      onOverflow: (error) {
        overflows += 1;
        state.close(error);
      },
    );

    state
      ..enqueue(NatsMessage(subject: "events", payload: const [1]))
      ..enqueue(NatsMessage(subject: "events", payload: const [2]))
      ..enqueue(NatsMessage(subject: "events", payload: const [3]));

    expect(overflows, 1);
    await expectLater(
      state.stream,
      emitsError(isA<NatsSlowConsumerException>()),
    );
  });

  test("retained byte bound includes message metadata", () async {
    final errors = <NatsSlowConsumerException>[];
    late final SubscriptionState state;
    state = SubscriptionState(
      id: 8,
      subject: "events",
      queue: null,
      maximumMessages: 100,
      maximumBytes: 2,
      replayOnReconnect: true,
      onCancel: () async {},
      onOverflow: (error) {
        errors.add(error);
        state.close(error);
      },
    );

    state.enqueue(NatsMessage(subject: "events", payload: const [1, 2, 3]));

    expect(errors, hasLength(1));
    await expectLater(state.stream, emitsError(same(errors.single)));
  });

  test("paused delivery retains order until resume", () async {
    final state = SubscriptionState(
      id: 9,
      subject: "events",
      queue: null,
      maximumMessages: 3,
      maximumBytes: 64,
      replayOnReconnect: true,
      onCancel: () async {},
      onOverflow: (error) => fail("Unexpected overflow: $error"),
    );
    final received = <int>[];
    final subscription = state.stream.listen(
      (message) => received.add(message.payload.single),
    )..pause();

    state
      ..enqueue(NatsMessage(subject: "events", payload: const [1]))
      ..enqueue(NatsMessage(subject: "events", payload: const [2]));
    await Future<void>.delayed(Duration.zero);
    expect(received, isEmpty);

    subscription.resume();
    await Future<void>.delayed(Duration.zero);
    await Future<void>.delayed(Duration.zero);
    expect(received, [1, 2]);
    await subscription.cancel();
  });

  test("drain waits for retained messages and stream completion", () async {
    final state = SubscriptionState(
      id: 10,
      subject: "events",
      queue: null,
      maximumMessages: 3,
      maximumBytes: 64,
      replayOnReconnect: true,
      onCancel: () async {},
      onOverflow: (error) => fail("Unexpected overflow: $error"),
    );
    final received = <int>[];
    final streamDone = Completer<void>();
    final subscription = state.stream.listen(
      (message) => received.add(message.payload.single),
      onDone: streamDone.complete,
    )..pause();
    state
      ..enqueue(NatsMessage(subject: "events", payload: const [1]))
      ..enqueue(NatsMessage(subject: "events", payload: const [2]));

    var drained = false;
    final drain = state.drain().then((_) => drained = true);
    await Future<void>.delayed(Duration.zero);
    expect(drained, isFalse);

    subscription.resume();
    await drain;
    await streamDone.future;
    expect(received, [1, 2]);
    expect(drained, isTrue);
    await state.done;
    await subscription.cancel();
  });

  test(
    "drain without a listener discards retained messages and settles",
    () async {
      final state = _state(11)
        ..enqueue(NatsMessage(subject: "events", payload: const [1]));

      await state.drain().timeout(const Duration(seconds: 1));

      expect(state.queuedMessages, 0);
      await state.done.timeout(const Duration(seconds: 1));
    },
  );

  test("normal close settles done without a listener", () async {
    final state = _state(12)..close();

    await state.done.timeout(const Duration(seconds: 1));
  });

  test(
    "unsubscribe without a listener settles the public subscription",
    () async {
      final state = _state(13);
      final subscription = CoreNatsSubscription(
        state,
        () async => state.close(),
        (_) async {},
        (_) => state.drain(),
      );

      await subscription.unsubscribe();

      await subscription.done.timeout(const Duration(seconds: 1));
    },
  );

  test("error close preserves terminal result without a listener", () async {
    final state = _state(14);
    const error = NatsConnectionException("terminal subscription failure");

    state.close(error);

    await expectLater(
      state.done.timeout(const Duration(seconds: 1)),
      throwsA(same(error)),
    );
    await expectLater(state.stream, emitsError(same(error)));
  });

  test("automatic unsubscribe accepts exactly its total allowance", () async {
    var maximumReached = 0;
    late final SubscriptionState state;
    state = SubscriptionState(
      id: 15,
      subject: "events",
      queue: null,
      maximumMessages: 4,
      maximumBytes: 64,
      maximumDeliveries: 2,
      replayOnReconnect: true,
      onCancel: () async {},
      onOverflow: (error) => fail("Unexpected overflow: $error"),
      onMaximumDeliveries: () {
        maximumReached += 1;
        state.drain().ignore();
      },
    );
    final received = <int>[];
    final listener = state.stream.listen(
      (message) => received.add(message.payload.single),
    );

    state
      ..enqueue(NatsMessage(subject: "events", payload: const [1]))
      ..enqueue(NatsMessage(subject: "events", payload: const [2]))
      ..enqueue(NatsMessage(subject: "events", payload: const [3]));

    await state.done;
    expect(received, [1, 2]);
    expect(maximumReached, 1);
    expect(state.receivedMessages, 2);
    expect(state.remainingDeliveries, 0);
    expect(state.queuedMessages, 0);
    await listener.cancel();
  });

  test("automatic unsubscribe derives each generation absolute limit", () {
    final state =
        SubscriptionState(
            id: 16,
            subject: "events",
            queue: null,
            maximumMessages: 8,
            maximumBytes: 128,
            maximumDeliveries: 5,
            replayOnReconnect: true,
            onCancel: () async {},
            onOverflow: (error) => fail("Unexpected overflow: $error"),
          )
          ..enqueue(NatsMessage(subject: "events", payload: const [1]))
          ..enqueue(NatsMessage(subject: "events", payload: const [2]));
    expect(state.serverGenerationMaximumDeliveries, 5);

    // ignore: cascade_invocations
    state.beginConnectionGeneration();
    expect(state.remainingDeliveries, 3);
    expect(state.serverGenerationMaximumDeliveries, 3);
    state.enqueue(NatsMessage(subject: "events", payload: const [3]));
    expect(state.serverGenerationMaximumDeliveries, 3);
  });

  test("dynamic automatic limit cannot undercut accepted deliveries", () {
    final state =
        SubscriptionState(
            id: 17,
            subject: "events",
            queue: null,
            maximumMessages: 8,
            maximumBytes: 128,
            replayOnReconnect: true,
            onCancel: () async {},
            onOverflow: (error) => fail("Unexpected overflow: $error"),
          )
          ..enqueue(NatsMessage(subject: "events", payload: const [1]))
          ..enqueue(NatsMessage(subject: "events", payload: const [2]));

    expect(state.setMaximumDeliveries(5), isFalse);
    expect(state.serverGenerationMaximumDeliveries, 5);
    expect(() => state.setMaximumDeliveries(1), throwsArgumentError);
  });
}

SubscriptionState _state(int id) => SubscriptionState(
  id: id,
  subject: "events",
  queue: null,
  maximumMessages: 3,
  maximumBytes: 64,
  replayOnReconnect: true,
  onCancel: () async {},
  onOverflow: (error) => fail("Unexpected overflow: $error"),
);
