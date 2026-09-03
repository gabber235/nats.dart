import "dart:async";

import "package:nats_core/nats_core.dart";
import "package:nats_core/src/connection/request_many_state.dart";
import "package:nats_core/src/connection/request_mux.dart";
import "package:test/test.dart";

void main() {
  test("multiple response options reject invalid deadlines and bounds", () {
    expect(
      () => NatsRequestManyOptions(timeout: Duration.zero),
      throwsArgumentError,
    );
    expect(
      () => NatsRequestManyOptions(stallTimeout: Duration.zero),
      throwsArgumentError,
    );
    expect(
      () => NatsRequestManyOptions(
        timeout: const Duration(seconds: 1),
        stallTimeout: const Duration(seconds: 1),
      ),
      throwsArgumentError,
    );
    expect(
      () => NatsRequestManyOptions(maximumReplies: 0),
      throwsArgumentError,
    );
    expect(
      () => NatsRequestManyOptions(maximumBufferedMessages: 0),
      throwsArgumentError,
    );
    expect(
      () => NatsRequestManyOptions(maximumBufferedBytes: -1),
      throwsArgumentError,
    );
  });

  test("single and multiple requests share one wildcard inbox", () async {
    var inboxOpens = 0;
    late void Function(NatsMessage) route;
    final replies = <String>[];
    final mux = RequestMux(
      openInbox: (_, callback) async {
        inboxOpens += 1;
        route = callback;
      },
      closeInbox: () async {},
      publish: (_, _, {replySubject, headers, expectedEpoch}) async {
        replies.add(replySubject!);
      },
    );

    final singleFuture = mux.request(
      subject: "service.single",
      payload: const [],
      timeout: const Duration(seconds: 30),
      connectionEpoch: 7,
    );
    final many = await mux.requestMany(
      subject: "service.many",
      payload: const [],
      options: NatsRequestManyOptions(maximumReplies: 2),
      connectionEpoch: 7,
      onOverflow: (_) {},
    );
    final manyReplies = many.messages.toList();

    route(_message(replies[0], 1));
    route(_message(replies[1], 2));
    route(_message(replies[1], 3));

    expect((await singleFuture).payload, [1]);
    expect((await manyReplies).map((message) => message.payload.single), [
      2,
      3,
    ]);
    expect(inboxOpens, 1);
    expect(mux.pending, 0);
  });

  test("no responders before any reply is a typed terminal error", () async {
    late void Function(NatsMessage) route;
    late String reply;
    final mux = RequestMux(
      openInbox: (_, callback) async => route = callback,
      closeInbox: () async {},
      publish: (_, _, {replySubject, headers, expectedEpoch}) async =>
          reply = replySubject!,
    );
    final request = await mux.requestMany(
      subject: "service.missing",
      payload: const [],
      options: NatsRequestManyOptions(),
      connectionEpoch: 3,
      onOverflow: (_) {},
    );

    route(
      NatsMessage(
        subject: reply,
        payload: const [],
        headers: NatsHeaders(statusCode: 503),
      ),
    );

    await expectLater(
      request.messages,
      emitsError(
        isA<NatsNoRespondersException>().having(
          (error) => error.subject,
          "subject",
          "service.missing",
        ),
      ),
    );
    expect(mux.pending, 0);
  });

  test("sentinel completes without becoming an application reply", () async {
    final terminal = Completer<void>();
    final state = RequestManyState(
      subject: "service.stream",
      options: NatsRequestManyOptions(completeOnEmptyPayload: true),
      onTerminal: terminal.complete,
      onOverflow: (_) {},
    );
    final replies = state.messages.toList();

    state
      ..add(_message("reply", 1))
      ..add(_message("reply", null));

    await terminal.future;
    expect((await replies).map((message) => message.payload.single), [1]);
  });

  test("stall completion does not replace the absolute deadline", () async {
    final timers = _ManualTimers();
    final terminal = Completer<void>();
    final state = RequestManyState(
      subject: "service.stream",
      options: NatsRequestManyOptions(
        timeout: const Duration(seconds: 10),
        stallTimeout: const Duration(seconds: 1),
      ),
      onTerminal: terminal.complete,
      onOverflow: (_) {},
      timerFactory: timers.create,
    );
    final replies = state.messages.toList();

    state.add(_message("reply", 1));
    final firstStall = timers.single(const Duration(seconds: 1));
    state.add(_message("reply", 2));
    final secondStall = timers.single(const Duration(seconds: 1));

    expect(firstStall.isActive, isFalse);
    expect(timers.single(const Duration(seconds: 10)).isActive, isTrue);
    secondStall.fire();
    await terminal.future;
    expect((await replies).map((message) => message.payload.single), [1, 2]);
  });

  test("first reply waits only on the absolute deadline", () async {
    final timers = _ManualTimers();
    final terminal = Completer<void>();
    final state = RequestManyState(
      subject: "service.stream",
      options: NatsRequestManyOptions(
        timeout: const Duration(seconds: 10),
        stallTimeout: const Duration(seconds: 1),
      ),
      onTerminal: terminal.complete,
      onOverflow: (_) {},
      timerFactory: timers.create,
    );
    final replies = state.messages.toList();

    timers.single(const Duration(seconds: 10)).fire();

    await terminal.future;
    expect(await replies, isEmpty);
    expect(timers.active(const Duration(seconds: 1)), isFalse);
  });

  test("reply buffer overflow reports every discarded message", () async {
    var dropped = 0;
    final state = RequestManyState(
      subject: "service.stream",
      options: NatsRequestManyOptions(maximumBufferedMessages: 1),
      onTerminal: () {},
      onOverflow: (count) => dropped = count,
    );
    final messages = state.messages;

    state
      ..add(_message("reply", 1))
      ..add(_message("reply", 2));

    await expectLater(messages, emitsError(isA<NatsSlowConsumerException>()));
    expect(dropped, 2);
  });

  test("explicit stop removes mux state and discards late replies", () async {
    late void Function(NatsMessage) route;
    late String reply;
    final mux = RequestMux(
      openInbox: (_, callback) async => route = callback,
      closeInbox: () async {},
      publish: (_, _, {replySubject, headers, expectedEpoch}) async =>
          reply = replySubject!,
    );
    final request = await mux.requestMany(
      subject: "service.stream",
      payload: const [],
      options: NatsRequestManyOptions(),
      connectionEpoch: 1,
      onOverflow: (_) {},
    );

    await request.stop();
    route(_message(reply, 9));

    expect(await request.messages.toList(), isEmpty);
    expect(mux.pending, 0);
  });

  test(
    "listener cancellation and connection failure release mux state",
    () async {
      late void Function(NatsMessage) route;
      final mux = RequestMux(
        openInbox: (_, callback) async => route = callback,
        closeInbox: () async {},
        publish: (_, _, {replySubject, headers, expectedEpoch}) async {},
      );
      final cancelled = await mux.requestMany(
        subject: "service.cancelled",
        payload: const [],
        options: NatsRequestManyOptions(),
        connectionEpoch: 1,
        onOverflow: (_) {},
      );
      final listener = cancelled.messages.listen((_) {});

      await listener.cancel();
      expect(mux.pending, 0);

      final failed = await mux.requestMany(
        subject: "service.failed",
        payload: const [],
        options: NatsRequestManyOptions(),
        connectionEpoch: 1,
        onOverflow: (_) {},
      );
      mux.failAll(const NatsClosedException());

      await expectLater(
        failed.messages,
        emitsError(isA<NatsClosedException>()),
      );
      expect(mux.pending, 0);
      expect(route, isNotNull);
    },
  );

  test(
    "maximum reply completion settles done before a messages listener",
    () async {
      final state = RequestManyState(
        subject: "service.stream",
        options: NatsRequestManyOptions(maximumReplies: 1),
        onTerminal: () {},
        onOverflow: (_) {},
      )..add(_message("reply", 1));

      await state.done.timeout(const Duration(seconds: 1));

      expect(state.retainedReplies, 0);
      expect(state.retainedBytes, 0);
      expect(
        (await state.messages.toList()).map(
          (message) => message.payload.single,
        ),
        [1],
      );
    },
  );

  test("total timeout settles done before a messages listener", () async {
    final timers = _ManualTimers();
    final state = RequestManyState(
      subject: "service.stream",
      options: NatsRequestManyOptions(timeout: const Duration(seconds: 10)),
      onTerminal: () {},
      onOverflow: (_) {},
      timerFactory: timers.create,
    )..add(_message("reply", 1));

    timers.single(const Duration(seconds: 10)).fire();
    await state.done.timeout(const Duration(seconds: 1));

    expect(state.retainedReplies, 0);
    expect(state.retainedBytes, 0);
    expect(
      (await state.messages.toList()).map((message) => message.payload.single),
      [1],
    );
  });

  test("stall timeout settles done before a messages listener", () async {
    final timers = _ManualTimers();
    final state = RequestManyState(
      subject: "service.stream",
      options: NatsRequestManyOptions(
        timeout: const Duration(seconds: 10),
        stallTimeout: const Duration(seconds: 1),
      ),
      onTerminal: () {},
      onOverflow: (_) {},
      timerFactory: timers.create,
    )..add(_message("reply", 1));

    timers.single(const Duration(seconds: 1)).fire();
    await state.done.timeout(const Duration(seconds: 1));

    expect(state.retainedReplies, 0);
    expect(state.retainedBytes, 0);
    expect(
      (await state.messages.toList()).map((message) => message.payload.single),
      [1],
    );
  });
}

NatsMessage _message(String subject, int? byte) {
  return NatsMessage(
    subject: subject,
    payload: byte == null ? const [] : [byte],
  );
}

final class _ManualTimers {
  final List<_ManualTimer> _timers = [];

  Timer create(Duration duration, void Function() callback) {
    final timer = _ManualTimer(duration, callback);
    _timers.add(timer);
    return timer;
  }

  _ManualTimer single(Duration duration) {
    return _timers.lastWhere(
      (timer) => timer.duration == duration && timer.isActive,
    );
  }

  bool active(Duration duration) {
    return _timers.any((timer) => timer.duration == duration && timer.isActive);
  }
}

final class _ManualTimer implements Timer {
  _ManualTimer(this.duration, this.callback);

  final Duration duration;
  final void Function() callback;
  bool _active = true;

  @override
  bool get isActive => _active;

  @override
  int get tick => _active ? 0 : 1;

  @override
  void cancel() {
    _active = false;
  }

  void fire() {
    if (!_active) {
      return;
    }
    _active = false;
    callback();
  }
}
