@TestOn("vm")
library;

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:test/test.dart";

import "support/scripted_nats_peer.dart";

void main() {
  test("single stream responses require the requested identity", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final streams = JetStreamContext(harness.connection).streams;
    final operations = <Future<JetStreamStreamInfo> Function()>[
      () => streams.info("EXPECTED"),
      () => streams.create(
        JetStreamStreamConfig(name: "EXPECTED", subjects: const ["events.>"]),
      ),
      () => streams.update(
        JetStreamStreamConfig(name: "EXPECTED", subjects: const ["events.>"]),
      ),
    ];

    for (final operation in operations) {
      final result = operation();
      final request = await harness.peer.nextPublicationMatching(
        r"$JS.API.STREAM.*.EXPECTED",
      );
      await harness.peer.sendMessage(
        request.replySubject!,
        encodeExactJson(_streamInfo("OTHER")),
      );
      await expectLater(result, throwsA(_protocolField("config.name")));
    }
  });

  test("consumer responses require outer and configuration identity", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final consumers = JetStreamContext(harness.connection).consumers;

    final infoResult = consumers.info("EVENTS", "WORKER");
    final infoRequest = await harness.peer.nextPublication(
      r"$JS.API.CONSUMER.INFO.EVENTS.WORKER",
    );
    await harness.peer.sendMessage(
      infoRequest.replySubject!,
      encodeExactJson(_consumerInfo("OTHER", "WORKER", "WORKER")),
    );
    await expectLater(infoResult, throwsA(_protocolField("stream_name")));

    final pullResult = consumers.pull("EVENTS", "WORKER");
    final pullRequest = await harness.peer.nextPublication(
      r"$JS.API.CONSUMER.INFO.EVENTS.WORKER",
    );
    await harness.peer.sendMessage(
      pullRequest.replySubject!,
      encodeExactJson(_consumerInfo("EVENTS", "OTHER", "OTHER")),
    );
    await expectLater(pullResult, throwsA(_protocolField("name")));

    final resetResult = consumers.reset("EVENTS", "WORKER");
    final resetRequest = await harness.peer.nextPublication(
      r"$JS.API.CONSUMER.RESET.EVENTS.WORKER",
    );
    await harness.peer.sendMessage(
      resetRequest.replySubject!,
      encodeExactJson({
        "reset_seq": BigInt.one,
        ..._consumerInfo("EVENTS", "WORKER", "OTHER"),
      }),
    );
    await expectLater(resetResult, throwsA(_protocolField("config.name")));

    final createResult = consumers.createPull(
      "EVENTS",
      JetStreamPullConsumerConfig(name: "WORKER"),
    );
    final createRequest = await harness.peer.nextPublication(
      r"$JS.API.CONSUMER.CREATE.EVENTS.WORKER",
    );
    await harness.peer.sendMessage(
      createRequest.replySubject!,
      encodeExactJson(_consumerInfo("EVENTS", "WORKER", "OTHER")),
    );
    await expectLater(createResult, throwsA(_protocolField("config.name")));
  });

  test(
    "cluster mutations stop after a wrong stream information response",
    () async {
      final harness = await _Harness.open();
      addTearDown(harness.close);
      final streams = JetStreamContext(harness.connection).streams;

      final stepDown = streams.requestLeaderStepDown("EXPECTED");
      final stepDownInfo = await harness.peer.nextPublication(
        r"$JS.API.STREAM.INFO.EXPECTED",
      );
      await harness.peer.sendMessage(
        stepDownInfo.replySubject!,
        encodeExactJson(_streamInfo("OTHER")),
      );
      await expectLater(stepDown, throwsA(_protocolField("config.name")));

      final remove = streams.removePeer("EXPECTED", "S1");
      final removeInfo = await harness.peer.nextPublication(
        r"$JS.API.STREAM.INFO.EXPECTED",
      );
      await harness.peer.sendMessage(
        removeInfo.replySubject!,
        encodeExactJson(_streamInfo("OTHER")),
      );
      await expectLater(remove, throwsA(_protocolField("config.name")));

      expect(
        harness.peer.publications.where(
          (publication) => publication.subject.contains("LEADER.STEPDOWN"),
        ),
        isEmpty,
      );
      expect(
        harness.peer.publications.where(
          (publication) => publication.subject.contains("PEER.REMOVE"),
        ),
        isEmpty,
      );
    },
  );

  test("consumer pages and reset reject schema integer overflow", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final consumers = JetStreamContext(harness.connection).consumers;
    final int64Overflow = BigInt.one << 63;
    final uint64Overflow = BigInt.one << 64;

    final listResult = consumers.listPage("EVENTS");
    final listRequest = await harness.peer.nextPublication(
      r"$JS.API.CONSUMER.LIST.EVENTS",
    );
    await harness.peer.sendMessage(
      listRequest.replySubject!,
      encodeExactJson({
        "total": int64Overflow,
        "offset": BigInt.zero,
        "limit": BigInt.one,
        "consumers": const <Object?>[],
      }),
    );
    await expectLater(listResult, throwsA(_protocolField("total")));

    final namesResult = consumers.namesPage("EVENTS");
    final namesRequest = await harness.peer.nextPublication(
      r"$JS.API.CONSUMER.NAMES.EVENTS",
    );
    await harness.peer.sendMessage(
      namesRequest.replySubject!,
      encodeExactJson({
        "total": BigInt.zero,
        "offset": int64Overflow,
        "limit": BigInt.one,
        "consumers": const <String>[],
      }),
    );
    await expectLater(namesResult, throwsA(_protocolField("offset")));

    final resetResult = consumers.reset("EVENTS", "WORKER");
    final resetRequest = await harness.peer.nextPublication(
      r"$JS.API.CONSUMER.RESET.EVENTS.WORKER",
    );
    await harness.peer.sendMessage(
      resetRequest.replySubject!,
      encodeExactJson({
        "reset_seq": uint64Overflow,
        ..._consumerInfo("EVENTS", "WORKER", "WORKER"),
      }),
    );
    await expectLater(resetResult, throwsA(_protocolField("reset_seq")));
  });

  test("consumer filters are preflighted against stream subjects", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final consumers = JetStreamContext(harness.connection).consumers;

    final rejected = consumers.createPull(
      "EVENTS",
      JetStreamPullConsumerConfig(
        name: "DISJOINT",
        filters: JetStreamConsumerFilters.multiple(const [
          "orders.created",
          "orders.updated",
        ]),
      ),
    );
    final rejectedInfo = await harness.peer.nextPublication(
      r"$JS.API.STREAM.INFO.EVENTS",
    );
    await harness.peer.sendMessage(
      rejectedInfo.replySubject!,
      encodeExactJson(_streamInfo("EVENTS")),
    );
    await expectLater(rejected, throwsArgumentError);
    expect(
      harness.peer.publications.where(
        (publication) => publication.subject.contains("CONSUMER.CREATE"),
      ),
      isEmpty,
    );

    const filters = ["events.created", "events.updated"];
    final accepted = consumers.createPull(
      "EVENTS",
      JetStreamPullConsumerConfig(
        name: "VALID",
        filters: JetStreamConsumerFilters.multiple(filters),
      ),
    );
    final acceptedInfo = await harness.peer.nextPublication(
      r"$JS.API.STREAM.INFO.EVENTS",
    );
    await harness.peer.sendMessage(
      acceptedInfo.replySubject!,
      encodeExactJson(_streamInfo("EVENTS")),
    );
    final create = await harness.peer.nextPublication(
      r"$JS.API.CONSUMER.CREATE.EVENTS.VALID",
    );
    await harness.peer.sendMessage(
      create.replySubject!,
      encodeExactJson(
        _consumerInfo("EVENTS", "VALID", "VALID", filters: filters),
      ),
    );
    expect((await accepted).config.filterSubjects, filters);
  });

  test("adversarial filter preflight never publishes a mutation", () async {
    final harness = await _Harness.open();
    addTearDown(harness.close);
    final consumers = JetStreamContext(harness.connection).consumers;
    final filters = JetStreamConsumerFilters.multiple([
      for (var index = 0; index < 2048; index += 1) "events.*.filter.$index",
    ]);

    final rejected = consumers.createPull(
      "EVENTS",
      JetStreamPullConsumerConfig(name: "BOUNDED", filters: filters),
    );
    final infoRequest = await harness.peer.nextPublication(
      r"$JS.API.STREAM.INFO.EVENTS",
    );
    await harness.peer.sendMessage(
      infoRequest.replySubject!,
      encodeExactJson(
        _streamInfo(
          "EVENTS",
          subjects: [
            for (var index = 0; index < 2048; index += 1)
              "events.$index.filter.$index",
          ],
        ),
      ),
    );

    await expectLater(
      rejected,
      throwsA(
        isA<ArgumentError>().having(
          (error) => error.message,
          "message",
          contains("work exceeds"),
        ),
      ),
    );
    expect(
      harness.peer.publications.where(
        (publication) => publication.subject.contains("CONSUMER.CREATE"),
      ),
      isEmpty,
    );
  });
}

Matcher _protocolField(String field) => isA<JetStreamProtocolException>()
    .having((error) => error.fieldPath, "fieldPath", field);

Map<String, Object?> _streamInfo(
  String name, {
  List<String> subjects = const ["events.>"],
}) => {
  "config": {
    "name": name,
    "subjects": subjects,
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

Map<String, Object?> _consumerInfo(
  String stream,
  String name,
  String configName, {
  List<String> filters = const [],
}) => {
  "stream_name": stream,
  "name": name,
  "created": "2026-09-02T00:00:00Z",
  "config": {
    "name": configName,
    "deliver_policy": "all",
    "ack_policy": "explicit",
    "replay_policy": "instant",
    if (filters.isNotEmpty) "filter_subjects": filters,
  },
  "delivered": {"consumer_seq": BigInt.zero, "stream_seq": BigInt.zero},
  "ack_floor": {"consumer_seq": BigInt.zero, "stream_seq": BigInt.zero},
  "num_ack_pending": BigInt.zero,
  "num_redelivered": BigInt.zero,
  "num_waiting": BigInt.zero,
  "num_pending": BigInt.zero,
  "ts": "2026-09-02T00:00:00Z",
};

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
