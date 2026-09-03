import "dart:async";

import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/stream_cluster_admin.dart";
import "package:nats_jetstream/src/wire_response.dart";
import "package:test/test.dart";

void main() {
  group("leader selection", () {
    test("encodes any, preferred, and placement requests exactly", () {
      final sourceTags = ["cloud:aws", "region:eu"];
      final placement = JetStreamLeaderPlacement(
        cluster: "C1",
        tags: sourceTags,
      );
      sourceTags.add("mutated");

      expect(const JetStreamAnyLeader().toJson(), isEmpty);
      expect(JetStreamPreferredLeader("B").toJson(), {
        "placement": {"preferred": "B"},
      });
      expect(placement.toJson(), {
        "placement": {
          "cluster": "C1",
          "tags": ["cloud:aws", "region:eu"],
        },
      });
      expect(() => placement.tags.add("forbidden"), throwsUnsupportedError);
    });

    test("rejects empty and ambiguous selectors", () {
      expect(() => JetStreamPreferredLeader(""), throwsArgumentError);
      expect(() => JetStreamPreferredLeader(" B"), throwsArgumentError);
      expect(JetStreamLeaderPlacement.new, throwsArgumentError);
      expect(() => JetStreamLeaderPlacement(tags: [""]), throwsArgumentError);
    });
  });

  test(
    "sends one exact mutation and returns the prior topology receipt",
    () async {
      var mutationCount = 0;
      String? mutationSubject;
      Map<String, Object?>? mutationBody;
      final selection = JetStreamPreferredLeader("B");
      final admin = _admin(
        readInfo: (_, _) async => _info(
          leader: "A",
          leaderSince: "2026-09-01T00:00:00Z",
          replicas: const [
            _Replica("B", current: true, offline: false),
            _Replica("C", current: true, offline: false),
          ],
        ),
        requestStepDown: (subject, body, _) async {
          mutationCount += 1;
          mutationSubject = subject;
          mutationBody = body;
          return _response(subject, {"success": true});
        },
      );

      final receipt = await admin.requestLeaderStepDown(
        "EVENTS",
        selection: selection,
      );

      expect(mutationCount, 1);
      expect(mutationSubject, r"$JS.API.STREAM.LEADER.STEPDOWN.EVENTS");
      expect(mutationBody, {
        "placement": {"preferred": "B"},
      });
      expect(receipt.stream, "EVENTS");
      expect(receipt.previousLeader, "A");
      expect(receipt.previousLeaderSince, "2026-09-01T00:00:00Z");
      expect(receipt.selection, same(selection));
    },
  );

  test("does not retry a mutation whose response is not successful", () async {
    var mutationCount = 0;
    final admin = _admin(
      readInfo: (_, _) async => _clusteredInfo(),
      requestStepDown: (subject, _, _) async {
        mutationCount += 1;
        return _response(subject, {"success": false});
      },
    );

    await expectLater(
      admin.requestLeaderStepDown("EVENTS"),
      throwsA(isA<JetStreamProtocolException>()),
    );
    expect(mutationCount, 1);
  });

  test("does not retry an ambiguous mutation timeout", () async {
    var mutationCount = 0;
    final admin = _admin(
      readInfo: (_, _) async => _clusteredInfo(),
      requestStepDown: (_, _, _) async {
        mutationCount += 1;
        throw TimeoutException("reply lost after publication");
      },
    );

    await expectLater(
      admin.requestLeaderStepDown("EVENTS"),
      throwsA(isA<TimeoutException>()),
    );
    expect(mutationCount, 1);
  });

  test("rejects an unavailable preferred replica before mutation", () async {
    var mutationCount = 0;
    final admin = _admin(
      readInfo: (_, _) async => _info(
        leader: "A",
        replicas: const [
          _Replica("B", current: false, offline: true),
          _Replica("C", current: true, offline: false),
        ],
      ),
      requestStepDown: (subject, _, _) async {
        mutationCount += 1;
        return _response(subject, {"success": true});
      },
    );

    await expectLater(
      admin.requestLeaderStepDown(
        "EVENTS",
        selection: JetStreamPreferredLeader("B"),
      ),
      throwsStateError,
    );
    expect(mutationCount, 0);
  });

  test(
    "waits for the exact preferred leader without repeating mutation",
    () async {
      var readCount = 0;
      var mutationCount = 0;
      final observations = [
        _clusteredInfo(),
        _info(
          leader: "C",
          replicas: const [
            _Replica("A", current: true, offline: false),
            _Replica("B", current: true, offline: false),
          ],
        ),
        _info(
          leader: "B",
          replicas: const [
            _Replica("A", current: true, offline: false),
            _Replica("C", current: true, offline: false),
          ],
        ),
      ];
      final admin = _admin(
        readInfo: (_, _) async => observations[readCount++],
        requestStepDown: (subject, _, _) async {
          mutationCount += 1;
          return _response(subject, {"success": true});
        },
      );

      final converged = await admin.stepDownLeader(
        "EVENTS",
        selection: JetStreamPreferredLeader("B"),
        maximumWait: const Duration(seconds: 1),
        pollInterval: const Duration(microseconds: 1),
      );

      expect(converged.cluster?.leader, "B");
      expect(readCount, 3);
      expect(mutationCount, 1);
    },
  );
}

JetStreamStreamClusterAdmin _admin({
  required JetStreamStreamInfoRequest readInfo,
  required JetStreamLeaderStepDownRequest requestStepDown,
}) => JetStreamStreamClusterAdmin.withRequests(
  apiPrefix: r"$JS.API",
  requestTimeout: const Duration(seconds: 1),
  readInfo: readInfo,
  requestStepDown: requestStepDown,
);

JetStreamWireResponse _response(String subject, Map<String, Object?> body) =>
    JetStreamWireResponse.decode(encodeExactJson(body), subject);

JetStreamStreamInfo _clusteredInfo() => _info(
  leader: "A",
  replicas: const [
    _Replica("B", current: true, offline: false),
    _Replica("C", current: true, offline: false),
  ],
);

JetStreamStreamInfo _info({
  required String leader,
  String? leaderSince,
  required List<_Replica> replicas,
}) => JetStreamStreamInfo(
  config: JetStreamStreamConfig(
    name: "EVENTS",
    storage: JetStreamStorage.memory,
  ),
  created: "2026-09-01T00:00:00Z",
  state: JetStreamStreamState(
    messages: BigInt.zero,
    bytes: BigInt.zero,
    firstSequence: BigInt.zero,
    firstTimestamp: "0001-01-01T00:00:00Z",
    lastSequence: BigInt.zero,
    lastTimestamp: "0001-01-01T00:00:00Z",
    consumerCount: BigInt.zero,
    deleted: const [],
    deletedCount: BigInt.zero,
    subjectCount: BigInt.zero,
    subjects: const {},
    raw: const {},
  ),
  cluster: JetStreamClusterInfo(
    name: "C1",
    raftGroup: "R1",
    leader: leader,
    leaderSince: leaderSince,
    systemAccount: false,
    trafficAccount: null,
    replicas: replicas
        .map(
          (peer) => JetStreamPeerInfo(
            name: peer.name,
            current: peer.current,
            offline: peer.offline,
            activeNanoseconds: BigInt.zero,
            lag: BigInt.zero,
            raw: const {},
          ),
        )
        .toList(growable: false),
    raw: const {},
  ),
  mirror: null,
  sources: const [],
  timestamp: "2026-09-01T00:00:01Z",
  raw: const {},
);

final class _Replica {
  const _Replica(this.name, {required this.current, required this.offline});

  final String name;
  final bool current;
  final bool offline;
}
