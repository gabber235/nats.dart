import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/stream_cluster_admin.dart";
import "package:nats_jetstream/src/stream_peer_admin.dart";
import "package:nats_jetstream/src/wire_response.dart";
import "package:test/test.dart";

void main() {
  test(
    "classifies a clean replacement after one successful mutation",
    () async {
      var readCount = 0;
      var mutationCount = 0;
      String? mutationSubject;
      Map<String, Object?>? mutationBody;
      final admin = _admin(
        readInfo: (_, _) async => readCount++ == 0
            ? _info(leader: "A", followers: const ["B", "C"])
            : _info(leader: "A", followers: const ["C", "D"]),
        requestRemoval: (subject, body, _) async {
          mutationCount += 1;
          mutationSubject = subject;
          mutationBody = body;
          return _response(subject, {"success": true});
        },
      );

      final result = await admin.removePeer(
        "EVENTS",
        "B",
        maximumWait: const Duration(seconds: 1),
        pollInterval: const Duration(microseconds: 1),
      );

      expect(mutationCount, 1);
      expect(mutationSubject, r"$JS.API.STREAM.PEER.REMOVE.EVENTS");
      expect(mutationBody, {"peer": "B"});
      expect(
        result.classification,
        JetStreamPeerRemovalClassification.replaced,
      );
      expect(result.serverReportedSuccess, isTrue);
      expect(result.mutation, isA<JetStreamPeerRemovalAccepted>());
      expect(result.mutationError, isNull);
      expect(result.ambiguityCause, isNull);
      expect(result.removedServer, "B");
      expect(_members(result.lastInfo), {"A", "C", "D"});
    },
  );

  test("preserves 10075 after removal reduced membership", () async {
    var readCount = 0;
    var mutationCount = 0;
    final admin = _admin(
      readInfo: (_, _) async => readCount++ == 0
          ? _info(leader: "A", followers: const ["B", "C"])
          : _info(leader: "A", followers: const ["C"]),
      requestRemoval: (subject, _, _) async {
        mutationCount += 1;
        return _response(subject, {
          "error": {
            "code": 503,
            "err_code": 10075,
            "description": "peer remap failed",
          },
        });
      },
    );

    final result = await admin.removePeer(
      "EVENTS",
      "B",
      maximumWait: const Duration(seconds: 1),
      pollInterval: const Duration(microseconds: 1),
    );

    expect(mutationCount, 1);
    expect(
      result.classification,
      JetStreamPeerRemovalClassification.removedWithoutReplacement,
    );
    expect(result.serverReportedSuccess, isFalse);
    expect(result.mutation, isA<JetStreamPeerRemovalRemapFailed>());
    expect(result.mutationError?.errorCode, 10075);
    expect(result.mutationError?.code, 503);
    expect(result.mutationError?.description, "peer remap failed");
    expect(result.ambiguityCause, isNull);
    expect(_members(result.lastInfo), {"A", "C"});
  });

  test("waits through transient reduction after accepted remap", () async {
    var readCount = 0;
    var mutationCount = 0;
    final observations = [
      _info(leader: "A", followers: const ["B", "C"]),
      _info(leader: "A", followers: const ["C"]),
      _info(leader: "A", followers: const ["C", "D"]),
    ];
    final admin = _admin(
      readInfo: (_, _) async => observations[readCount++],
      requestRemoval: (subject, _, _) async {
        mutationCount += 1;
        return _response(subject, {"success": true});
      },
    );

    final result = await admin.removePeer(
      "EVENTS",
      "B",
      maximumWait: const Duration(seconds: 1),
      pollInterval: const Duration(microseconds: 1),
    );

    expect(mutationCount, 1);
    expect(readCount, 3);
    expect(result.classification, JetStreamPeerRemovalClassification.replaced);
    expect(_members(result.lastInfo), {"A", "C", "D"});
  });

  test("classifies preserved R1 peer after 10075 as unknown", () async {
    var mutationCount = 0;
    final preserved = _info(leader: "A", followers: const [], replicas: 1);
    final admin = _admin(
      readInfo: (_, _) async => preserved,
      requestRemoval: (subject, _, _) async {
        mutationCount += 1;
        return _response(subject, {
          "error": {
            "code": 503,
            "err_code": 10075,
            "description": "peer remap failed",
          },
        });
      },
    );

    final result = await admin.removePeer(
      "EVENTS",
      "A",
      leaderRemoval: JetStreamLeaderRemoval.allow,
      maximumWait: const Duration(milliseconds: 2),
      pollInterval: const Duration(microseconds: 100),
    );

    expect(mutationCount, 1);
    expect(result.classification, JetStreamPeerRemovalClassification.unknown);
    expect(result.mutationError?.errorCode, 10075);
    expect(_members(result.lastInfo), {"A"});
  });

  test("refuses current leader removal before publishing", () async {
    var mutationCount = 0;
    final admin = _admin(
      readInfo: (_, _) async => _info(leader: "A", followers: const ["B", "C"]),
      requestRemoval: (subject, _, _) async {
        mutationCount += 1;
        return _response(subject, {"success": true});
      },
    );

    await expectLater(admin.removePeer("EVENTS", "A"), throwsStateError);

    expect(mutationCount, 0);
  });

  test("rejects a server outside the stream assignment", () async {
    var mutationCount = 0;
    final admin = _admin(
      readInfo: (_, _) async => _info(leader: "A", followers: const ["B", "C"]),
      requestRemoval: (subject, _, _) async {
        mutationCount += 1;
        return _response(subject, {"success": true});
      },
    );

    await expectLater(
      admin.removePeer("EVENTS", "D"),
      throwsA(isA<ArgumentError>()),
    );

    expect(mutationCount, 0);
  });

  test("does not reconcile a definitive peer membership rejection", () async {
    var readCount = 0;
    var mutationCount = 0;
    final admin = _admin(
      readInfo: (_, _) async {
        readCount += 1;
        return _info(leader: "A", followers: const ["B", "C"]);
      },
      requestRemoval: (subject, _, _) async {
        mutationCount += 1;
        return _response(subject, {
          "error": {
            "code": 400,
            "err_code": 10040,
            "description": "peer not a member",
          },
        });
      },
    );

    await expectLater(
      admin.removePeer("EVENTS", "B"),
      throwsA(
        isA<JetStreamApiException>().having(
          (error) => error.errorCode,
          "errorCode",
          10040,
        ),
      ),
    );

    expect(readCount, 1);
    expect(mutationCount, 1);
  });

  test("reconciles a timeout ambiguity without retrying mutation", () async {
    var readCount = 0;
    var mutationCount = 0;
    final timeout = NatsTimeoutException("Request");
    final admin = _admin(
      readInfo: (_, _) async => readCount++ == 0
          ? _info(leader: "A", followers: const ["B", "C"])
          : _info(leader: "A", followers: const ["C", "D"]),
      requestRemoval: (_, _, _) async {
        mutationCount += 1;
        throw timeout;
      },
    );

    final result = await admin.removePeer(
      "EVENTS",
      "B",
      maximumWait: const Duration(seconds: 1),
      pollInterval: const Duration(microseconds: 1),
    );

    expect(mutationCount, 1);
    expect(result.classification, JetStreamPeerRemovalClassification.replaced);
    expect(result.serverReportedSuccess, isFalse);
    expect(result.mutation, isA<JetStreamPeerRemovalAmbiguous>());
    expect(result.ambiguityCause, same(timeout));
    expect(result.mutationError, isNull);
  });
}

JetStreamStreamPeerAdmin _admin({
  required JetStreamStreamInfoRequest readInfo,
  required JetStreamPeerRemovalRequest requestRemoval,
}) => JetStreamStreamPeerAdmin.withRequests(
  apiPrefix: r"$JS.API",
  requestTimeout: const Duration(seconds: 1),
  readInfo: readInfo,
  requestRemoval: requestRemoval,
);

JetStreamWireResponse _response(String subject, Map<String, Object?> body) =>
    JetStreamWireResponse.decode(encodeExactJson(body), subject);

JetStreamStreamInfo _info({
  required String leader,
  required List<String> followers,
  int replicas = 3,
}) => JetStreamStreamInfo(
  config: JetStreamStreamConfig(
    name: "EVENTS",
    storage: JetStreamStorage.memory,
    replicas: replicas,
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
    leaderSince: "2026-09-01T00:00:00Z",
    systemAccount: false,
    trafficAccount: null,
    replicas: followers
        .map(
          (server) => JetStreamPeerInfo(
            name: server,
            current: true,
            offline: false,
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

Set<String> _members(JetStreamStreamInfo info) => {
  info.cluster!.leader!,
  ...info.cluster!.replicas.map((peer) => peer.name),
};
