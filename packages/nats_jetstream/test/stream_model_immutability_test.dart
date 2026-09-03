import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  test("stream snapshot DTOs capture mutable caller collections", () {
    final deleted = [BigInt.one];
    final subjects = {"events.a": BigInt.one};
    final replicas = [_peer()];
    final transforms = [_transform()];
    final source = _source(subjectTransforms: transforms);
    final sources = [source];

    final state = _state(deleted: deleted, subjects: subjects);
    final cluster = _cluster(replicas: replicas);
    final info = _info(state: state, cluster: cluster, sources: sources);

    deleted[0] = BigInt.two;
    deleted.add(BigInt.from(3));
    subjects["events.a"] = BigInt.two;
    subjects["events.b"] = BigInt.one;
    replicas.clear();
    transforms.clear();
    sources.clear();

    expect(state.deleted, [BigInt.one]);
    expect(state.subjects, {"events.a": BigInt.one});
    expect(cluster.replicas, hasLength(1));
    expect(source.subjectTransforms, hasLength(1));
    expect(info.sources, [source]);
  });

  test("stream snapshot DTO collections reject every mutation operation", () {
    final state = _state(
      deleted: [BigInt.one],
      subjects: {"events.a": BigInt.one},
    );
    final firstPeer = _peer();
    final cluster = _cluster(replicas: [firstPeer]);
    final firstTransform = _transform();
    final source = _source(subjectTransforms: [firstTransform]);
    final info = _info(state: state, cluster: cluster, sources: [source]);

    _expectUnmodifiableList(
      state.deleted,
      addition: BigInt.two,
      replacement: BigInt.from(3),
    );
    _expectUnmodifiableMap(
      state.subjects,
      additionKey: "events.b",
      additionValue: BigInt.two,
      replacementValue: BigInt.from(3),
    );
    _expectUnmodifiableList(
      cluster.replicas,
      addition: _peer(name: "C"),
      replacement: _peer(name: "D"),
    );
    _expectUnmodifiableList(
      source.subjectTransforms,
      addition: JetStreamSubjectTransform(
        source: "source.b",
        destination: "events.b",
      ),
      replacement: JetStreamSubjectTransform(
        source: "source.c",
        destination: "events.c",
      ),
    );
    _expectUnmodifiableList(
      info.sources,
      addition: _source(name: "SOURCE_B"),
      replacement: _source(name: "SOURCE_C"),
    );
  });

  test("every stream snapshot DTO deeply captures exact raw JSON", () {
    final snapshotters =
        <
          ({
            String name,
            Map<String, Object?> Function(Map<String, Object?>) capture,
          })
        >[
          (name: "state", capture: (raw) => _state(raw: raw).raw),
          (name: "peer", capture: (raw) => _peer(raw: raw).raw),
          (name: "cluster", capture: (raw) => _cluster(raw: raw).raw),
          (name: "source", capture: (raw) => _source(raw: raw).raw),
          (name: "info", capture: (raw) => _info(raw: raw).raw),
        ];

    for (final snapshotter in snapshotters) {
      final nestedObject = <String, Object?>{"value": "original"};
      final nestedList = <Object?>[nestedObject];
      final raw = <String, Object?>{"nested": nestedList};

      final snapshot = snapshotter.capture(raw);
      raw["added"] = true;
      nestedList.add("mutated");
      nestedObject["value"] = "mutated";

      expect(snapshot, {
        "nested": [
          {"value": "original"},
        ],
      }, reason: snapshotter.name);
      _expectUnmodifiableMap(
        snapshot,
        additionKey: "added",
        additionValue: true,
        replacementValue: const <Object?>[],
      );
      final capturedList = snapshot["nested"]! as List<Object?>;
      _expectUnmodifiableList(
        capturedList,
        addition: "added",
        replacement: "replaced",
      );
      final capturedObject = capturedList.single! as Map<String, Object?>;
      _expectUnmodifiableMap(
        capturedObject,
        additionKey: "added",
        additionValue: true,
        replacementValue: "replaced",
      );
    }
  });

  test("page and direct results own immutable collection snapshots", () {
    final info = _info();
    final streamInput = [info];
    final nameInput = ["EVENTS"];
    final message = JetStreamStoredMessage(
      subject: "events.a",
      sequence: BigInt.one,
      payload: const [1],
      timestamp: "2026-09-02T00:00:00Z",
    );
    final messageInput = [message];

    final streamPage = JetStreamStreamPage(
      total: BigInt.one,
      offset: BigInt.zero,
      limit: BigInt.from(1024),
      streams: streamInput,
    );
    final namePage = JetStreamStreamNamePage(
      total: BigInt.one,
      offset: BigInt.zero,
      limit: BigInt.from(1024),
      names: nameInput,
    );
    final batch = JetStreamDirectBatchResult(
      messages: messageInput,
      pending: BigInt.zero,
      lastSequence: BigInt.one,
    );

    streamInput.clear();
    nameInput.clear();
    messageInput.clear();

    expect(streamPage.streams, [info]);
    expect(namePage.names, ["EVENTS"]);
    expect(batch.messages, [message]);
    _expectUnmodifiableList(
      streamPage.streams,
      addition: _info(name: "ADDED"),
      replacement: _info(name: "REPLACED"),
    );
    _expectUnmodifiableList(
      namePage.names,
      addition: "ADDED",
      replacement: "REPLACED",
    );
    _expectUnmodifiableList(
      batch.messages,
      addition: JetStreamStoredMessage(
        subject: "events.b",
        sequence: BigInt.two,
        payload: const [2],
        timestamp: "2026-09-02T00:00:01Z",
      ),
      replacement: JetStreamStoredMessage(
        subject: "events.c",
        sequence: BigInt.from(3),
        payload: const [3],
        timestamp: "2026-09-02T00:00:02Z",
      ),
    );
  });
}

JetStreamStreamState _state({
  List<BigInt> deleted = const [],
  Map<String, BigInt> subjects = const {},
  Map<String, Object?> raw = const {},
}) => JetStreamStreamState(
  messages: BigInt.zero,
  bytes: BigInt.zero,
  firstSequence: BigInt.zero,
  firstTimestamp: "0001-01-01T00:00:00Z",
  lastSequence: BigInt.zero,
  lastTimestamp: "0001-01-01T00:00:00Z",
  consumerCount: BigInt.zero,
  deleted: deleted,
  deletedCount: BigInt.zero,
  subjectCount: BigInt.from(subjects.length),
  subjects: subjects,
  raw: raw,
);

JetStreamPeerInfo _peer({
  String name = "B",
  Map<String, Object?> raw = const {},
}) => JetStreamPeerInfo(
  name: name,
  current: true,
  offline: false,
  activeNanoseconds: BigInt.zero,
  lag: BigInt.zero,
  raw: raw,
);

JetStreamClusterInfo _cluster({
  List<JetStreamPeerInfo> replicas = const [],
  Map<String, Object?> raw = const {},
}) => JetStreamClusterInfo(
  name: "C1",
  raftGroup: "R1",
  leader: "A",
  leaderSince: "2026-09-02T00:00:00Z",
  systemAccount: false,
  trafficAccount: null,
  replicas: replicas,
  raw: raw,
);

JetStreamSubjectTransform _transform() =>
    JetStreamSubjectTransform(source: "source.a", destination: "events.a");

JetStreamStreamSourceInfo _source({
  String name = "SOURCE_A",
  List<JetStreamSubjectTransform> subjectTransforms = const [],
  Map<String, Object?> raw = const {},
}) => JetStreamStreamSourceInfo(
  name: name,
  lag: BigInt.zero,
  activeNanoseconds: BigInt.zero,
  filterSubject: null,
  subjectTransforms: subjectTransforms,
  raw: raw,
);

JetStreamStreamInfo _info({
  String name = "EVENTS",
  JetStreamStreamState? state,
  JetStreamClusterInfo? cluster,
  List<JetStreamStreamSourceInfo> sources = const [],
  Map<String, Object?> raw = const {},
}) => JetStreamStreamInfo(
  config: JetStreamStreamConfig(name: name, storage: JetStreamStorage.memory),
  created: "2026-09-02T00:00:00Z",
  state: state ?? _state(),
  cluster: cluster,
  mirror: null,
  sources: sources,
  timestamp: "2026-09-02T00:00:01Z",
  raw: raw,
);

void _expectUnmodifiableList<T>(
  List<T> values, {
  required T addition,
  required T replacement,
}) {
  expect(() => values.add(addition), throwsUnsupportedError);
  expect(() => values[0] = replacement, throwsUnsupportedError);
  expect(() => values.remove(values.first), throwsUnsupportedError);
}

void _expectUnmodifiableMap<K, V>(
  Map<K, V> values, {
  required K additionKey,
  required V additionValue,
  required V replacementValue,
}) {
  final existingKey = values.keys.first;
  expect(
    () => values.addAll({additionKey: additionValue}),
    throwsUnsupportedError,
  );
  expect(() => values[existingKey] = replacementValue, throwsUnsupportedError);
  expect(() => values.remove(existingKey), throwsUnsupportedError);
}
