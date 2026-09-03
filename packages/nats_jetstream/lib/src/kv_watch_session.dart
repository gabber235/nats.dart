import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/consumer_config.dart";
import "package:nats_jetstream/src/continuous_watch.dart";
import "package:nats_jetstream/src/continuous_watch_session.dart";
import "package:nats_jetstream/src/kv.dart";
import "package:nats_jetstream/src/kv_codec.dart";
import "package:nats_jetstream/src/kv_codecs.dart";
import "package:nats_jetstream/src/kv_watch.dart";

/// KV adapter over the shared generation fenced watch lifecycle.
final class JetStreamKeyValueWatchSession implements JetStreamKeyValueWatch {
  /// Creates a lazy session over one verified KV bucket.
  JetStreamKeyValueWatchSession(
    NatsConnection connection,
    String apiPrefix,
    Duration timeout,
    String bucket,
    String backingStreamName,
    String readSubjectPrefix,
    JetStreamKeyValueCodecs codecs,
    JetStreamKeyValueWatchOptions options,
  ) : _session = JetStreamContinuousWatchSession(
        connection: connection,
        apiPrefix: apiPrefix,
        timeout: timeout,
        options: JetStreamContinuousWatchOptions(
          stream: backingStreamName,
          filters: _consumerFilters(readSubjectPrefix, codecs, options.filters),
          start: JetStreamContinuousWatchStart(switch (options.start.kind) {
            JetStreamKeyValueWatchStartKind.current =>
              JetStreamContinuousWatchStartKind.current,
            JetStreamKeyValueWatchStartKind.history =>
              JetStreamContinuousWatchStartKind.history,
            JetStreamKeyValueWatchStartKind.updates =>
              JetStreamContinuousWatchStartKind.updates,
            JetStreamKeyValueWatchStartKind.revision =>
              JetStreamContinuousWatchStartKind.revision,
          }, options.start.revision),
          maximumPendingMessages: options.maximumPendingMessages,
          maximumPendingBytes: options.maximumPendingBytes,
          heartbeat: options.heartbeat,
          label: "KV watch",
        ),
        decode: (delivery) => decodeKeyValueDelivery(
          delivery,
          bucket: bucket,
          subjectPrefix: readSubjectPrefix,
          codecs: codecs,
        ),
        include: (entry) =>
            !options.ignoreDeletes ||
            entry.operation == JetStreamKeyValueOperation.put,
        changedEvent: JetStreamKeyValueChanged.new,
        initialCompleteEvent: const JetStreamKeyValueInitialStateComplete(),
      ) {
    events = _session.events;
  }

  final JetStreamContinuousWatchSession<
    JetStreamKeyValueEntry,
    JetStreamKeyValueWatchEvent
  >
  _session;

  @override
  late final Stream<JetStreamKeyValueWatchEvent> events;

  @override
  Future<void> get done => _session.done;

  @override
  Future<void> close() => _session.close();
}

JetStreamConsumerFilters _consumerFilters(
  String prefix,
  JetStreamKeyValueCodecs codecs,
  List<String> filters,
) {
  final subjects = filters
      .map((filter) {
        final encoded = codecs.encodeKey(filter);
        validateJetStreamKeyValueFilter(encoded);
        return "$prefix$encoded";
      })
      .toList(growable: false);
  return subjects.length == 1
      ? JetStreamConsumerFilters.single(subjects.single)
      : JetStreamConsumerFilters.multiple(subjects);
}
