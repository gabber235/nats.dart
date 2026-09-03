import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/consumer_config.dart";
import "package:nats_jetstream/src/continuous_watch.dart";
import "package:nats_jetstream/src/continuous_watch_session.dart";
import "package:nats_jetstream/src/object_store_protocol.dart";
import "package:nats_jetstream/src/object_store_stream.dart";
import "package:nats_jetstream/src/object_store_watch.dart";
import "package:nats_jetstream/src/object_store_watch_codec.dart";

/// Object Store adapter over the shared continuous watch lifecycle.
final class JetStreamObjectWatchSession implements JetStreamObjectWatch {
  /// Creates a lazy session over one verified Object Store bucket.
  JetStreamObjectWatchSession(
    NatsConnection connection,
    String apiPrefix,
    Duration timeout,
    String bucket,
    JetStreamObjectWatchOptions options,
  ) : _session = JetStreamContinuousWatchSession(
        connection: connection,
        apiPrefix: apiPrefix,
        timeout: timeout,
        options: JetStreamContinuousWatchOptions(
          stream: objectStreamName(bucket),
          filters: _filters(connection, apiPrefix, timeout, bucket, options),
          start: JetStreamContinuousWatchStart(switch (options.start.kind) {
            JetStreamObjectWatchStartKind.current =>
              JetStreamContinuousWatchStartKind.current,
            JetStreamObjectWatchStartKind.history =>
              JetStreamContinuousWatchStartKind.history,
            JetStreamObjectWatchStartKind.updates =>
              JetStreamContinuousWatchStartKind.updates,
            JetStreamObjectWatchStartKind.revision =>
              JetStreamContinuousWatchStartKind.revision,
          }, options.start.revision),
          maximumPendingMessages: options.maximumPendingMessages,
          maximumPendingBytes: options.maximumPendingBytes,
          heartbeat: options.heartbeat,
          label: "Object Store watch",
        ),
        decode: (delivery) =>
            decodeObjectWatchDelivery(delivery, bucket: bucket),
        include: (value) => !options.ignoreDeletes || !value.info.deleted,
        changedEvent: (value) =>
            JetStreamObjectChanged(value.info, value.encodedBytes),
        initialCompleteEvent: const JetStreamObjectInitialStateComplete(),
      ) {
    events = _session.events;
  }

  final JetStreamContinuousWatchSession<
    JetStreamObjectWatchValue,
    JetStreamObjectWatchEvent
  >
  _session;

  @override
  late final Stream<JetStreamObjectWatchEvent> events;

  @override
  Future<void> get done => _session.done;

  @override
  Future<void> close() => _session.close();
}

JetStreamConsumerFilters _filters(
  NatsConnection connection,
  String apiPrefix,
  Duration timeout,
  String bucket,
  JetStreamObjectWatchOptions options,
) {
  final protocol = JetStreamObjectProtocol(
    connection,
    apiPrefix,
    timeout,
    bucket,
  );
  final names = options.names;
  if (names == null) {
    return JetStreamConsumerFilters.single("\$O.$bucket.M.>");
  }
  final subjects = names.map(protocol.metadataSubject).toList()..sort();
  return subjects.length == 1
      ? JetStreamConsumerFilters.single(subjects.single)
      : JetStreamConsumerFilters.multiple(subjects);
}
