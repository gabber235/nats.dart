// ignore_for_file: public_member_api_docs

import "package:nats_jetstream/src/consumer_config.dart";

/// Initial delivery mode shared by metadata backed JetStream watches.
enum JetStreamContinuousWatchStartKind { current, history, updates, revision }

/// Internal validated watch position.
final class JetStreamContinuousWatchStart {
  const JetStreamContinuousWatchStart(this.kind, [this.revision]);

  final JetStreamContinuousWatchStartKind kind;
  final BigInt? revision;
}

/// Internal protocol and resource bounds for one continuous watch.
final class JetStreamContinuousWatchOptions {
  const JetStreamContinuousWatchOptions({
    required this.stream,
    required this.filters,
    required this.start,
    required this.maximumPendingMessages,
    required this.maximumPendingBytes,
    required this.heartbeat,
    required this.label,
  });

  final String stream;
  final JetStreamConsumerFilters filters;
  final JetStreamContinuousWatchStart start;
  final int maximumPendingMessages;
  final int maximumPendingBytes;
  final Duration heartbeat;
  final String label;
}
