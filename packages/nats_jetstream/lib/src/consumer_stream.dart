import "package:nats_jetstream/src/consumer_consume_options.dart";
import "package:nats_jetstream/src/consumer_fetch.dart";
import "package:nats_jetstream/src/consumer_stream_session.dart";

/// Continuous pull delivery for an existing named consumer.
extension JetStreamPullConsumerStream on JetStreamPullConsumer {
  /// Creates a lazy bounded single listener delivery operation.
  JetStreamConsumerStream consume({JetStreamConsumeOptions? options}) {
    return JetStreamConsumerStreamSession(
      connection,
      apiPrefix,
      stream,
      name,
      options ?? JetStreamConsumeOptions(),
    );
  }
}
