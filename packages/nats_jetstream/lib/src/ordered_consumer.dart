import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/consumer_consume_options.dart";
import "package:nats_jetstream/src/consumer_management.dart";
import "package:nats_jetstream/src/identifiers.dart";
import "package:nats_jetstream/src/ordered_consumer_models.dart";
import "package:nats_jetstream/src/ordered_consumer_session.dart";

export "package:nats_jetstream/src/ordered_consumer_models.dart";

/// Library owned ordered consumer configuration handle.
final class JetStreamOrderedConsumer {
  JetStreamOrderedConsumer._(
    this._connection,
    this._apiPrefix,
    this._timeout,
    this.stream,
    this.options,
  );

  final NatsConnection _connection;
  final String _apiPrefix;
  final Duration _timeout;

  /// Stream from which ordered messages are delivered.
  final String stream;

  /// Creation and recovery policy for every hidden consumer.
  final JetStreamOrderedConsumerOptions options;

  /// Starts one lazy bounded ordered delivery operation.
  ///
  /// The operation owns every ephemeral server consumer it creates. Stop and
  /// drain both remove that hidden state. Sequence gaps, consumer deletion,
  /// leadership change, and Core reconnection recreate from the next stream
  /// sequence without exposing control frames or duplicate deliveries.
  JetStreamOrderedConsumerStream consume({JetStreamConsumeOptions? options}) {
    return JetStreamOrderedConsumerSession(
      connection: _connection,
      apiPrefix: _apiPrefix,
      timeout: _timeout,
      stream: stream,
      orderedOptions: this.options,
      consumeOptions: options ?? JetStreamConsumeOptions(),
    );
  }
}

/// Ordered delivery entry point for consumer management.
extension JetStreamOrderedConsumerManagement on JetStreamConsumerManager {
  /// Creates a local handle that owns all ephemeral server state it uses.
  JetStreamOrderedConsumer ordered(
    String stream, {
    JetStreamOrderedConsumerOptions? options,
  }) {
    validateJetStreamStreamName(stream);
    return JetStreamOrderedConsumer._(
      connection,
      apiPrefix,
      timeout,
      stream,
      options ?? JetStreamOrderedConsumerOptions(),
    );
  }
}
