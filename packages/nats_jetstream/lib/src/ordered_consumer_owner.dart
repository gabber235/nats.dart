// ignore_for_file: public_member_api_docs

import "dart:async";
import "dart:math";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/consumer_config.dart";
import "package:nats_jetstream/src/consumer_fetch.dart";
import "package:nats_jetstream/src/consumer_management.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/expiring_ownership_set.dart";
import "package:nats_jetstream/src/lifecycle_signal.dart";
import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/ordered_consumer_models.dart";

final class JetStreamOrderedConsumerOwner {
  JetStreamOrderedConsumerOwner({
    required this.connection,
    required this.apiPrefix,
    required this.timeout,
    required this.stream,
    required this.options,
  }) : _staleConsumers = JetStreamExpiringOwnershipSet(
         options.inactiveThreshold,
       );

  final NatsConnection connection;
  final String apiPrefix;
  final Duration timeout;
  final String stream;
  final JetStreamOrderedConsumerOptions options;

  final JetStreamLifecycleSignal _lifecycle = JetStreamLifecycleSignal();
  final JetStreamExpiringOwnershipSet _staleConsumers;
  JetStreamOwnedOrderedConsumer? _active;
  Future<void>? _closeFuture;

  Future<JetStreamOwnedOrderedConsumer> open(
    JetStreamConsumerStart start, {
    required Duration requestTimeout,
  }) async {
    final generation = connection.generation;
    final consumer = "O${_randomToken(21)}";
    _staleConsumers.add(consumer);
    try {
      final info = await _manager(requestTimeout).createPull(
        stream,
        JetStreamPullConsumerConfig(
          name: consumer,
          durable: false,
          start: start,
          filters: options.filters,
          ackPolicy: JetStreamConsumerAckPolicy.none,
          replayPolicy: options.replayPolicy,
          maximumDeliveries: -1,
          maximumWaiting: 512,
          inactiveThreshold: JetStreamNanoseconds.fromDuration(
            options.inactiveThreshold,
          ),
          headersOnly: options.headersOnly,
          metadata: {...options.metadata, "nats.dart.owner": "ordered"},
          memoryStorage: true,
          replicas: 1,
        ),
      );
      if (!connection.isConnected || connection.generation != generation) {
        throw const NatsConnectionException(
          "Connection changed while creating ordered consumer",
        );
      }
      final opened = JetStreamOwnedOrderedConsumer(
        consumer: consumer,
        generation: generation,
        handle: JetStreamPullConsumer(connection, apiPrefix, stream, consumer),
        initialPending: info.pending,
      );
      _active = opened;
      _staleConsumers.remove(consumer);
      return opened;
    } on Object catch (error, stackTrace) {
      try {
        await _deleteConsumer(consumer);
      } on Object {
        // Creation failure stays primary. Later cleanup retains ownership.
      }
      Error.throwWithStackTrace(error, stackTrace);
    }
  }

  Future<void> closeGeneration(JetStreamOwnedOrderedConsumer opened) async {
    if (!connection.isConnected) {
      _staleConsumers.add(opened.consumer);
      _clearActive(opened);
      return;
    }
    try {
      await _deleteConsumer(opened.consumer);
    } on NatsException {
      // A later ready generation retries cleanup.
    } finally {
      _clearActive(opened);
    }
  }

  void _clearActive(JetStreamOwnedOrderedConsumer opened) {
    if (identical(_active, opened)) {
      _active = null;
    }
  }

  Future<bool> waitUntilConnected({Duration? maximumWait}) async {
    return _lifecycle.waitUntilConnected(connection, maximumWait: maximumWait);
  }

  Future<void> deleteStaleConsumers() async {
    if (!connection.isConnected) {
      return;
    }
    for (final consumer in _staleConsumers.snapshot()) {
      try {
        await _deleteConsumer(consumer);
      } on NatsException {
        return;
      }
    }
  }

  void cancelWaits() => _lifecycle.cancel();

  Future<void> close() {
    _lifecycle.cancel();
    return _closeFuture ??= _close();
  }

  Future<void> _close() async {
    if (_active case final active?) {
      if (connection.isConnected) {
        try {
          await _deleteConsumer(active.consumer);
        } on NatsException {
          // The reconnect cleanup path below owns this consumer.
        }
      } else {
        _staleConsumers.add(active.consumer);
      }
      _active = null;
    }
    if (_staleConsumers.isNotEmpty && !connection.isConnected) {
      try {
        await waitUntilConnected(maximumWait: timeout);
      } on NatsException {
        return;
      }
    }
    await deleteStaleConsumers();
  }

  Future<void> _deleteConsumer(String consumer) async {
    try {
      await _manager(timeout).delete(stream, consumer);
      _staleConsumers.remove(consumer);
    } on JetStreamApiException catch (error) {
      if (error.errorCode == 10014) {
        _staleConsumers.remove(consumer);
        return;
      }
      _staleConsumers.add(consumer);
      rethrow;
    } on NatsException {
      _staleConsumers.add(consumer);
      rethrow;
    }
  }

  JetStreamConsumerManager _manager(Duration requestTimeout) =>
      JetStreamConsumerManager(connection, apiPrefix, requestTimeout);
}

final class JetStreamOwnedOrderedConsumer {
  const JetStreamOwnedOrderedConsumer({
    required this.consumer,
    required this.generation,
    required this.handle,
    required this.initialPending,
  });

  final String consumer;
  final int generation;
  final JetStreamPullConsumer handle;
  final BigInt initialPending;
}

String _randomToken(int length) {
  const alphabet =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  final random = Random.secure();
  return List.generate(
    length,
    (_) => alphabet[random.nextInt(alphabet.length)],
    growable: false,
  ).join();
}
