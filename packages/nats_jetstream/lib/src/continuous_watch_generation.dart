// ignore_for_file: public_member_api_docs

import "dart:async";
import "dart:math";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/consumer_config.dart";
import "package:nats_jetstream/src/consumer_management.dart";
import "package:nats_jetstream/src/consumer_push_config.dart";
import "package:nats_jetstream/src/continuous_delivery_capacity.dart";
import "package:nats_jetstream/src/continuous_watch.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/expiring_ownership_set.dart";
import "package:nats_jetstream/src/lifecycle_signal.dart";
import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/stream_manager.dart";

/// Owns Core and server resources for every generation of one watch.
final class JetStreamContinuousWatchGenerationOwner {
  /// Creates a generation owner.
  JetStreamContinuousWatchGenerationOwner(
    this.connection,
    this.apiPrefix,
    this.timeout,
    this.options,
  );

  final NatsConnection connection;
  final String apiPrefix;
  final Duration timeout;
  final JetStreamContinuousWatchOptions options;

  final JetStreamLifecycleSignal _lifecycle = JetStreamLifecycleSignal();
  final JetStreamExpiringOwnershipSet _staleConsumers =
      JetStreamExpiringOwnershipSet(const Duration(seconds: 5));
  NatsSubscription? _activeSubscription;
  String? _activeConsumer;
  Future<void>? _closeFuture;

  /// Maps the public initial mode to one consumer start position.
  Future<JetStreamConsumerStart> initialStart() async {
    return switch (options.start.kind) {
      JetStreamContinuousWatchStartKind.current =>
        const JetStreamConsumerStart.lastPerSubject(),
      JetStreamContinuousWatchStartKind.history =>
        const JetStreamConsumerStart.all(),
      JetStreamContinuousWatchStartKind.revision =>
        JetStreamConsumerStart.sequence(options.start.revision!),
      JetStreamContinuousWatchStartKind.updates => await _updatesStart(),
    };
  }

  Future<JetStreamConsumerStart> _updatesStart() async {
    final state = await JetStreamStreamManager(
      connection,
      apiPrefix,
      timeout,
    ).info(options.stream);
    return state.lastSequence == BigInt.zero
        ? const JetStreamConsumerStart.newMessages()
        : JetStreamConsumerStart.sequence(state.lastSequence + BigInt.one);
  }

  /// Opens one generation scoped subscription and push consumer.
  Future<JetStreamContinuousWatchGeneration> open(
    JetStreamConsumerStart start,
  ) async {
    final generation = connection.generation;
    final inbox = connection.newInbox();
    final consumer = "W${_randomToken(21)}";
    NatsSubscription? subscription;
    try {
      subscription = await connection.subscribe(
        inbox,
        options: NatsSubscriptionOptions(
          maximumMessages: continuousDeliveryCoreMaximumMessages(
            options.maximumPendingMessages,
          ),
          maximumBytes: continuousDeliveryCoreMaximumBytes(
            options.maximumPendingBytes,
          ),
          reconnect: NatsSubscriptionReconnectPolicy.close,
        ),
      );
      _activeSubscription = subscription;
      await connection.flush(timeout: timeout);
      if (!connection.isConnected || connection.generation != generation) {
        throw NatsConnectionException(
          "Connection changed before creating ${options.label} consumer",
        );
      }
      _staleConsumers.add(consumer);
      final info = await _manager.createPush(
        options.stream,
        JetStreamPushConsumerConfig(
          name: consumer,
          deliverSubject: inbox,
          durable: false,
          start: start,
          filters: options.filters,
          ackPolicy: JetStreamConsumerAckPolicy.none,
          maximumDeliveries: 1,
          inactiveThreshold: JetStreamNanoseconds.fromDuration(
            const Duration(seconds: 5),
          ),
          memoryStorage: true,
          replicas: 1,
          flowControl: true,
          idleHeartbeat: JetStreamNanoseconds.fromDuration(options.heartbeat),
        ),
      );
      _activeConsumer = consumer;
      if (!connection.isConnected || connection.generation != generation) {
        throw NatsConnectionException(
          "Connection changed while creating ${options.label} consumer",
        );
      }
      _staleConsumers.remove(consumer);
      return JetStreamContinuousWatchGeneration(
        generation,
        consumer,
        subscription,
        info.pending,
      );
    } on Object catch (error, stackTrace) {
      try {
        await subscription?.unsubscribe();
      } on Object {
        // The opening failure remains primary. Local closure already ran.
      }
      if (_staleConsumers.contains(consumer)) {
        try {
          await _deleteConsumer(consumer);
        } on Object {
          // The opening failure remains primary. Cleanup retries on reconnect.
        }
      }
      if (_activeConsumer == consumer) {
        _activeConsumer = null;
      }
      if (identical(_activeSubscription, subscription)) {
        _activeSubscription = null;
      }
      Error.throwWithStackTrace(error, stackTrace);
    }
  }

  /// Closes one generation and deletes its consumer when possible.
  Future<void> closeGeneration(
    JetStreamContinuousWatchGeneration opened,
  ) async {
    if (!connection.isConnected) {
      _staleConsumers.add(opened.consumer);
      await _clearGeneration(opened);
      return;
    }
    try {
      await _deleteConsumer(opened.consumer);
    } on NatsException {
      // Reconnect owns transport cleanup recovery.
    } finally {
      await _clearGeneration(opened);
    }
  }

  Future<void> _clearGeneration(
    JetStreamContinuousWatchGeneration opened,
  ) async {
    await opened.subscription.unsubscribe();
    if (identical(_activeSubscription, opened.subscription)) {
      _activeSubscription = null;
    }
    if (_activeConsumer == opened.consumer) {
      _activeConsumer = null;
    }
  }

  /// Waits for a ready generation or throws terminal close.
  Future<bool> waitUntilConnected({Duration? maximumWait}) async {
    return _lifecycle.waitUntilConnected(connection, maximumWait: maximumWait);
  }

  /// Deletes consumers left by disconnected generations.
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

  /// Stops active delivery and cleans every owned consumer possible.
  Future<void> close() {
    _lifecycle.cancel();
    return _closeFuture ??= _close();
  }

  Future<void> _close() async {
    if (_activeConsumer case final consumer?) {
      if (connection.isConnected) {
        try {
          await _deleteConsumer(consumer);
        } on NatsException {
          // The reconnect cleanup path below owns this consumer.
        }
      } else {
        _staleConsumers.add(consumer);
      }
      _activeConsumer = null;
    }
    await _activeSubscription?.unsubscribe();
    _activeSubscription = null;
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
      await _manager.delete(options.stream, consumer);
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

  JetStreamConsumerManager get _manager =>
      JetStreamConsumerManager(connection, apiPrefix, timeout);
}

/// Resources for one ready watch generation.
final class JetStreamContinuousWatchGeneration {
  const JetStreamContinuousWatchGeneration(
    this.generation,
    this.consumer,
    this.subscription,
    this.initialPending,
  );

  final int generation;
  final String consumer;
  final NatsSubscription subscription;
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
