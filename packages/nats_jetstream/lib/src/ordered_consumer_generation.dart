// ignore_for_file: public_member_api_docs

import "dart:async";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/consumer_consume_options.dart";
import "package:nats_jetstream/src/consumer_delivery.dart";
import "package:nats_jetstream/src/consumer_stream_session.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/ordered_consumer_cursor.dart";
import "package:nats_jetstream/src/ordered_consumer_models.dart";
import "package:nats_jetstream/src/ordered_consumer_owner.dart";
import "package:nats_jetstream/src/ordered_consumer_recovery.dart";

final class JetStreamOrderedConsumerGenerationDelivery {
  JetStreamOrderedConsumerGenerationDelivery({
    required this.connection,
    required this.stream,
    required this.opened,
    required this.cursor,
    required this.options,
    required this.onDelivery,
    required this.isDeliveryPaused,
    required this.onEvent,
  });

  final NatsConnection connection;
  final String stream;
  final JetStreamOwnedOrderedConsumer opened;
  final JetStreamOrderedConsumerCursor cursor;
  final JetStreamConsumeOptions options;
  final void Function(JetStreamDelivery delivery) onDelivery;
  final bool Function() isDeliveryPaused;
  final void Function(JetStreamOrderedConsumerEvent event) onEvent;

  JetStreamConsumerStream? _consumerStream;
  StreamSubscription<JetStreamDelivery>? _deliveries;
  var _paused = false;

  Future<JetStreamOrderedGenerationOutcome> run() async {
    final requested = Completer<JetStreamOrderedGenerationOutcome>();
    final handle = opened.handle;
    final consumerStream = JetStreamConsumerStreamSession(
      handle.connection,
      handle.apiPrefix,
      handle.stream,
      handle.name,
      options,
      propagateTerminalErrors: false,
    );
    _consumerStream = consumerStream;
    final deliveryTerminated = Completer<void>();
    final connectionEvents = connection.events.listen((event) {
      if (event is NatsDisconnected &&
          event.generation == opened.generation &&
          !requested.isCompleted) {
        requested.complete(
          const JetStreamOrderedGenerationOutcome.recreate(
            JetStreamOrderedConsumerRecreationReason
                .connectionGenerationChanged,
          ),
        );
      }
    });
    final deliveries = consumerStream.messages.listen(
      (delivery) => _inspectDelivery(delivery, requested),
      onError: (Object _, StackTrace _) {
        if (!deliveryTerminated.isCompleted) {
          deliveryTerminated.complete();
        }
      },
      onDone: () {
        if (!deliveryTerminated.isCompleted) {
          deliveryTerminated.complete();
        }
      },
      cancelOnError: false,
    );
    _deliveries = deliveries;
    if (_paused) {
      deliveries.pause();
    }
    final outcome = await Future.any([
      requested.future,
      _streamOutcome(consumerStream),
    ]);
    if (!requested.isCompleted) {
      requested.complete(outcome);
    }
    await _ignoreStopFailure(consumerStream);
    deliveries.resume();
    await deliveryTerminated.future;
    await _ignoreCancelFailure(deliveries);
    await connectionEvents.cancel();
    if (identical(_consumerStream, consumerStream)) {
      _consumerStream = null;
    }
    if (identical(_deliveries, deliveries)) {
      _deliveries = null;
    }
    return outcome;
  }

  void pause() {
    if (_paused) {
      return;
    }
    _paused = true;
    _deliveries?.pause();
  }

  void resume() {
    if (!_paused) {
      return;
    }
    _paused = false;
    _deliveries?.resume();
  }

  Future<void> stop() async {
    final active = _consumerStream;
    if (active != null) {
      await _ignoreStopFailure(active);
    }
  }

  Future<void> drain(Duration timeout) async {
    final active = _consumerStream;
    if (active != null) {
      await active.drain(timeout: timeout);
    }
  }

  void _inspectDelivery(
    JetStreamDelivery delivery,
    Completer<JetStreamOrderedGenerationOutcome> requested,
  ) {
    if (requested.isCompleted) {
      return;
    }
    final metadata = delivery.metadata;
    if (metadata == null) {
      requested.complete(
        JetStreamOrderedGenerationOutcome.failure(
          JetStreamProtocolException(
            "Ordered delivery is missing sequence metadata",
            subject: delivery.message.replySubject,
          ),
          StackTrace.current,
        ),
      );
      return;
    }
    if (metadata.stream != stream) {
      requested.complete(
        JetStreamOrderedGenerationOutcome.failure(
          JetStreamProtocolException(
            "Ordered delivery stream ${metadata.stream} does not match $stream",
            subject: delivery.message.replySubject,
            fieldPath: "stream",
          ),
          StackTrace.current,
        ),
      );
      return;
    }
    if (metadata.consumer != opened.consumer) {
      requested.complete(
        JetStreamOrderedGenerationOutcome.failure(
          JetStreamProtocolException(
            "Ordered delivery consumer ${metadata.consumer} does not match "
            "${opened.consumer}",
            subject: delivery.message.replySubject,
            fieldPath: "consumer",
          ),
          StackTrace.current,
        ),
      );
      return;
    }
    final inspection = cursor.inspect(
      consumer: opened.consumer,
      consumerSequence: metadata.consumerSequence,
      streamSequence: metadata.streamSequence,
    );
    if (inspection.gap case final gap?) {
      onEvent(gap);
    }
    if (inspection.recreationReason case final reason?) {
      requested.complete(JetStreamOrderedGenerationOutcome.recreate(reason));
      return;
    }
    onDelivery(JetStreamDelivery.withoutAcknowledgement(delivery.message));
    pause();
    scheduleMicrotask(() {
      if (_deliveries != null && !isDeliveryPaused()) {
        resume();
      }
    });
  }

  Future<JetStreamOrderedGenerationOutcome> _streamOutcome(
    JetStreamConsumerStream stream,
  ) async {
    try {
      await stream.done;
      return const JetStreamOrderedGenerationOutcome.finished();
    } on Object catch (error, stackTrace) {
      final reason = orderedConsumerRecoveryReason(error);
      return reason == null
          ? JetStreamOrderedGenerationOutcome.failure(error, stackTrace)
          : JetStreamOrderedGenerationOutcome.recreate(reason);
    }
  }

  Future<void> _ignoreStopFailure(JetStreamConsumerStream stream) async {
    try {
      await stream.stop();
    } on Object {
      // The captured generation outcome remains primary.
    }
  }

  Future<void> _ignoreCancelFailure(
    StreamSubscription<JetStreamDelivery> subscription,
  ) async {
    try {
      await subscription.cancel();
    } on Object {
      // The selected generation outcome remains primary.
    }
  }
}

final class JetStreamOrderedGenerationOutcome {
  const JetStreamOrderedGenerationOutcome.finished()
    : reason = null,
      error = null,
      stackTrace = null;

  const JetStreamOrderedGenerationOutcome.recreate(this.reason)
    : error = null,
      stackTrace = null;

  const JetStreamOrderedGenerationOutcome.failure(this.error, this.stackTrace)
    : reason = null;

  final JetStreamOrderedConsumerRecreationReason? reason;
  final Object? error;
  final StackTrace? stackTrace;
}
