// ignore_for_file: public_member_api_docs

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/consumer_delivery.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/ordered_consumer_models.dart";

JetStreamOrderedConsumerRecreationReason? orderedConsumerRecoveryReason(
  Object error,
) {
  if (error is JetStreamConsumerHeartbeatException) {
    return JetStreamOrderedConsumerRecreationReason.heartbeatMissed;
  }
  if (error is NatsConnectionException) {
    return JetStreamOrderedConsumerRecreationReason.connectionGenerationChanged;
  }
  if (error is NatsNoRespondersException) {
    return JetStreamOrderedConsumerRecreationReason.noResponders;
  }
  if (error is JetStreamConsumerStatusException && error.code == 503) {
    return JetStreamOrderedConsumerRecreationReason.noResponders;
  }
  if (error is! JetStreamConsumerStatusException || error.code != 409) {
    return null;
  }
  final description = error.description.toLowerCase();
  if (description.contains("consumer deleted")) {
    return JetStreamOrderedConsumerRecreationReason.consumerDeleted;
  }
  if (description.contains("leadership change")) {
    return JetStreamOrderedConsumerRecreationReason.leadershipChanged;
  }
  return null;
}

bool isRecoverableOrderedConsumerOpenFailure(Object error) {
  return error is NatsConnectionException ||
      error is NatsNoRespondersException ||
      error is NatsTimeoutException ||
      error is JetStreamApiException && error.code == 503;
}
