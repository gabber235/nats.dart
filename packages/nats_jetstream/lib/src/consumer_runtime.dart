import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/consumer_config.dart";
import "package:nats_jetstream/src/consumer_models.dart";
import "package:nats_jetstream/src/consumer_wire.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// Reads and validates the acknowledgement policy for one pull operation.
Future<JetStreamConsumerAckPolicy> readPullConsumerAckPolicy(
  NatsConnection connection,
  String apiPrefix,
  String stream,
  String consumer,
  Duration timeout,
) async {
  final subject = "$apiPrefix.CONSUMER.INFO.$stream.$consumer";
  final message = await connection.request(subject, const [], timeout: timeout);
  final response = JetStreamWireResponse.decode(message.payload, subject)
    ..throwApiError();
  final info = requireConsumerIdentity(
    response,
    decodeConsumerInfo(response),
    expectedStream: stream,
    expectedConsumer: consumer,
  );
  if (info.config.kind != JetStreamConsumerKind.pull) {
    throw JetStreamProtocolException(
      "Pull operation resolved a push consumer",
      subject: subject,
      fieldPath: "config.deliver_subject",
    );
  }
  return info.config.ackPolicy;
}
