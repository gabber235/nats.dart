import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/publish_options.dart";

/// Applies typed publish [options] without overwriting caller [headers].
NatsHeaders? applyJetStreamPublishOptions(
  NatsHeaders? headers,
  JetStreamPublishOptions? options,
) {
  if (options == null) {
    return headers;
  }
  var result = headers;
  result = setOptionalJetStreamHeader(result, "Nats-Msg-Id", options.messageId);
  result = setOptionalJetStreamHeader(
    result,
    "Nats-Expected-Stream",
    options.expectedStream,
  );
  result = setOptionalJetStreamHeader(
    result,
    "Nats-Expected-Last-Sequence",
    options.expectedLastSequence?.toString(),
  );
  result = setOptionalJetStreamHeader(
    result,
    "Nats-Expected-Last-Subject-Sequence",
    options.expectedLastSubjectSequence?.toString(),
  );
  result = setOptionalJetStreamHeader(
    result,
    "Nats-Expected-Last-Subject-Sequence-Subject",
    options.expectedLastSubjectSequenceSubject,
  );
  result = setOptionalJetStreamHeader(
    result,
    "Nats-TTL",
    options.lifetime.wireValue,
  );
  return setOptionalJetStreamHeader(
    result,
    "Nats-Expected-Last-Msg-Id",
    options.expectedLastMessageId,
  );
}

/// Sets one optional managed header without overwriting caller state.
NatsHeaders? setOptionalJetStreamHeader(
  NatsHeaders? headers,
  String name,
  String? value,
) {
  return value == null
      ? headers
      : setJetStreamManagedHeader(headers, name, value);
}

/// Sets one managed header and rejects ambiguous caller ownership.
NatsHeaders setJetStreamManagedHeader(
  NatsHeaders? headers,
  String name,
  String value,
) {
  if (headers?.first(name) != null) {
    throw ArgumentError("Header $name is managed by the JetStream operation");
  }
  return (headers ?? NatsHeaders()).set(name, value);
}
