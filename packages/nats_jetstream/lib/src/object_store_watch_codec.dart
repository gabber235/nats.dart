// ignore_for_file: public_member_api_docs

import "dart:convert";

import "package:nats_jetstream/src/consumer_delivery.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/object_store.dart";
import "package:nats_jetstream/src/object_store_wire.dart";
import "package:nats_jetstream/src/timestamp.dart";

final class JetStreamObjectWatchValue {
  const JetStreamObjectWatchValue(this.info, this.encodedBytes);

  final JetStreamObjectInfo info;
  final int encodedBytes;
}

JetStreamObjectWatchValue decodeObjectWatchDelivery(
  JetStreamDelivery delivery, {
  required String bucket,
}) {
  final message = delivery.message;
  final metadata = delivery.metadata;
  if (metadata == null) {
    throw JetStreamProtocolException(
      "Object watch delivery is missing JetStream metadata",
      subject: message.subject,
    );
  }
  return decodeObjectWatchMessage(
    subject: message.subject,
    payload: message.payload,
    bucket: bucket,
    streamSequence: metadata.streamSequence,
    timestampNanoseconds: metadata.timestampNanoseconds,
  );
}

JetStreamObjectWatchValue decodeObjectWatchMessage({
  required String subject,
  required List<int> payload,
  required String bucket,
  required BigInt streamSequence,
  required BigInt timestampNanoseconds,
}) {
  final prefix = "\$O.$bucket.M.";
  if (!subject.startsWith(prefix)) {
    throw JetStreamProtocolException(
      "Object watch delivered a subject outside the metadata namespace",
      subject: subject,
    );
  }
  final encodedName = subject.substring(prefix.length);
  late final List<int> nameBytes;
  late final String objectName;
  try {
    nameBytes = base64Url.decode(encodedName);
    objectName = utf8.decode(nameBytes, allowMalformed: false);
  } on FormatException catch (error) {
    throw JetStreamProtocolException(
      "Object metadata subject contains an invalid encoded name",
      subject: subject,
      cause: error,
    );
  }
  if (objectName.isEmpty || base64Url.encode(nameBytes) != encodedName) {
    throw JetStreamProtocolException(
      "Object metadata subject name is not canonical padded base64url",
      subject: subject,
    );
  }
  final info = decodeObjectInfoPayload(
    payload,
    subject: subject,
    expectedBucket: bucket,
    expectedName: objectName,
    modified: jetStreamTimestamp(timestampNanoseconds),
    revision: streamSequence,
  );
  return JetStreamObjectWatchValue(info, payload.length);
}
