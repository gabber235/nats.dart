import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/identifiers.dart";
import "package:nats_jetstream/src/server_duration.dart";
import "package:nats_jetstream/src/wire_integer.dart";

/// Optimistic concurrency and deduplication controls for one publication.
final class JetStreamPublishOptions {
  /// Creates validated publication controls.
  factory JetStreamPublishOptions({
    String? messageId,
    String? expectedStream,
    BigInt? expectedLastSequence,
    BigInt? expectedLastSubjectSequence,
    String? expectedLastSubjectSequenceSubject,
    String? expectedLastMessageId,
    JetStreamMessageLifetime lifetime =
        const JetStreamMessageLifetime.inherit(),
  }) {
    _validateOptionalHeaderText(messageId, "messageId");
    if (expectedStream != null) {
      validateJetStreamStreamName(expectedStream);
    }
    _validateOptionalHeaderText(expectedLastMessageId, "expectedLastMessageId");
    _validateSequence(expectedLastSequence, "expectedLastSequence");
    _validateSequence(
      expectedLastSubjectSequence,
      "expectedLastSubjectSequence",
    );
    if (expectedLastSubjectSequenceSubject != null) {
      if (expectedLastSubjectSequence == null) {
        throw ArgumentError.value(
          expectedLastSubjectSequenceSubject,
          "expectedLastSubjectSequenceSubject",
          "Requires expectedLastSubjectSequence",
        );
      }
      validateSubscriptionSubject(expectedLastSubjectSequenceSubject);
    }
    return JetStreamPublishOptions._(
      messageId,
      expectedStream,
      expectedLastSequence,
      expectedLastSubjectSequence,
      expectedLastSubjectSequenceSubject,
      expectedLastMessageId,
      lifetime,
    );
  }

  const JetStreamPublishOptions._(
    this.messageId,
    this.expectedStream,
    this.expectedLastSequence,
    this.expectedLastSubjectSequence,
    this.expectedLastSubjectSequenceSubject,
    this.expectedLastMessageId,
    this.lifetime,
  );

  /// Deduplication identifier.
  final String? messageId;

  /// Stream expected to receive the message.
  final String? expectedStream;

  /// Expected final sequence for the whole stream.
  final BigInt? expectedLastSequence;

  /// Expected final sequence for one subject.
  final BigInt? expectedLastSubjectSequence;

  /// Subject used by [expectedLastSubjectSequence].
  final String? expectedLastSubjectSequenceSubject;

  /// Expected identifier of the final stored message.
  final String? expectedLastMessageId;

  /// Per message lifetime, inherited from the stream by default.
  final JetStreamMessageLifetime lifetime;
}

/// Validated lifetime for one persisted message.
final class JetStreamMessageLifetime {
  /// Inherits stream age limits without a per message TTL header.
  const JetStreamMessageLifetime.inherit() : wireValue = null;

  /// Prevents expiry through message TTL and stream maximum age rules.
  const JetStreamMessageLifetime.never() : wireValue = "never";

  /// Expires the message after a positive whole second duration.
  factory JetStreamMessageLifetime.expiresAfter(Duration duration) {
    if (duration < const Duration(seconds: 1) ||
        duration.inMicroseconds % Duration.microsecondsPerSecond != 0) {
      throw ArgumentError.value(
        duration,
        "duration",
        "Must be a positive whole second duration",
      );
    }
    jetStreamServerDurationNanoseconds(duration, "duration");
    return JetStreamMessageLifetime._("${duration.inSeconds}s");
  }

  const JetStreamMessageLifetime._(this.wireValue);

  /// Exact `Nats-TTL` header value, or `null` when inherited.
  final String? wireValue;
}

void _validateOptionalHeaderText(String? value, String name) {
  if (value == null) {
    return;
  }
  if (value.isEmpty) {
    throw ArgumentError.value(value, name, "Must not be empty");
  }
  NatsHeaders(entries: [MapEntry("Validation", value)]);
}

void _validateSequence(BigInt? value, String name) {
  if (value == null) {
    return;
  }
  validateJetStreamUint64(value, name);
}
