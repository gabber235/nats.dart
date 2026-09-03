import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/fast_batch.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// One decoded fast batch control response.
sealed class FastBatchControl {
  /// Creates a control response.
  const FastBatchControl();
}

/// Cumulative persistence and dynamic flow evidence.
final class FastBatchFlowAck extends FastBatchControl {
  /// Creates a flow acknowledgement.
  const FastBatchFlowAck(this.sequence, this.messages);

  /// Highest covered batch sequence.
  final BigInt sequence;

  /// Current acknowledgement interval.
  final int messages;
}

/// Informational missing sequence range.
final class FastBatchGap extends FastBatchControl {
  /// Creates a gap response.
  const FastBatchGap(this.value);

  /// Typed gap.
  final JetStreamFastBatchGap value;
}

/// Per message expectation rejection.
final class FastBatchError extends FastBatchControl {
  /// Creates an error response.
  const FastBatchError(this.value);

  /// Typed message error.
  final JetStreamFastBatchMessageError value;
}

/// Final standard JetStream publication acknowledgement.
final class FastBatchFinalAck extends FastBatchControl {
  /// Creates a final acknowledgement.
  const FastBatchFinalAck({
    required this.stream,
    required this.sequence,
    required this.batchId,
    required this.messageCount,
    required this.domain,
  });

  /// Stream that accepted the batch.
  final String stream;

  /// Final assigned stream sequence.
  final BigInt sequence;

  /// Server echoed batch identifier.
  final String batchId;

  /// Highest committed logical batch sequence.
  final BigInt messageCount;

  /// JetStream domain, when configured.
  final String? domain;
}

/// Strictly decodes one control or final response.
FastBatchControl decodeFastBatchControl(List<int> payload, String subject) {
  if (payload.length > 64 * 1024) {
    throw JetStreamProtocolException(
      "Fast batch control response exceeds 64 KiB",
      subject: subject,
    );
  }
  final response = JetStreamWireResponse.decode(payload, subject);
  final type = response.optionalString("type");
  switch (type) {
    case "ack":
      final messages = response.requiredBoundedInt(
        "msgs",
        minimum: 1,
        maximum: 65535,
      );
      return FastBatchFlowAck(response.requiredUint64("seq"), messages);
    case "gap":
      final expected = response.requiredUnsigned("last_seq");
      final received = response.requiredPositiveUint64("seq");
      if (expected >= received) {
        throw JetStreamProtocolException(
          "Fast batch gap range is invalid",
          subject: subject,
          fieldPath: "last_seq",
        );
      }
      return FastBatchGap(
        JetStreamFastBatchGap(
          expectedSequence: expected,
          receivedSequence: received,
        ),
      );
    case "err":
      final error = response.requiredObject("error");
      final sequence = response.requiredPositiveUint64("seq");
      return FastBatchError(
        JetStreamFastBatchMessageError(
          batchSequence: sequence,
          error: JetStreamApiException(
            subject: subject,
            code: response.requiredBoundedInt(
              "error.code",
              minimum: 300,
              maximum: 699,
              source: error,
            ),
            errorCode: response.requiredBoundedInt(
              "error.err_code",
              minimum: 0,
              maximum: 65535,
              source: error,
            ),
            description: response.requiredString(
              "error.description",
              source: error,
            ),
          ),
        ),
      );
    case null:
      response.throwApiError();
      return FastBatchFinalAck(
        stream: response.requiredString("stream"),
        sequence: response.requiredPositiveUint64("seq"),
        batchId: response.requiredString("batch"),
        messageCount: response.requiredUnsigned("count"),
        domain: response.optionalString("domain"),
      );
    default:
      throw JetStreamProtocolException(
        "Unknown fast batch control type",
        subject: subject,
        fieldPath: "type",
      );
  }
}
