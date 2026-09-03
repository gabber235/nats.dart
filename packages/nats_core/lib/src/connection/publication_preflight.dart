import "package:nats_core/src/error.dart";
import "package:nats_core/src/headers.dart";

/// Validates one publication without mutating connection or mux state.
void validatePublicationPreflight({
  required int payloadBytes,
  required NatsHeaders? headers,
  required bool headersSupported,
  required int maximumPayloadBytes,
}) {
  if (headers != null && !headersSupported) {
    throw const NatsHeaderException("Server does not support headers");
  }
  final headerBytes = headers?.encode().length ?? 0;
  final totalBytes = payloadBytes + headerBytes;
  if (totalBytes > maximumPayloadBytes) {
    throw NatsMaxPayloadException(
      actual: totalBytes,
      maximum: maximumPayloadBytes,
    );
  }
}
