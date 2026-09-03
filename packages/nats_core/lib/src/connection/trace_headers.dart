import "package:nats_core/src/error.dart";
import "package:nats_core/src/headers.dart";
import "package:nats_core/src/trace.dart";

const _traceDestination = "Nats-Trace-Dest";
const _traceOnly = "Nats-Trace-Only";
const _acceptEncoding = "Accept-Encoding";
const _managedTraceHeaders = {
  "nats-trace-dest",
  "nats-trace-only",
  "accept-encoding",
};

/// Returns caller headers merged with typed trace activation.
NatsHeaders? mergeTraceHeaders(NatsHeaders? headers, NatsTraceOptions? trace) {
  if (trace == null) {
    return headers;
  }
  for (final entry in headers?.entries ?? const <MapEntry<String, String>>[]) {
    if (_managedTraceHeaders.contains(entry.key.toLowerCase())) {
      throw NatsHeaderException(
        "Caller header conflicts with managed trace header ${entry.key}",
      );
    }
  }

  var merged = headers ?? NatsHeaders();
  merged = merged.add(_traceDestination, trace.destination);
  if (trace.traceOnly) {
    merged = merged.add(_traceOnly, "1");
  }
  if (trace.encoding case final encoding?) {
    merged = merged.add(_acceptEncoding, encoding.headerValue);
  }
  return merged;
}
