import "package:nats_core/src/subject.dart";

/// Compression accepted for ADR 41 trace event payloads.
enum NatsTraceEncoding {
  /// Standard gzip compression.
  gzip("gzip"),

  /// Snappy compression.
  snappy("snappy");

  const NatsTraceEncoding(this.headerValue);

  /// Exact `Accept-Encoding` header value.
  final String headerValue;
}

/// Explicit per message activation for NATS ADR 41 path tracing.
final class NatsTraceOptions {
  /// Creates validated trace activation options.
  factory NatsTraceOptions({
    required String destination,
    bool traceOnly = false,
    NatsTraceEncoding? encoding,
  }) {
    validatePublishSubject(destination);
    return NatsTraceOptions._(destination, traceOnly, encoding);
  }

  const NatsTraceOptions._(this.destination, this.traceOnly, this.encoding);

  /// Subject that receives server trace events.
  final String destination;

  /// Whether the server reports delivery without delivering the message.
  final bool traceOnly;

  /// Optional trace event payload compression accepted by the caller.
  final NatsTraceEncoding? encoding;
}
