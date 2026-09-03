import "dart:convert";

import "package:nats_core/nats_core.dart";
import "package:nats_core/src/connection/trace_headers.dart";
import "package:test/test.dart";

void main() {
  test("trace options validate an exact destination subject", () {
    expect(
      () => NatsTraceOptions(destination: "trace.events"),
      returnsNormally,
    );
    for (final subject in ["", "trace.*", "trace.>", "trace..events"]) {
      expect(
        () => NatsTraceOptions(destination: subject),
        throwsA(isA<NatsSubjectException>()),
      );
    }
  });

  test("trace headers merge exactly without mutating caller headers", () {
    final caller = NatsHeaders(
      entries: const [MapEntry("X-Caller", "preserved")],
    );

    final merged = mergeTraceHeaders(
      caller,
      NatsTraceOptions(
        destination: "trace.events",
        traceOnly: true,
        encoding: NatsTraceEncoding.snappy,
      ),
    )!;

    expect(caller.entries, hasLength(1));
    expect(caller.entries.single.key, "X-Caller");
    expect(caller.entries.single.value, "preserved");
    expect(caller.first("Nats-Trace-Dest"), isNull);
    expect(
      ascii.decode(merged.encode()),
      "NATS/1.0\r\n"
      "X-Caller: preserved\r\n"
      "Nats-Trace-Dest: trace.events\r\n"
      "Nats-Trace-Only: 1\r\n"
      "Accept-Encoding: snappy\r\n\r\n",
    );
  });

  test("trace options reject every managed caller header conflict", () {
    final trace = NatsTraceOptions(destination: "trace.events");
    for (final name in [
      "nats-trace-dest",
      "NATS-TRACE-ONLY",
      "Accept-Encoding",
    ]) {
      expect(
        () => mergeTraceHeaders(
          NatsHeaders(entries: [MapEntry(name, "caller")]),
          trace,
        ),
        throwsA(isA<NatsHeaderException>()),
      );
    }
  });
}
