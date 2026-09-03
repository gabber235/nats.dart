import "dart:async";

import "package:nats_core/src/connection/bounded_diagnostic_events.dart";
import "package:test/test.dart";

void main() {
  test("paused listener retains only the latest diagnostic", () async {
    final events = BoundedDiagnosticEvents<int>();
    final received = <int>[];
    final listener = events.stream.listen(received.add)..pause();

    for (var value = 0; value < 1000; value += 1) {
      events.add(value);
    }
    listener.resume();
    await Future<void>.delayed(Duration.zero);

    expect(received, [999]);
    await listener.cancel();
  });

  test("close preserves the latest diagnostic for a paused listener", () async {
    final events = BoundedDiagnosticEvents<int>();
    final received = <int>[];
    final done = Completer<void>();
    final listener = events.stream.listen(received.add, onDone: done.complete)
      ..pause();

    events
      ..add(1)
      ..add(2)
      ..close();
    listener.resume();
    await done.future.timeout(const Duration(seconds: 1));

    expect(received, [2]);
    await listener.cancel();
  });
}
