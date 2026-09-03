import "package:nats_jetstream/src/expiring_ownership_set.dart";
import "package:test/test.dart";

void main() {
  test("ownership expires when the server cleanup guarantee applies", () {
    var now = DateTime.utc(2026);
    final ownership = JetStreamExpiringOwnershipSet(
      const Duration(seconds: 5),
      clock: () => now,
    )..add("consumer");

    now = now.add(const Duration(seconds: 5));

    expect(ownership.snapshot(), isEmpty);
  });

  test("ownership evicts oldest entries at its fixed memory bound", () {
    final ownership =
        JetStreamExpiringOwnershipSet(
            const Duration(minutes: 1),
            maximumEntries: 2,
          )
          ..add("first")
          ..add("second")
          ..add("third");

    expect(ownership.snapshot(), ["second", "third"]);
  });
}
