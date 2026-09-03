import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  test("atomic message snapshots caller payload", () {
    final payload = [1, 2, 3];
    final message = JetStreamAtomicMessage("orders.created", payload);
    payload[0] = 9;

    expect(message.payload, [1, 2, 3]);
  });

  test("atomic message rejects nonbyte payload values", () {
    expect(
      () => JetStreamAtomicMessage("orders.created", const [-1]),
      throwsFormatException,
    );
    expect(
      () => JetStreamAtomicMessage("orders.created", const [256]),
      throwsFormatException,
    );
    expect(JetStreamAtomicMessage("orders.created", const [0, 255]).payload, [
      0,
      255,
    ]);
  });
}
