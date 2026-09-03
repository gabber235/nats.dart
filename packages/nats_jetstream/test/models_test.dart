import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  test("nanosecond conversion rejects values outside the Dart int range", () {
    var firstInvalidPositive = BigInt.one;
    while (firstInvalidPositive.isValidInt) {
      firstInvalidPositive <<= 1;
    }
    final maximum = firstInvalidPositive - BigInt.one;
    final minimum = -firstInvalidPositive;

    expect(
      JetStreamNanoseconds(maximum * BigInt.from(1000)).toDuration(),
      Duration(microseconds: maximum.toInt()),
    );
    expect(
      JetStreamNanoseconds(minimum * BigInt.from(1000)).toDuration(),
      Duration(microseconds: minimum.toInt()),
    );
    expect(
      () => JetStreamNanoseconds(
        firstInvalidPositive * BigInt.from(1000),
      ).toDuration(),
      throwsStateError,
    );
    expect(
      () => JetStreamNanoseconds(
        (minimum - BigInt.one) * BigInt.from(1000),
      ).toDuration(),
      throwsStateError,
    );
  });
}
