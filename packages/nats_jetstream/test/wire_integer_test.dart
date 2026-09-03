import "package:nats_jetstream/src/wire_integer.dart";
import "package:test/test.dart";

void main() {
  test("shared wire validator enforces exact integer boundaries", () {
    expect(
      validateJetStreamInt64(minimumJetStreamInt64, "value"),
      minimumJetStreamInt64,
    );
    expect(
      validateJetStreamInt64(maximumJetStreamInt64, "value"),
      maximumJetStreamInt64,
    );
    expect(
      validateJetStreamUint64(maximumJetStreamUint64, "value"),
      maximumJetStreamUint64,
    );
    expect(
      validateJetStreamMessageSize(maximumJetStreamInt32, "value"),
      maximumJetStreamInt32,
    );
    expect(validateJetStreamMessageSize(-BigInt.one, "value"), -BigInt.one);

    for (final call in <Object Function()>[
      () => validateJetStreamInt64(minimumJetStreamInt64 - BigInt.one, "value"),
      () => validateJetStreamInt64(maximumJetStreamInt64 + BigInt.one, "value"),
      () => validateJetStreamUint64(-BigInt.one, "value"),
      () =>
          validateJetStreamUint64(maximumJetStreamUint64 + BigInt.one, "value"),
      () => validateJetStreamMessageSize(-BigInt.two, "value"),
      () => validateJetStreamMessageSize(
        maximumJetStreamInt32 + BigInt.one,
        "value",
      ),
    ]) {
      expect(call, throwsArgumentError);
    }
  });

  test("shared wire validator distinguishes duration domains", () {
    expect(validateJetStreamDuration(BigInt.zero, "value"), BigInt.zero);
    expect(
      validatePositiveJetStreamDuration(maximumJetStreamInt64, "value"),
      maximumJetStreamInt64,
    );
    expect(
      () => validatePositiveJetStreamDuration(BigInt.zero, "value"),
      throwsArgumentError,
    );
    expect(
      () => validateJetStreamDuration(
        maximumJetStreamInt64 + BigInt.one,
        "value",
      ),
      throwsArgumentError,
    );
  });
}
