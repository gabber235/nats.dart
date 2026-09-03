import "package:nats_jetstream/src/subject_pattern_set.dart";
import "package:test/test.dart";

void main() {
  test("large disjoint subject sets validate with bounded work", () {
    final patterns = [
      for (var index = 0; index < 4096; index += 1) "events.$index.*",
    ];
    expect(
      () => validateDisjointNatsSubjectPatterns(patterns),
      returnsNormally,
    );
  });

  test("subject set rejects duplicate and wildcard intersections", () {
    for (final patterns in [
      ["events.one", "events.one"],
      ["events.*", "events.one"],
      ["events.>", "events.one.more"],
      ["*.created", "events.*"],
    ]) {
      expect(
        () => validateDisjointNatsSubjectPatterns(patterns),
        throwsArgumentError,
      );
    }
  });

  test("subject set rejects count above the explicit ceiling", () {
    expect(
      () => validateDisjointNatsSubjectPatterns([
        for (
          var index = 0;
          index <= maximumJetStreamSubjectPatterns;
          index += 1
        )
          "events.$index",
      ]),
      throwsArgumentError,
    );
  });

  test("long token paths avoid recursive stack growth", () {
    final prefix = List.filled(2047, "a").join(".");
    expect(
      () => validateDisjointNatsSubjectPatterns(["$prefix.b", "$prefix.*"]),
      throwsArgumentError,
    );
  });

  test("cross set validation requires every pattern to intersect", () {
    expect(
      () => validateNatsSubjectPatternsIntersectAny(
        ["events.*", "orders.created"],
        ["events.>", "orders.*"],
      ),
      returnsNormally,
    );
    expect(
      () => validateNatsSubjectPatternsIntersectAny(
        ["events.*", "payments.created"],
        ["events.>", "orders.*"],
      ),
      throwsArgumentError,
    );
  });

  test("adversarial cross set validation stops at the work ceiling", () {
    final patterns = [
      for (var index = 0; index < 2048; index += 1) "events.*.filter.$index",
    ];
    final candidates = [
      for (var index = 0; index < 2048; index += 1)
        "events.$index.filter.$index",
    ];
    expect(
      () => validateNatsSubjectPatternsIntersectAny(patterns, candidates),
      throwsA(
        isA<ArgumentError>().having(
          (error) => error.message,
          "message",
          contains("work exceeds"),
        ),
      ),
    );
  });

  test("adversarial disjoint validation stops at the work ceiling", () {
    final patterns = [
      for (var index = 0; index < 2048; index += 1) "events.$index.value",
      for (var index = 0; index < 2048; index += 1) "events.*.filter.$index",
    ];
    expect(
      () => validateDisjointNatsSubjectPatterns(patterns),
      throwsA(
        isA<ArgumentError>().having(
          (error) => error.message,
          "message",
          contains("work exceeds"),
        ),
      ),
    );
  });
}
