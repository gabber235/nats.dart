import "dart:convert";

import "package:nats_jetstream/src/exact_json.dart";
import "package:test/test.dart";

void main() {
  test("exact JSON preserves integer tokens beyond JavaScript precision", () {
    final decoded = decodeExactJsonObject(
      utf8.encode(
        r'{"value":18446744073709551615,"negative":-9223372036854775809}',
      ),
    );

    expect(decoded["value"], BigInt.parse("18446744073709551615"));
    expect(decoded["negative"], BigInt.parse("-9223372036854775809"));
  });

  test("exact JSON round trips escaped strings and BigInt values", () {
    final value = <String, Object?>{
      "text": "quote \" and snowman ☃",
      "integer": BigInt.parse("18446744073709551615"),
      "items": <Object?>[true, null, 1.5],
    };

    expect(decodeExactJsonObject(encodeExactJson(value)), value);
  });

  test("exact JSON requires exact UTF 8 strings and keys", () {
    for (final invalid in ["\uD800", "\uDC00"]) {
      expect(
        () => immutableExactJsonObject({"value": invalid}),
        throwsArgumentError,
      );
      expect(
        () => immutableExactJsonObject({invalid: "value"}),
        throwsArgumentError,
      );
      expect(
        () => encodeExactJson({"value": invalid}),
        throwsA(isA<JsonUnsupportedObjectError>()),
      );
      expect(
        () => encodeExactJson({invalid: "value"}),
        throwsA(isA<JsonUnsupportedObjectError>()),
      );
    }

    const valid = "supplementary 🙂 text";
    expect(decodeExactJsonObject(encodeExactJson({valid: valid})), {
      valid: valid,
    });
    expect(
      () => decodeExactJsonObject(utf8.encode(r'{"value":"\uD800"}')),
      throwsFormatException,
    );
  });

  test("exact JSON rejects ambiguous and malformed input", () {
    expect(
      () => decodeExactJsonObject(utf8.encode(r'{"x":1,"x":2}')),
      throwsFormatException,
    );
    expect(
      () => decodeExactJsonObject(utf8.encode(r'{"x":01}')),
      throwsFormatException,
    );
    expect(
      () => decodeExactJsonObject(utf8.encode("[]")),
      throwsFormatException,
    );
    expect(
      () => decodeExactJsonObject(utf8.encode(r'{"x":1}'), maxBytes: 2),
      throwsFormatException,
    );
  });

  test("exact JSON bounds integer token complexity", () {
    const maximum = "99999999999999999999";
    expect(
      decodeExactJsonObject(utf8.encode('{"value":$maximum}'))["value"],
      BigInt.parse(maximum),
    );
    expect(
      () => decodeExactJsonObject(utf8.encode('{"value":${maximum}9}')),
      throwsFormatException,
    );
    expect(
      () => encodeExactJson({"value": BigInt.parse("${maximum}9")}),
      throwsA(isA<JsonUnsupportedObjectError>()),
    );

    final oversized = utf8.encode('{"value":${"9" * 1000000}}');
    final elapsed = Stopwatch()..start();
    expect(() => decodeExactJsonObject(oversized), throwsFormatException);
    expect(elapsed.elapsed, lessThan(const Duration(seconds: 2)));
  });

  test("exact JSON snapshots nested values deeply", () {
    final leaf = <String, Object?>{"enabled": true};
    final values = <Object?>[leaf];
    final source = <String, Object?>{"values": values};

    final snapshot = immutableExactJsonObject(source);
    leaf["enabled"] = false;
    values.add("changed");
    source["new"] = true;

    final snapshotValues = snapshot["values"]! as List<Object?>;
    final snapshotLeaf = snapshotValues.single as Map<String, Object?>;
    expect(snapshotLeaf["enabled"], isTrue);
    expect(snapshotValues, hasLength(1));
    expect(snapshot, isNot(contains("new")));
    expect(() => snapshotValues.add("new"), throwsUnsupportedError);
    expect(() => snapshotLeaf["enabled"] = false, throwsUnsupportedError);
  });

  test("exact JSON snapshot rejects unsupported and cyclic values", () {
    expect(
      () => immutableExactJsonObject({"value": <Object?>{}}),
      throwsArgumentError,
    );
    expect(
      () => immutableExactJsonObject({
        "value": <Object?, Object?>{1: "invalid"},
      }),
      throwsArgumentError,
    );
    for (final value in [
      double.nan,
      double.infinity,
      double.negativeInfinity,
    ]) {
      expect(
        () => immutableExactJsonObject({"value": value}),
        throwsArgumentError,
      );
    }

    final self = <Object?>[];
    self.add(self);
    expect(
      () => immutableExactJsonObject({"value": self}),
      throwsArgumentError,
    );
    final left = <String, Object?>{};
    final right = <String, Object?>{"left": left};
    left["right"] = right;
    expect(() => immutableExactJsonObject(left), throwsArgumentError);
  });
}
