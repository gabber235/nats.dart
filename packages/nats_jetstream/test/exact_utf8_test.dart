import "dart:convert";

import "package:nats_jetstream/src/exact_utf8.dart";
import "package:test/test.dart";

void main() {
  test("exact UTF 8 rejects both lone surrogate forms", () {
    for (final value in [
      String.fromCharCode(0xd800),
      String.fromCharCode(0xdc00),
    ]) {
      expect(() => encodeExactUtf8(value), throwsArgumentError);
    }
  });

  test("exact UTF 8 preserves supplementary characters", () {
    const value = "stored 😀 text";
    final bytes = encodeExactUtf8(value);
    expect(utf8.decode(bytes, allowMalformed: false), value);
  });
}
