import "dart:convert";
import "dart:typed_data";

import "package:nats_core/nats_core.dart";
import "package:test/test.dart";

void main() {
  group("NatsHeaders", () {
    test("preserves repeated values and matches names case insensitively", () {
      final source = <MapEntry<String, String>>[
        const MapEntry("Traceparent", "first"),
        const MapEntry("traceparent", "second"),
      ];

      final headers = NatsHeaders(entries: source);
      source.clear();

      expect(headers.values("TRACEPARENT"), ["first", "second"]);
      expect(headers.first("traceParent"), "first");
      expect(headers.entries, hasLength(2));
    });

    test("functional changes do not mutate the source", () {
      final original = NatsHeaders(
        entries: const [MapEntry("A", "one"), MapEntry("A", "two")],
      );

      final added = original.add("A", "three");
      final replaced = original.set("a", "replacement");
      final removed = original.remove("A");

      expect(original.values("a"), ["one", "two"]);
      expect(added.values("a"), ["one", "two", "three"]);
      expect(replaced.values("a"), ["replacement"]);
      expect(removed.values("a"), isEmpty);
    });

    test("encodes and parses status and repeated values exactly", () {
      final headers = NatsHeaders(
        entries: const [MapEntry("X-Test", "one"), MapEntry("X-Test", "two")],
        statusCode: 503,
        statusDescription: "No Responders",
      );

      final encoded = headers.encode();
      expect(
        ascii.decode(encoded),
        "NATS/1.0 503 No Responders\r\n"
        "X-Test: one\r\n"
        "X-Test: two\r\n"
        "\r\n",
      );

      final parsed = NatsHeaders.parse(encoded);
      expect(parsed.statusCode, 503);
      expect(parsed.statusDescription, "No Responders");
      expect(parsed.values("x-test"), ["one", "two"]);
    });

    test("encodes and parses UTF 8 values without weakening names", () {
      final headers = NatsHeaders(
        entries: const [MapEntry("X-Label", "상태 변경")],
        statusCode: 200,
        statusDescription: "성공",
      );

      final parsed = NatsHeaders.parse(headers.encode());

      expect(parsed.first("X-Label"), "상태 변경");
      expect(parsed.statusDescription, "성공");
      expect(
        () => NatsHeaders(entries: const [MapEntry("이름", "value")]),
        throwsA(isA<NatsHeaderException>()),
      );
    });

    test("rejects malformed blocks and injection", () {
      expect(
        () =>
            NatsHeaders.parse(Uint8List.fromList(ascii.encode("NATS/1.0\n\n"))),
        throwsA(isA<NatsHeaderException>()),
      );
      expect(
        () => NatsHeaders(
          entries: const [MapEntry("Injected\r\nHeader", "value")],
        ),
        throwsA(isA<NatsHeaderException>()),
      );
      expect(
        () => NatsHeaders(
          entries: const [MapEntry("Header", "value\r\ninjected")],
        ),
        throwsA(isA<NatsHeaderException>()),
      );
      expect(
        () => NatsHeaders(
          statusCode: 200,
          statusDescription: "OK\r\nInjected: yes",
        ),
        throwsA(isA<NatsHeaderException>()),
      );
      for (final description in ["control\u0001", "delete\u007f"]) {
        expect(
          () => NatsHeaders(statusCode: 200, statusDescription: description),
          throwsA(isA<NatsHeaderException>()),
          reason: description.codeUnits.toString(),
        );
      }
      expect(
        () => NatsHeaders.parse(
          Uint8List.fromList(ascii.encode("NATS/1.0\r\n continuation\r\n\r\n")),
        ),
        throwsA(isA<NatsHeaderException>()),
      );
    });
  });
}
