import "dart:convert";

import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/wire_response.dart";
import "package:test/test.dart";

void main() {
  test("wire response reads exact unsigned and decimal values", () {
    final response = JetStreamWireResponse.decode(
      utf8.encode(r'{"seq":18446744073709551615,"val":"-9223372036854775809"}'),
      "orders.created",
    );

    expect(
      response.requiredUnsigned("seq"),
      BigInt.parse("18446744073709551615"),
    );
    expect(
      response.requiredPositiveUint64("seq"),
      BigInt.parse("18446744073709551615"),
    );
    expect(
      response.requiredDecimal("val"),
      BigInt.parse("-9223372036854775809"),
    );
  });

  test("wire response rejects zero and overflow positive uint64 values", () {
    for (final value in ["0", "18446744073709551616"]) {
      final response = JetStreamWireResponse.decode(
        utf8.encode('{"seq":$value}'),
        "orders.created",
      );
      expect(
        () => response.requiredPositiveUint64("seq"),
        throwsA(
          isA<JetStreamProtocolException>().having(
            (error) => error.fieldPath,
            "fieldPath",
            "seq",
          ),
        ),
      );
    }
  });

  test("unsigned accessors enforce uint64 for every container shape", () {
    final overflow = BigInt.one << 64;
    final cases = <void Function(JetStreamWireResponse)>[
      (response) => response.requiredUnsigned("value"),
      (response) => response.optionalUnsigned("value"),
      (response) => response.optionalUnsignedList("values"),
      (response) => response.optionalUnsignedMap("counts"),
    ];

    for (final decode in cases) {
      final response = JetStreamWireResponse.decode(
        encodeExactJson({
          "value": overflow,
          "values": [overflow],
          "counts": {"subject": overflow},
        }),
        "orders.created",
      );
      expect(
        () => decode(response),
        throwsA(isA<JetStreamProtocolException>()),
      );
    }
  });

  test("signed int64 accessors enforce both boundaries", () {
    final maximum = (BigInt.one << 63) - BigInt.one;
    final minimum = -(BigInt.one << 63);
    final accepted = JetStreamWireResponse.decode(
      encodeExactJson({"minimum": minimum, "maximum": maximum}),
      "orders.created",
    );
    expect(accepted.requiredSignedInt64("minimum"), minimum);
    expect(accepted.requiredSignedInt64("maximum"), maximum);
    expect(accepted.requiredNonNegativeInt64("maximum"), maximum);

    for (final value in [minimum - BigInt.one, maximum + BigInt.one]) {
      final response = JetStreamWireResponse.decode(
        encodeExactJson({"value": value}),
        "orders.created",
      );
      expect(
        () => response.requiredSignedInt64("value"),
        throwsA(isA<JetStreamProtocolException>()),
      );
    }
  });

  test("bounded integer and positive duration accessors enforce semantics", () {
    final response = JetStreamWireResponse.decode(
      encodeExactJson({
        "bounded": BigInt.from(5),
        "low": BigInt.from(4),
        "high": BigInt.from(6),
        "zero": BigInt.zero,
      }),
      "orders.created",
    );

    expect(response.requiredBoundedInt("bounded", minimum: 5, maximum: 5), 5);
    for (final field in ["low", "high"]) {
      expect(
        () => response.requiredBoundedInt(field, minimum: 5, maximum: 5),
        throwsA(isA<JetStreamProtocolException>()),
      );
    }
    expect(
      () => response.optionalPositiveNanoseconds("zero"),
      throwsA(isA<JetStreamProtocolException>()),
    );
  });

  test("API error integer fields enforce protocol ranges", () {
    for (final (field, value) in [
      ("code", BigInt.from(299)),
      ("code", BigInt.from(700)),
      ("err_code", BigInt.from(-1)),
      ("err_code", BigInt.from(65536)),
    ]) {
      final response = JetStreamWireResponse.decode(
        encodeExactJson({
          "error": {
            "code": BigInt.from(400),
            "err_code": BigInt.from(10000),
            "description": "wrong",
            field: value,
          },
        }),
        "orders.created",
      );
      expect(
        response.throwApiError,
        throwsA(
          isA<JetStreamProtocolException>().having(
            (error) => error.fieldPath,
            "fieldPath",
            "error.$field",
          ),
        ),
      );
    }
  });

  test("wire response distinguishes absent and malformed optional fields", () {
    final absent = JetStreamWireResponse.decode(
      utf8.encode(r'{"seq":1}'),
      "orders.created",
    );
    expect(absent.optionalBoolean("duplicate"), isFalse);

    final malformed = JetStreamWireResponse.decode(
      utf8.encode(r'{"duplicate":"false"}'),
      "orders.created",
    );
    expect(
      () => malformed.optionalBoolean("duplicate"),
      throwsA(
        isA<JetStreamProtocolException>()
            .having((error) => error.subject, "subject", "orders.created")
            .having((error) => error.fieldPath, "fieldPath", "duplicate"),
      ),
    );
  });

  test(
    "wire response rejects negative unsigned and invalid decimal fields",
    () {
      final response = JetStreamWireResponse.decode(
        utf8.encode(r'{"seq":-1,"val":"1.5"}'),
        "orders.created",
      );

      expect(
        () => response.requiredUnsigned("seq"),
        throwsA(isA<JetStreamProtocolException>()),
      );
      expect(
        () => response.requiredDecimal("val"),
        throwsA(isA<JetStreamProtocolException>()),
      );
    },
  );

  test("wire response bounds decimal string complexity", () {
    const maximum = "99999999999999999999";
    final accepted = JetStreamWireResponse.decode(
      utf8.encode('{"val":"$maximum"}'),
      "counter.value",
    );
    expect(
      accepted.requiredDecimal("val", maximumDigits: 20),
      BigInt.parse(maximum),
    );

    final rejected = JetStreamWireResponse.decode(
      utf8.encode('{"val":"${maximum}9"}'),
      "counter.value",
    );
    expect(
      () => rejected.requiredDecimal("val", maximumDigits: 20),
      throwsA(isA<JetStreamProtocolException>()),
    );

    expect(rejected.requiredDecimal("val"), BigInt.parse("${maximum}9"));

    final oversized = JetStreamWireResponse.decode(
      utf8.encode('{"val":"${"9" * 1000000}"}'),
      "counter.value",
    );
    final elapsed = Stopwatch()..start();
    expect(
      () => oversized.requiredDecimal("val"),
      throwsA(isA<JetStreamProtocolException>()),
    );
    expect(elapsed.elapsed, lessThan(const Duration(milliseconds: 100)));
  });

  test("API errors preserve stable fields and redact safe response", () {
    final response = JetStreamWireResponse.decode(
      utf8.encode(
        r'{"type":"io.nats.test","token":"secret-value",'
        r'"error":{"code":400,"err_code":10071,"description":"wrong"}}',
      ),
      "orders.created",
    );

    expect(
      response.throwApiError,
      throwsA(
        isA<JetStreamApiException>()
            .having((error) => error.code, "code", 400)
            .having((error) => error.errorCode, "errorCode", 10071)
            .having(
              (error) => error.responseType,
              "responseType",
              "io.nats.test",
            )
            .having(
              (error) => error.safeResponse,
              "safeResponse",
              allOf(contains("<redacted>"), isNot(contains("secret-value"))),
            ),
      ),
    );
  });

  test("API error diagnostics redact credential variants and stay bounded", () {
    final response = JetStreamWireResponse.decode(
      utf8.encode(
        '{"outer":{"PaSs":"password-value",'
        '"client_private_key":"private-value","SIG":"signature-value"},'
        '"padding":"${"x" * 10000}",'
        '"error":{"code":400,"err_code":10071,"description":"wrong"}}',
      ),
      "orders.created",
    );

    try {
      response.throwApiError();
      fail("Expected API error");
    } on JetStreamApiException catch (error) {
      final safe = error.safeResponse!;
      expect(safe.length, 2048);
      expect(safe, contains("<redacted>"));
      expect(safe, isNot(contains("password-value")));
      expect(safe, isNot(contains("private-value")));
      expect(safe, isNot(contains("signature-value")));
    }
  });
}
