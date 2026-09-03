import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/management.dart";
import "package:nats_jetstream/src/wire_response.dart";
import "package:test/test.dart";

void main() {
  test("account info decodes exact current usage limits API and tiers", () {
    final maximumUint64 = BigInt.parse("18446744073709551615");
    final body = _accountBody()
      ..["reserved_storage"] = maximumUint64
      ..["future_account"] = {"exact": maximumUint64};
    final tier =
        (body["tiers"]! as Map<String, Object?>)["R3"]! as Map<String, Object?>;
    tier["future_tier"] = [maximumUint64];

    final info = _decode(body);

    expect(info.memoryBytes, BigInt.from(11));
    expect(info.storageBytes, BigInt.from(12));
    expect(info.reservedMemoryBytes, BigInt.from(13));
    expect(info.reservedStorageBytes, maximumUint64);
    expect(info.streams, BigInt.from(2));
    expect(info.consumers, BigInt.from(3));
    expect(info.domain, "EDGE");
    expect(info.apiLevel, 4);
    expect(info.api.total, BigInt.from(21));
    expect(info.api.errors, BigInt.from(2));
    expect(info.api.inflight, BigInt.one);
    expect(info.limits.maximumMemoryBytes, BigInt.from(-1));
    expect(info.limits.maximumBytesRequired, isTrue);
    expect(info.tiers.keys, ["R3"]);
    expect(info.tiers["R3"]?.reservedStorageBytes, BigInt.from(32));
    expect(info.tiers["R3"]?.limits.maximumConsumers, BigInt.from(9));
    expect(
      (info.raw["future_account"]! as Map<String, Object?>)["exact"],
      maximumUint64,
    );
    expect(
      () => (info.raw["future_account"]! as Map<String, Object?>).clear(),
      throwsUnsupportedError,
    );
    expect(
      () => (info.tiers["R3"]!.raw["future_tier"]! as List<Object?>).clear(),
      throwsUnsupportedError,
    );
    expect(info.tiers.clear, throwsUnsupportedError);
  });

  test("account info defaults fields omitted by older servers", () {
    final body = _accountBody()
      ..remove("reserved_memory")
      ..remove("reserved_storage")
      ..remove("tiers");
    (body["limits"]! as Map<String, Object?>).remove("max_bytes_required");
    (body["api"]! as Map<String, Object?>)
      ..remove("level")
      ..remove("inflight");

    final info = _decode(body);

    expect(info.reservedMemoryBytes, BigInt.zero);
    expect(info.reservedStorageBytes, BigInt.zero);
    expect(info.limits.maximumBytesRequired, isFalse);
    expect(info.api.level, 0);
    expect(info.api.inflight, BigInt.zero);
    expect(info.tiers, isEmpty);
  });

  test("account info rejects malformed current fields exactly", () {
    final cases = <(String, Map<String, Object?> Function())>[
      (
        "reserved_memory",
        () => _accountBody()..["reserved_memory"] = BigInt.from(-1),
      ),
      (
        "limits.max_bytes_required",
        () {
          final body = _accountBody();
          (body["limits"]! as Map<String, Object?>)["max_bytes_required"] =
              "true";
          return body;
        },
      ),
      (
        "api.total",
        () {
          final body = _accountBody();
          (body["api"]! as Map<String, Object?>)["total"] = BigInt.from(-1);
          return body;
        },
      ),
      (
        "api.level",
        () {
          final body = _accountBody();
          (body["api"]! as Map<String, Object?>)["level"] = BigInt.from(-1);
          return body;
        },
      ),
      ("tiers.R3", () => _accountBody()..["tiers"] = {"R3": const <Object?>[]}),
    ];

    for (final (path, createBody) in cases) {
      expect(
        () => _decode(createBody()),
        throwsA(
          isA<JetStreamProtocolException>().having(
            (error) => error.fieldPath,
            "fieldPath",
            path,
          ),
        ),
        reason: path,
      );
    }
  });

  test("account usage and limits enforce schema integer ranges", () {
    final uint64Overflow = BigInt.one << 64;
    final int64Overflow = BigInt.one << 63;
    final int64Underflow = -int64Overflow - BigInt.one;
    final cases = <(String, void Function(Map<String, Object?>))>[
      for (final field in [
        "memory",
        "storage",
        "reserved_memory",
        "reserved_storage",
      ])
        (field, (body) => body[field] = uint64Overflow),
      for (final field in ["total", "errors", "inflight"])
        (
          "api.$field",
          (body) =>
              (body["api"]! as Map<String, Object?>)[field] = uint64Overflow,
        ),
      for (final field in ["streams", "consumers"])
        (field, (body) => body[field] = int64Overflow),
      for (final field in [
        "max_memory",
        "max_storage",
        "max_streams",
        "max_consumers",
        "max_ack_pending",
        "memory_max_stream_bytes",
        "storage_max_stream_bytes",
      ]) ...[
        (
          "limits.$field",
          (body) =>
              (body["limits"]! as Map<String, Object?>)[field] = int64Overflow,
        ),
        (
          "limits.$field",
          (body) =>
              (body["limits"]! as Map<String, Object?>)[field] = int64Underflow,
        ),
      ],
    ];

    for (final (path, mutate) in cases) {
      final body = _accountBody();
      mutate(body);
      expect(
        () => _decode(body),
        throwsA(
          isA<JetStreamProtocolException>().having(
            (error) => error.fieldPath,
            "fieldPath",
            path,
          ),
        ),
        reason: path,
      );
    }
  });

  test("account limits accept only minus one or nonnegative values", () {
    for (final field in [
      "max_memory",
      "max_storage",
      "max_streams",
      "max_consumers",
      "max_ack_pending",
      "memory_max_stream_bytes",
      "storage_max_stream_bytes",
    ]) {
      final body = _accountBody();
      (body["limits"]! as Map<String, Object?>)[field] = BigInt.from(-2);
      expect(
        () => _decode(body),
        throwsA(
          isA<JetStreamProtocolException>().having(
            (error) => error.fieldPath,
            "fieldPath",
            "limits.$field",
          ),
        ),
        reason: field,
      );
    }
  });
}

JetStreamAccountInfo _decode(Map<String, Object?> body) {
  return JetStreamManagement.decodeAccountInfo(
    JetStreamWireResponse.decode(encodeExactJson(body), r"$JS.API.INFO"),
  );
}

Map<String, Object?> _accountBody() {
  return {
    "type": "io.nats.jetstream.api.v1.account_info_response",
    "memory": BigInt.from(11),
    "storage": BigInt.from(12),
    "reserved_memory": BigInt.from(13),
    "reserved_storage": BigInt.from(14),
    "streams": BigInt.from(2),
    "consumers": BigInt.from(3),
    "domain": "EDGE",
    "limits": _limits(maximumConsumers: BigInt.from(8)),
    "api": {
      "level": BigInt.from(4),
      "total": BigInt.from(21),
      "errors": BigInt.from(2),
      "inflight": BigInt.one,
      "future_api": true,
    },
    "tiers": {
      "R3": {
        "memory": BigInt.from(29),
        "storage": BigInt.from(30),
        "reserved_memory": BigInt.from(31),
        "reserved_storage": BigInt.from(32),
        "streams": BigInt.from(4),
        "consumers": BigInt.from(5),
        "limits": _limits(maximumConsumers: BigInt.from(9)),
      },
    },
  };
}

Map<String, Object?> _limits({required BigInt maximumConsumers}) {
  return {
    "max_memory": BigInt.from(-1),
    "max_storage": BigInt.from(1024),
    "max_streams": BigInt.from(7),
    "max_consumers": maximumConsumers,
    "max_ack_pending": BigInt.from(1000),
    "memory_max_stream_bytes": BigInt.from(512),
    "storage_max_stream_bytes": BigInt.from(768),
    "max_bytes_required": true,
    "future_limit": "preserved",
  };
}
