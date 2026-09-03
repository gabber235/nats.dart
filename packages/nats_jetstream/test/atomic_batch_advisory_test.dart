import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:test/test.dart";

void main() {
  const reasons = {
    "timeout": JetStreamAtomicBatchAbandonKind.timeout,
    "large": JetStreamAtomicBatchAbandonKind.large,
    "incomplete": JetStreamAtomicBatchAbandonKind.incomplete,
    "unsupported": JetStreamAtomicBatchAbandonKind.unsupported,
  };

  for (final reason in reasons.entries) {
    test("decodes the ${reason.key} abandonment reason", () {
      final advisory = decodeJetStreamAtomicBatchAbandonedAdvisory(
        _message(reason: reason.key),
      );

      expect(advisory.type, _type);
      expect(advisory.id, "event-1");
      expect(advisory.timestamp, "2026-09-02T00:00:00.123456789Z");
      expect(advisory.account, "ACCOUNT");
      expect(advisory.domain, "DOMAIN");
      expect(advisory.stream, "EVENTS");
      expect(advisory.batchId, "batch-1");
      expect(advisory.reason.wireValue, reason.key);
      expect(advisory.reason.knownKind, reason.value);
    });
  }

  test("preserves an unknown reason and exact immutable JSON", () {
    final advisory = decodeJetStreamAtomicBatchAbandonedAdvisory(
      _message(
        reason: "future_reason",
        extra: {
          "future": [
            {"sequence": BigInt.parse("18446744073709551615")},
          ],
        },
      ),
    );

    expect(advisory.reason.wireValue, "future_reason");
    expect(advisory.reason.knownKind, isNull);
    expect(advisory.raw["future"], [
      {"sequence": BigInt.parse("18446744073709551615")},
    ]);
    expect(() => advisory.raw["added"] = true, throwsUnsupportedError);
    expect(
      () => (advisory.raw["future"]! as List<Object?>).add("changed"),
      throwsUnsupportedError,
    );
    expect(
      () =>
          ((advisory.raw["future"]! as List<Object?>).single!
                  as Map<String, Object?>)["sequence"] =
              BigInt.zero,
      throwsUnsupportedError,
    );
  });

  test("correlates only the exact stream and batch pair", () {
    final advisory = decodeJetStreamAtomicBatchAbandonedAdvisory(_message());

    expect(advisory.matches(stream: "EVENTS", batchId: "batch-1"), isTrue);
    expect(advisory.matches(stream: "OTHER", batchId: "batch-1"), isFalse);
    expect(advisory.matches(stream: "EVENTS", batchId: "batch-2"), isFalse);
  });

  for (final scenario in <({String name, NatsMessage message, String? field})>[
    (
      name: "wrong subject prefix",
      message: _message(subject: r"$JS.EVENT.ADVISORY.STREAM.OTHER.EVENTS"),
      field: null,
    ),
    (
      name: "invalid subject stream",
      message: _message(
        subject: r"$JS.EVENT.ADVISORY.STREAM.BATCH_ABANDONED.EVENTS.INVALID",
      ),
      field: null,
    ),
    (
      name: "wrong event type",
      message: _message(type: "future.type"),
      field: "type",
    ),
    (
      name: "missing event identity",
      message: _message(remove: "id"),
      field: "id",
    ),
    (
      name: "invalid timestamp",
      message: _message(timestamp: "2026-09-02 00:00:00"),
      field: "timestamp",
    ),
    (
      name: "subject and payload stream mismatch",
      message: _message(stream: "OTHER"),
      field: "stream",
    ),
    (
      name: "missing batch identity",
      message: _message(remove: "batch"),
      field: "batch",
    ),
    (name: "empty reason", message: _message(reason: ""), field: "reason"),
  ]) {
    test("rejects ${scenario.name}", () {
      expect(
        () => decodeJetStreamAtomicBatchAbandonedAdvisory(scenario.message),
        throwsA(
          isA<JetStreamProtocolException>().having(
            (error) => error.fieldPath,
            "fieldPath",
            scenario.field,
          ),
        ),
      );
    });
  }
}

const _type = "io.nats.jetstream.advisory.v1.stream_batch_abandoned";

NatsMessage _message({
  String subject = r"$JS.EVENT.ADVISORY.STREAM.BATCH_ABANDONED.EVENTS",
  String type = _type,
  String timestamp = "2026-09-02T00:00:00.123456789Z",
  String stream = "EVENTS",
  String reason = "timeout",
  String? remove,
  Map<String, Object?> extra = const {},
}) {
  final body = <String, Object?>{
    "type": type,
    "id": "event-1",
    "timestamp": timestamp,
    "account": "ACCOUNT",
    "domain": "DOMAIN",
    "stream": stream,
    "batch": "batch-1",
    "reason": reason,
    ...extra,
  }..remove(remove);
  return NatsMessage(subject: subject, payload: encodeExactJson(body));
}
