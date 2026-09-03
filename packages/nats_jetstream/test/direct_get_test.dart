import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/direct_get_wire.dart";
import "package:test/test.dart";

void main() {
  const requestSubject = r"$JS.API.DIRECT.GET.EVENTS";

  test("direct message decoding strips protocol headers", () {
    final message = NatsMessage(
      subject: "_INBOX.reply",
      payload: const [1, 2, 3],
      headers: NatsHeaders(
        entries: const [
          MapEntry("Nats-Stream", "EVENTS"),
          MapEntry("Nats-Subject", "events.a"),
          MapEntry("Nats-Sequence", "9007199254740993"),
          MapEntry("Nats-Time-Stamp", "2026-09-01T00:00:00.123456789Z"),
          MapEntry("X-Test", "one"),
          MapEntry("X-Test", "two"),
        ],
      ),
    );

    final decoded = decodeDirectStoredMessage(
      message,
      requestSubject,
      expectedSequence: BigInt.parse("9007199254740993"),
    );

    expect(decoded.subject, "events.a");
    expect(decoded.sequence, BigInt.parse("9007199254740993"));
    expect(decoded.timestamp, "2026-09-01T00:00:00.123456789Z");
    expect(decoded.payload, [1, 2, 3]);
    expect(decoded.headers?.values("X-Test"), ["one", "two"]);
    expect(decoded.headers?.first("Nats-Sequence"), isNull);
  });

  test("direct batch decoding enforces the response chain", () {
    final message = NatsMessage(
      subject: "_INBOX.reply",
      payload: const [1],
      headers: NatsHeaders(
        entries: const [
          MapEntry("Nats-Stream", "EVENTS"),
          MapEntry("Nats-Subject", "events.a"),
          MapEntry("Nats-Sequence", "8"),
          MapEntry("Nats-Time-Stamp", "2026-09-01T00:00:00Z"),
          MapEntry("Nats-Num-Pending", "2"),
          MapEntry("Nats-Last-Sequence", "7"),
        ],
      ),
    );

    expect(
      decodeDirectBatchStoredMessage(
        message,
        requestSubject,
        BigInt.from(7),
      ).sequence,
      BigInt.from(8),
    );
    expect(
      () => decodeDirectBatchStoredMessage(
        message,
        requestSubject,
        BigInt.from(6),
      ),
      throwsA(isA<JetStreamProtocolException>()),
    );
  });

  test("direct message rejects an invalid server timestamp", () {
    final message = NatsMessage(
      subject: "_INBOX.reply",
      payload: const [],
      headers: NatsHeaders(
        entries: const [
          MapEntry("Nats-Stream", "EVENTS"),
          MapEntry("Nats-Subject", "events.a"),
          MapEntry("Nats-Sequence", "1"),
          MapEntry("Nats-Time-Stamp", "2026-09-42T00:00:00Z"),
        ],
      ),
    );

    expect(
      () => decodeDirectStoredMessage(message, requestSubject),
      throwsA(
        isA<JetStreamProtocolException>().having(
          (error) => error.fieldPath,
          "fieldPath",
          "Nats-Time-Stamp",
        ),
      ),
    );
  });

  test("direct batch end preserves exact pagination cursors", () {
    final end = decodeDirectBatchEnd(
      NatsMessage(
        subject: "_INBOX.reply",
        payload: const [],
        headers: NatsHeaders(
          statusCode: 204,
          statusDescription: "EOB",
          entries: const [
            MapEntry("Nats-Num-Pending", "3"),
            MapEntry("Nats-Last-Sequence", "9007199254740993"),
            MapEntry("Nats-UpTo-Sequence", "9007199254740999"),
          ],
        ),
      ),
      requestSubject,
    );

    expect(end.pending, BigInt.from(3));
    expect(end.lastSequence, BigInt.parse("9007199254740993"));
    expect(end.upToSequence, BigInt.parse("9007199254740999"));
  });

  test("direct numeric headers enforce bounded uint64 grammar", () {
    JetStreamStoredMessage decodeSequence(String value) =>
        decodeDirectStoredMessage(
          NatsMessage(
            subject: "_INBOX.reply",
            payload: const [],
            headers: NatsHeaders(
              entries: [
                const MapEntry("Nats-Stream", "EVENTS"),
                const MapEntry("Nats-Subject", "events.a"),
                MapEntry("Nats-Sequence", value),
                const MapEntry("Nats-Time-Stamp", "2026-09-01T00:00:00Z"),
              ],
            ),
          ),
          requestSubject,
        );

    expect(
      decodeSequence("18446744073709551615").sequence,
      BigInt.parse("18446744073709551615"),
    );
    for (final value in [
      "0",
      "18446744073709551616",
      "100000000000000000000",
      "+1",
      "-1",
    ]) {
      expect(
        () => decodeSequence(value),
        throwsA(isA<JetStreamProtocolException>()),
      );
    }

    expect(
      () => decodeSequence("9" * 1000000),
      throwsA(isA<JetStreamProtocolException>()),
    );
  });

  test("direct status remains machine readable", () {
    final message = NatsMessage(
      subject: "_INBOX.reply",
      payload: const [],
      headers: NatsHeaders(
        statusCode: 404,
        statusDescription: "Message Not Found",
      ),
    );

    expect(
      () => decodeDirectStoredMessage(message, requestSubject),
      throwsA(
        isA<JetStreamDirectGetException>()
            .having((error) => error.statusCode, "statusCode", 404)
            .having(
              (error) => error.description,
              "description",
              "Message Not Found",
            ),
      ),
    );
  });

  test("direct batch request snapshots and validates selectors", () {
    final subjects = ["events.a", "events.*"];
    final request = JetStreamDirectBatchRequest(
      maximumMessages: 8,
      maximumBufferedBytes: 4096,
      maximumServerBytes: 2048,
      multipleLastSubjects: subjects,
      upToSequence: BigInt.from(99),
    );
    subjects.add("events.b");

    expect(request.multipleLastSubjects, ["events.a", "events.*"]);
    expect(request.toJson(), {
      "batch": 8,
      "max_bytes": 2048,
      "multi_last": ["events.a", "events.*"],
      "up_to_seq": BigInt.from(99),
    });
    expect(
      () => JetStreamDirectBatchRequest(
        maximumMessages: 1,
        maximumBufferedBytes: 1,
        startSequence: BigInt.one,
        startTime: "2026-09-01T00:00:00Z",
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamDirectBatchRequest(
        maximumMessages: 1,
        maximumBufferedBytes: 1,
        startTime: "2026-09-42T00:00:00Z",
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamDirectBatchRequest(
        maximumMessages: 1,
        maximumBufferedBytes: 1,
        startTime: "2026-09-01 00:00:00",
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamDirectBatchRequest(
        maximumMessages: 1,
        maximumBufferedBytes: 1,
        startSequence: BigInt.one << 64,
      ),
      throwsArgumentError,
    );
  });

  test("direct batch failure snapshots partial messages", () {
    final source = [
      JetStreamStoredMessage(
        subject: "events.a",
        sequence: BigInt.one,
        payload: const [1],
        timestamp: "2026-09-01T00:00:00Z",
      ),
    ];
    final failure = JetStreamDirectBatchException(
      partialMessages: source,
      cause: const JetStreamDirectBatchLimitException("bytes"),
    );
    source.clear();

    expect(failure.partialMessages, hasLength(1));
    expect(failure.cause, isA<JetStreamDirectBatchLimitException>());
  });
}
