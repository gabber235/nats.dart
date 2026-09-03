import "dart:convert";

import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/fast_batch_control.dart";
import "package:test/test.dart";

void main() {
  group("fast batch options", () {
    test("validate protocol and local flow bounds", () {
      expect(
        () => JetStreamFastBatchOptions(initialFlow: 0),
        throwsArgumentError,
      );
      expect(
        () => JetStreamFastBatchOptions(initialFlow: 65536),
        throwsArgumentError,
      );
      expect(
        () => JetStreamFastBatchOptions(maximumOutstandingAcks: 0),
        throwsArgumentError,
      );
      expect(
        () => JetStreamFastBatchOptions(maximumOutstandingAcks: 4),
        throwsArgumentError,
      );
      expect(
        () => JetStreamFastBatchOptions(maximumOutstandingBytes: 0),
        throwsArgumentError,
      );
      expect(
        () => JetStreamFastBatchOptions(maximumDiagnostics: -1),
        throwsArgumentError,
      );
      expect(
        () => JetStreamFastBatchOptions(controlTimeout: Duration.zero),
        throwsArgumentError,
      );
      expect(
        () => JetStreamFastBatchOptions(
          controlTimeout: const Duration(seconds: 10),
        ),
        throwsArgumentError,
      );
    });

    test("validates batch IDs by UTF8 bytes and subject token grammar", () {
      final sixtyFourAscii = List.filled(64, "a").join();
      expect(
        JetStreamFastBatchOptions(batchId: sixtyFourAscii).batchId,
        sixtyFourAscii,
      );
      expect(
        () => JetStreamFastBatchOptions(batchId: List.filled(65, "a").join()),
        throwsArgumentError,
      );
      expect(
        () => JetStreamFastBatchOptions(batchId: List.filled(22, "한").join()),
        throwsArgumentError,
      );
      for (final invalid in ["a.b", "a b", "a*", "a>", "a\u0000b"]) {
        expect(
          () => JetStreamFastBatchOptions(batchId: invalid),
          throwsArgumentError,
        );
      }
    });
  });

  test("acknowledgement reports truncated diagnostic totals", () {
    final acknowledgement = JetStreamFastBatchAck(
      stream: "EVENTS",
      sequence: BigInt.one,
      batchId: "batch",
      messageCount: BigInt.one,
      acknowledgedBatchSequence: BigInt.one,
      gaps: const [],
      errors: const [],
      totalGaps: 3,
      totalErrors: 2,
    );

    expect(acknowledgement.totalGaps, 3);
    expect(acknowledgement.totalErrors, 2);
    expect(acknowledgement.diagnosticsTruncated, isTrue);
    expect(
      () => JetStreamFastBatchAck(
        stream: "EVENTS",
        sequence: BigInt.one,
        batchId: "batch",
        messageCount: BigInt.one,
        acknowledgedBatchSequence: BigInt.one,
        gaps: [
          JetStreamFastBatchGap(
            expectedSequence: BigInt.zero,
            receivedSequence: BigInt.one,
          ),
        ],
        errors: const [],
        totalGaps: 0,
        totalErrors: 0,
      ),
      throwsArgumentError,
    );
  });

  test("message snapshots mutable payload", () {
    final payload = [1, 2, 3];
    final message = JetStreamFastBatchMessage("events", payload);
    payload[0] = 9;

    expect(message.payload, [1, 2, 3]);
    expect(() => message.payload[0] = 9, throwsUnsupportedError);
  });

  test("message rejects nonbyte payload values", () {
    expect(
      () => JetStreamFastBatchMessage("events", const [-1]),
      throwsFormatException,
    );
    expect(
      () => JetStreamFastBatchMessage("events", const [256]),
      throwsFormatException,
    );
    expect(JetStreamFastBatchMessage("events", const [0, 255]).payload, [
      0,
      255,
    ]);
  });

  group("fast batch control decoder", () {
    test("decodes exact cumulative flow acknowledgements", () {
      final control = decodeFastBatchControl(
        utf8.encode('{"type":"ack","seq":18446744073709551614,"msgs":15}'),
        "_INBOX.batch.ack",
      );

      expect(control, isA<FastBatchFlowAck>());
      final ack = control as FastBatchFlowAck;
      expect(ack.sequence, BigInt.parse("18446744073709551614"));
      expect(ack.messages, 15);
    });

    test("decodes gaps without turning them into acknowledgements", () {
      final control =
          decodeFastBatchControl(
                utf8.encode('{"type":"gap","last_seq":10,"seq":15}'),
                "_INBOX.batch.gap",
              )
              as FastBatchGap;

      expect(control.value.expectedSequence, BigInt.from(10));
      expect(control.value.receivedSequence, BigInt.from(15));
      expect(
        () => decodeFastBatchControl(
          utf8.encode('{"type":"gap","last_seq":15,"seq":15}'),
          "_INBOX.batch.gap",
        ),
        throwsA(isA<JetStreamProtocolException>()),
      );
    });

    test("decodes per message API errors", () {
      final control =
          decodeFastBatchControl(
                utf8.encode(
                  '{"type":"err","seq":10,"error":{"code":400,'
                  '"err_code":10071,"description":"wrong last sequence"}}',
                ),
                "_INBOX.batch.error",
              )
              as FastBatchError;

      expect(control.value.batchSequence, BigInt.from(10));
      expect(control.value.error.code, 400);
      expect(control.value.error.errorCode, 10071);
    });

    test("decodes final batch evidence", () {
      final control =
          decodeFastBatchControl(
                utf8.encode(
                  '{"stream":"ORDERS","seq":19,"batch":"batch1","count":7}',
                ),
                "_INBOX.batch.final",
              )
              as FastBatchFinalAck;

      expect(control.stream, "ORDERS");
      expect(control.sequence, BigInt.from(19));
      expect(control.batchId, "batch1");
      expect(control.messageCount, BigInt.from(7));
    });

    test("rejects zero sequence in final batch acknowledgement", () {
      expect(
        () => decodeFastBatchControl(
          utf8.encode('{"stream":"ORDERS","seq":0,"batch":"batch1","count":1}'),
          "_INBOX.batch.final",
        ),
        throwsA(
          isA<JetStreamProtocolException>().having(
            (error) => error.fieldPath,
            "fieldPath",
            "seq",
          ),
        ),
      );
    });

    test("accepts initial flow acknowledgement sequence zero", () {
      final control = decodeFastBatchControl(
        utf8.encode('{"type":"ack","seq":0,"msgs":1}'),
        "_INBOX.batch",
      );

      expect(control, isA<FastBatchFlowAck>());
      expect((control as FastBatchFlowAck).sequence, BigInt.zero);
      expect(control.messages, 1);
    });

    test("rejects uint64 overflow in every fast control counter", () {
      const overflow = "18446744073709551616";
      final cases = <(String, String)>[
        ("seq", '{"type":"ack","seq":$overflow,"msgs":1}'),
        ("last_seq", '{"type":"gap","last_seq":$overflow,"seq":2}'),
        ("seq", '{"type":"gap","last_seq":1,"seq":$overflow}'),
        (
          "seq",
          '{"type":"err","seq":$overflow,"error":'
              '{"code":400,"err_code":10071,"description":"wrong"}}',
        ),
        (
          "seq",
          '{"stream":"ORDERS","seq":$overflow,'
              '"batch":"batch1","count":1}',
        ),
        (
          "count",
          '{"stream":"ORDERS","seq":1,'
              '"batch":"batch1","count":$overflow}',
        ),
      ];

      for (final (path, payload) in cases) {
        expect(
          () => decodeFastBatchControl(utf8.encode(payload), "_INBOX.batch"),
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

    test("rejects malformed, unknown, and oversized controls", () {
      expect(
        () => decodeFastBatchControl(
          utf8.encode('{"type":"ack","seq":1,"msgs":0}'),
          "_INBOX.batch.ack",
        ),
        throwsA(isA<JetStreamProtocolException>()),
      );
      expect(
        () => decodeFastBatchControl(
          utf8.encode('{"type":"future"}'),
          "_INBOX.batch.future",
        ),
        throwsA(isA<JetStreamProtocolException>()),
      );
      expect(
        () => decodeFastBatchControl(
          List.filled(64 * 1024 + 1, 0),
          "_INBOX.batch.large",
        ),
        throwsA(isA<JetStreamProtocolException>()),
      );
    });
  });
}
