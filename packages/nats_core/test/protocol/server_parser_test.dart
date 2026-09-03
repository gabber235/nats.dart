import "dart:convert";
import "dart:math";
import "dart:typed_data";

import "package:nats_core/nats_core.dart";
import "package:nats_core/src/protocol/server_operation.dart";
import "package:nats_core/src/protocol/server_parser.dart";
import "package:test/test.dart";

void main() {
  group("ServerProtocolParser", () {
    test("parses coalesced operations", () {
      final parser = ServerProtocolParser();
      final operations = parser.add(
        ascii.encode(
          "INFO {\"server_id\":\"s1\",\"headers\":true}\r\n"
          "PING\r\nPONG\r\n+OK\r\n-ERR 'Authorization Violation'\r\n",
        ),
      );

      expect(operations, hasLength(5));
      expect(operations[0], isA<ServerInfoOperation>());
      final info = operations[0] as ServerInfoOperation;
      expect(info.fields["server_id"], "s1");
      expect(info.fields["headers"], isTrue);
      expect(operations[1], isA<ServerPingOperation>());
      expect(operations[2], isA<ServerPongOperation>());
      expect(operations[3], isA<ServerOkOperation>());
      expect(
        (operations[4] as ServerErrorOperation).message,
        "Authorization Violation",
      );
    });

    test("parses binary MSG under every two part split", () {
      final bytes = Uint8List.fromList([
        ...ascii.encode("MSG binary 42 replies.here 5\r\n"),
        0,
        255,
        13,
        10,
        1,
        13,
        10,
      ]);

      for (var split = 0; split <= bytes.length; split += 1) {
        final parser = ServerProtocolParser();
        final operations = <ServerOperation>[
          ...parser.add(bytes.sublist(0, split)),
          ...parser.add(bytes.sublist(split)),
        ];
        expect(operations, hasLength(1), reason: "split $split");
        final message = operations.single as ServerMessageOperation;
        expect(message.subject, "binary", reason: "split $split");
        expect(message.subscriptionId, 42, reason: "split $split");
        expect(message.replySubject, "replies.here", reason: "split $split");
        expect(message.payload, [0, 255, 13, 10, 1], reason: "split $split");
        expect(parser.bufferedBytes, 0, reason: "split $split");
      }
    });

    test("parses HMSG one byte at a time", () {
      final headers = NatsHeaders(
        entries: const [MapEntry("Trace", "one"), MapEntry("trace", "two")],
        statusCode: 200,
        statusDescription: "OK",
      ).encode();
      final payload = Uint8List.fromList([0, 1, 2, 255]);
      final total = headers.length + payload.length;
      final bytes = Uint8List.fromList([
        ...ascii.encode("HMSG events 7 reply ${headers.length} $total\r\n"),
        ...headers,
        ...payload,
        13,
        10,
      ]);
      final parser = ServerProtocolParser();
      final operations = <ServerOperation>[];

      for (final byte in bytes) {
        operations.addAll(parser.add([byte]));
      }

      final message = operations.single as ServerMessageOperation;
      expect(message.subject, "events");
      expect(message.subscriptionId, 7);
      expect(message.replySubject, "reply");
      expect(message.headers?.statusCode, 200);
      expect(message.headers?.values("TRACE"), ["one", "two"]);
      expect(message.payload, payload);
    });

    test("parses a large payload delivered one byte at a time", () {
      const payloadLength = 128 * 1024;
      final frame = Uint8List.fromList([
        ...ascii.encode("MSG fragmented 9 $payloadLength\r\n"),
        ...List<int>.generate(payloadLength, (index) => index & 255),
        13,
        10,
      ]);
      final parser = ServerProtocolParser();
      final operations = <ServerOperation>[];

      for (final byte in frame) {
        operations.addAll(parser.add([byte]));
      }

      final message = operations.single as ServerMessageOperation;
      expect(message.payload, hasLength(payloadLength));
      expect(message.payload[0], 0);
      expect(message.payload[65535], 255);
      expect(message.payload.last, 255);
      expect(parser.bufferedBytes, 0);
    });

    test("parses many coalesced frames without retaining consumed bytes", () {
      const frameCount = 100000;
      final parser = ServerProtocolParser();

      final operations = parser.add(
        ascii.encode(List.filled(frameCount, "PING\r\n").join()),
      );

      expect(operations, hasLength(frameCount));
      expect(operations, everyElement(isA<ServerPingOperation>()));
      expect(parser.bufferedBytes, 0);
    });

    test("accepts server whitespace and an empty reply placeholder", () {
      final header = ascii.encode("NATS/1.0\r\n\r\n");
      final parser = ServerProtocolParser();
      final operations = parser.add([
        ...ascii.encode("HMSG inbox 7  ${header.length} ${header.length}\r\n"),
        ...header,
        13,
        10,
      ]);

      final message = operations.single as ServerMessageOperation;
      expect(message.subject, "inbox");
      expect(message.subscriptionId, 7);
      expect(message.replySubject, isNull);
      expect(message.payload, isEmpty);
    });

    test("seeded chunk partitions preserve all messages", () {
      final bytes = Uint8List.fromList([
        ...ascii.encode("PING\r\nMSG a 1 0\r\n\r\nMSG b 2 3\r\n"),
        0,
        13,
        10,
        13,
        10,
        ...ascii.encode("PONG\r\n"),
      ]);

      for (var seed = 0; seed < 100; seed += 1) {
        final random = Random(seed);
        final parser = ServerProtocolParser();
        final operations = <ServerOperation>[];
        var offset = 0;
        while (offset < bytes.length) {
          final remaining = bytes.length - offset;
          final length = min(remaining, random.nextInt(8) + 1);
          operations.addAll(parser.add(bytes.sublist(offset, offset + length)));
          offset += length;
        }

        expect(operations, hasLength(4), reason: "seed $seed");
        expect(operations[0], isA<ServerPingOperation>());
        expect((operations[1] as ServerMessageOperation).payload, isEmpty);
        expect((operations[2] as ServerMessageOperation).payload, [0, 13, 10]);
        expect(operations[3], isA<ServerPongOperation>());
        expect(parser.bufferedBytes, 0);
      }
    });

    test("reset discards a partial operation", () {
      final parser = ServerProtocolParser()
        ..add(ascii.encode("MSG a 1 5\r\n12"));
      expect(parser.bufferedBytes, 2);

      parser.reset();

      expect(parser.bufferedBytes, 0);
      expect(
        parser.add(ascii.encode("PING\r\n")).single,
        isA<ServerPingOperation>(),
      );
    });

    test("rejects malformed control lines", () {
      final cases = <String>[
        "PING\n",
        "PING\rX",
        "UNKNOWN\r\n",
        "MSG a 1\r\n",
        "MSG a 1 -1\r\n",
        "MSG a 1 999999999999999999999999999999\r\n",
        "HMSG a 1 10 9\r\n",
        "MSG events.* 1 0\r\n\r\n",
        "MSG events 1 replies.> 0\r\n\r\n",
        "MSG events 0 0\r\n\r\n",
      ];

      for (final value in cases) {
        expect(
          () => ServerProtocolParser().add(ascii.encode(value)),
          throwsA(isA<NatsProtocolException>()),
          reason: value,
        );
      }
    });

    test(
      "rejects invalid UTF8 control bytes and malformed payload framing",
      () {
        expect(
          () => ServerProtocolParser().add([255, 13, 10]),
          throwsA(isA<NatsProtocolException>()),
        );
        expect(
          () => ServerProtocolParser().add(ascii.encode("MSG a 1 1\r\nxXX")),
          throwsA(isA<NatsProtocolException>()),
        );
      },
    );

    test("accepts UTF8 fields and case insensitive operation names", () {
      final parser = ServerProtocolParser();
      final operations = parser.add(
        utf8.encode(
          "info {\"name\":\"서버\"}\r\n"
          "msg 상태.변경 7 1\r\nx\r\n"
          "ping\r\n",
        ),
      );

      expect((operations[0] as ServerInfoOperation).fields["name"], "서버");
      expect((operations[1] as ServerMessageOperation).subject, "상태.변경");
      expect((operations[1] as ServerMessageOperation).payload, [120]);
      expect(operations[2], isA<ServerPingOperation>());
    });

    test("INFO requires exact Unicode strings and keys", () {
      for (final value in [
        r'{"nonce":"\ud800"}',
        r'{"nonce":"\udc00"}',
        r'{"nested":{"\ud800":"value"}}',
        r'{"nested":{"key":"\udc00"}}',
      ]) {
        expect(
          () => ServerProtocolParser().add(utf8.encode("INFO $value\r\n")),
          throwsA(isA<NatsProtocolException>()),
        );
      }
      final operation =
          ServerProtocolParser()
                  .add(
                    utf8.encode(
                      r'INFO {"value":"\ud83d\ude00"}'
                      "\r\n",
                    ),
                  )
                  .single
              as ServerInfoOperation;
      expect(operation.fields["value"], "😀");
    });

    test("applies the dedicated INFO limit beyond the control line limit", () {
      final padding = "x" * 5000;
      final parser = ServerProtocolParser();
      final operation =
          parser
                  .add(
                    utf8.encode("INFO ${jsonEncode({"value": padding})}\r\n"),
                  )
                  .single
              as ServerInfoOperation;

      expect(operation.fields["value"], padding);
    });

    test("bounds INFO nesting before JSON decoding", () {
      String infoWithLists(int count) =>
          "INFO {\"nested\":${"[" * count}0${"]" * count}}\r\n";

      final accepted = ServerProtocolParser()
          .add(ascii.encode(infoWithLists(64)))
          .single;
      expect(accepted, isA<ServerInfoOperation>());
      expect(
        () => ServerProtocolParser().add(ascii.encode(infoWithLists(65))),
        throwsA(isA<NatsProtocolException>()),
      );

      final escapedBrackets = jsonEncode({"value": "literal {[ ] }"});
      expect(
        ServerProtocolParser()
            .add(utf8.encode("INFO $escapedBrackets\r\n"))
            .single,
        isA<ServerInfoOperation>(),
      );
    });

    test("rejects a near limit deeply nested INFO promptly", () {
      const nesting = 400000;
      final frame = "INFO {\"nested\":${"[" * nesting}0${"]" * nesting}}\r\n";
      final elapsed = Stopwatch()..start();

      expect(
        () => ServerProtocolParser().add(ascii.encode(frame)),
        throwsA(isA<NatsProtocolException>()),
      );

      expect(elapsed.elapsed, lessThan(const Duration(seconds: 2)));
    });

    test("enforces control and payload bounds before allocation", () {
      expect(
        () => ServerProtocolParser(
          maxControlLineBytes: 4,
        ).add(ascii.encode("PINGX")),
        throwsA(isA<NatsProtocolException>()),
      );
      expect(
        () => ServerProtocolParser(
          maxPayloadBytes: 4,
        ).add(ascii.encode("MSG a 1 5\r\n")),
        throwsA(isA<NatsProtocolException>()),
      );
    });

    test("oversized transport chunks retain only the control bound", () {
      final parser = ServerProtocolParser(
        maxControlLineBytes: 64,
        maxInfoBytes: 128,
        maxPayloadBytes: 32,
      );

      expect(
        () => parser.add(Uint8List(1024 * 1024)..fillRange(0, 1024 * 1024, 65)),
        throwsA(isA<NatsProtocolException>()),
      );
      expect(parser.bufferedBytes, lessThanOrEqualTo(66));
    });

    test("oversized transport chunk after a tiny payload stays bounded", () {
      final parser = ServerProtocolParser(
        maxControlLineBytes: 64,
        maxInfoBytes: 128,
        maxPayloadBytes: 32,
      )..add(ascii.encode("MSG tiny 1 1\r\n"));
      final frame = Uint8List(1024 * 1024);
      frame[0] = 120;
      frame[1] = 13;
      frame[2] = 10;
      frame.fillRange(3, frame.length, 65);

      expect(() => parser.add(frame), throwsA(isA<NatsProtocolException>()));
      expect(parser.bufferedBytes, lessThanOrEqualTo(66));
    });
  });
}
