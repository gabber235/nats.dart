import "dart:convert";
import "dart:typed_data";

import "package:nats_core/nats_core.dart";
import "package:nats_core/src/protocol/client_encoder.dart";
import "package:test/test.dart";

void main() {
  const encoder = ClientProtocolEncoder(4096);

  test("encodes exact Core control operations", () {
    expect(ascii.decode(encoder.ping()), "PING\r\n");
    expect(ascii.decode(encoder.pong()), "PONG\r\n");
    expect(
      ascii.decode(encoder.subscribe(subject: "events", subscriptionId: 42)),
      "SUB events 42\r\n",
    );
    expect(
      ascii.decode(
        encoder.subscribe(
          subject: "events",
          subscriptionId: 42,
          queue: "workers",
        ),
      ),
      "SUB events workers 42\r\n",
    );
    expect(ascii.decode(encoder.unsubscribe(42)), "UNSUB 42\r\n");
    expect(
      ascii.decode(encoder.unsubscribe(42, maximumDeliveries: 7)),
      "UNSUB 42 7\r\n",
    );
    expect(
      () => encoder.unsubscribe(42, maximumDeliveries: 0),
      throwsArgumentError,
    );
  });

  test("encodes binary PUB without decoding its payload", () {
    final encoded = encoder.publish(
      subject: "binary",
      payload: [0, 255, 13, 10],
    );

    expect(encoded, [
      ...ascii.encode("PUB binary 4\r\n"),
      0,
      255,
      13,
      10,
      13,
      10,
    ]);
  });

  test("encodes HPUB lengths and repeated headers", () {
    final headers = NatsHeaders(
      entries: const [MapEntry("X-Test", "one"), MapEntry("X-Test", "two")],
    );
    final headerBytes = headers.encode();
    final encoded = encoder.publish(
      subject: "events",
      replySubject: "reply",
      payload: [1, 2, 3],
      headers: headers,
    );

    expect(encoded, [
      ...ascii.encode(
        "HPUB events reply ${headerBytes.length} ${headerBytes.length + 3}\r\n",
      ),
      ...headerBytes,
      1,
      2,
      3,
      13,
      10,
    ]);
  });

  test("encodes UTF8 protocol fields without changing byte lengths", () {
    expect(
      utf8.decode(
        encoder.subscribe(subject: "상태.변경", subscriptionId: 7, queue: "작업자"),
      ),
      "SUB 상태.변경 작업자 7\r\n",
    );
    expect(
      utf8.decode(encoder.connect(const {"name": "통합 클라이언트"})),
      "CONNECT {\"name\":\"통합 클라이언트\"}\r\n",
    );
    expect(encoder.publish(subject: "상태.변경", payload: const [1]), [
      ...utf8.encode("PUB 상태.변경 1\r\n"),
      1,
      13,
      10,
    ]);
  });

  test("encodes a maximum sized frame into one exact byte buffer", () {
    final headers = NatsHeaders(entries: const [MapEntry("X-Test", "bounded")]);
    final headerBytes = headers.encode();
    final payload = Uint8List(8 * 1024 * 1024)
      ..[0] = 1
      ..[8 * 1024 * 1024 - 1] = 255;
    final encoded = encoder.publish(
      subject: "maximum",
      payload: payload,
      headers: headers,
    );
    final controlEnd = encoded.indexOf(10) + 1;
    final payloadStart = controlEnd + headerBytes.length;

    expect(encoded, isA<Uint8List>());
    expect(encoded.length, payloadStart + payload.length + 2);
    expect(encoded[payloadStart], 1);
    expect(encoded[payloadStart + payload.length - 1], 255);
    expect(encoded.sublist(encoded.length - 2), const [13, 10]);
  });

  test("rejects a combined control line above the configured bound", () {
    expect(
      () => encoder.publish(
        subject: "s" * 3000,
        replySubject: "r" * 3000,
        payload: const [],
      ),
      throwsA(isA<NatsProtocolException>()),
    );
    expect(
      () => encoder.subscribe(
        subject: "s" * 3000,
        queue: "q" * 3000,
        subscriptionId: 1,
      ),
      throwsA(isA<NatsProtocolException>()),
    );
  });
}
