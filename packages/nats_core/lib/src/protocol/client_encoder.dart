import "dart:convert";
import "dart:typed_data";

import "package:nats_core/src/error.dart";
import "package:nats_core/src/headers.dart";

/// Encodes complete client operations without retaining caller buffers.
final class ClientProtocolEncoder {
  /// Creates a stateless encoder.
  const ClientProtocolEncoder(this.maximumControlLineBytes);

  /// Maximum encoded bytes before the CRLF terminator.
  final int maximumControlLineBytes;

  /// Encodes CONNECT JSON.
  Uint8List connect(Map<String, Object?> fields) =>
      _controlFrame("CONNECT ${jsonEncode(fields)}\r\n");

  /// Encodes PING.
  Uint8List ping() => _controlFrame("PING\r\n");

  /// Encodes PONG.
  Uint8List pong() => _controlFrame("PONG\r\n");

  /// Encodes SUB.
  Uint8List subscribe({
    required String subject,
    required int subscriptionId,
    String? queue,
  }) => _controlFrame(
    queue == null
        ? "SUB $subject $subscriptionId\r\n"
        : "SUB $subject $queue $subscriptionId\r\n",
  );

  /// Encodes immediate or delivery limited UNSUB.
  Uint8List unsubscribe(int subscriptionId, {int? maximumDeliveries}) {
    if (maximumDeliveries != null && maximumDeliveries < 1) {
      throw ArgumentError.value(
        maximumDeliveries,
        "maximumDeliveries",
        "Must be positive",
      );
    }
    return _controlFrame(
      maximumDeliveries == null
          ? "UNSUB $subscriptionId\r\n"
          : "UNSUB $subscriptionId $maximumDeliveries\r\n",
    );
  }

  /// Encodes PUB or HPUB according to [headers].
  Uint8List publish({
    required String subject,
    required List<int> payload,
    String? replySubject,
    NatsHeaders? headers,
  }) {
    final reply = replySubject == null ? "" : " $replySubject";
    if (headers == null) {
      return _payloadFrame("PUB $subject$reply ${payload.length}\r\n", payload);
    }
    final encodedHeaders = headers.encode();
    final totalLength = encodedHeaders.length + payload.length;
    return _payloadFrame(
      "HPUB $subject$reply ${encodedHeaders.length} $totalLength\r\n",
      payload,
      prefix: encodedHeaders,
    );
  }

  Uint8List _controlFrame(String value) => _controlBytes(value);

  Uint8List _payloadFrame(
    String control,
    List<int> payload, {
    List<int> prefix = const [],
  }) {
    final controlBytes = _controlBytes(control);
    final frame = Uint8List(
      controlBytes.length + prefix.length + payload.length + 2,
    );
    var offset = 0;
    frame.setRange(offset, offset + controlBytes.length, controlBytes);
    offset += controlBytes.length;
    frame.setRange(offset, offset + prefix.length, prefix);
    offset += prefix.length;
    frame.setRange(offset, offset + payload.length, payload);
    frame
      ..[frame.length - 2] = 13
      ..[frame.length - 1] = 10;
    return frame;
  }

  Uint8List _controlBytes(String value) {
    final bytes = Uint8List.fromList(utf8.encode(value));
    final lengthWithoutTerminator = bytes.length >= 2 ? bytes.length - 2 : 0;
    if (lengthWithoutTerminator > maximumControlLineBytes) {
      throw NatsProtocolException(
        "Client control line exceeds $maximumControlLineBytes bytes",
      );
    }
    return bytes;
  }
}
