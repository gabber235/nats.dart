import "dart:convert";
import "dart:typed_data";

import "package:nats_core/src/error.dart";
import "package:nats_core/src/headers.dart";
import "package:nats_core/src/immutable_json.dart";
import "package:nats_core/src/protocol/server_operation.dart";
import "package:nats_core/src/subject.dart";

part "package:nats_core/src/protocol/server_parser_buffer.dart";

const _maximumInfoJsonNesting = 65;

/// Bounded incremental parser for operations sent by NATS Server.
final class ServerProtocolParser {
  /// Creates a parser with explicit allocation ceilings.
  ServerProtocolParser({
    this.maxControlLineBytes = 4096,
    this.maxInfoBytes = 1024 * 1024,
    this.maxPayloadBytes = 8 * 1024 * 1024,
  }) {
    if (maxControlLineBytes < 1 || maxInfoBytes < 1 || maxPayloadBytes < 0) {
      throw ArgumentError("Parser limits must be positive");
    }
  }

  /// Maximum bytes accepted before the CRLF ending a control line.
  final int maxControlLineBytes;

  /// Maximum INFO JSON bytes accepted.
  final int maxInfoBytes;

  /// Maximum declared message bytes accepted.
  final int maxPayloadBytes;

  final _buffer = _ChunkBuffer();
  _PendingMessage? _pending;

  /// Bytes retained for an incomplete operation.
  int get bufferedBytes => _buffer.length;

  /// Adds an arbitrary byte [chunk] and returns every completed operation.
  List<ServerOperation> add(List<int> chunk) {
    final operations = <ServerOperation>[];
    var offset = 0;
    while (offset < chunk.length) {
      final quota = _inputQuota();
      var count = chunk.length - offset;
      if (count > quota) count = quota;
      if (count > 8192) count = 8192;
      final end = offset + count;
      _buffer.addRange(chunk, offset, end);
      offset = end;
      _parseAvailable(operations);
    }
    if (chunk.isEmpty) {
      _parseAvailable(operations);
    }
    return operations;
  }

  void _parseAvailable(List<ServerOperation> operations) {
    while (true) {
      final pending = _pending;
      if (pending != null) {
        final operation = _consumePayload(pending);
        if (operation == null) {
          break;
        }
        operations.add(operation);
        continue;
      }

      final lineEnd = _controlLineEnd();
      if (lineEnd == null) {
        break;
      }
      final line = _take(lineEnd);
      _discard(2);
      final operation = _parseControlLine(line);
      if (operation != null) {
        operations.add(operation);
      }
    }
  }

  /// Discards every retained partial operation.
  void reset() {
    _buffer.clear();
    _pending = null;
  }

  int _inputQuota() {
    final pending = _pending;
    if (pending != null) {
      return pending.totalLength + 2 - _buffer.length;
    }
    if (_buffer.length < 5) {
      return 5 - _buffer.length;
    }
    return _currentControlLineLimit() + 2 - _buffer.length;
  }

  int? _controlLineEnd() {
    while (true) {
      final byte = _buffer.nextScanByte();
      if (byte == null) {
        return null;
      }
      final index = _buffer.scannedBytes - 1;
      if (byte == 10) {
        if (index == 0 || _buffer.previousScanByte != 13) {
          throw const NatsProtocolException("Control line contains bare LF");
        }
        return index - 1;
      }
      if (_buffer.previousScanByte == 13) {
        throw const NatsProtocolException("Control line contains bare CR");
      }
      final maximum = _currentControlLineLimit();
      if (index >= maximum && byte != 13) {
        throw NatsProtocolException("Control line exceeds $maximum bytes");
      }
    }
  }

  int _currentControlLineLimit() {
    if (_buffer.scannedBytes < 5) {
      return maxControlLineBytes;
    }
    final isInfo =
        (_buffer.controlPrefix[0] == 73 || _buffer.controlPrefix[0] == 105) &&
        (_buffer.controlPrefix[1] == 78 || _buffer.controlPrefix[1] == 110) &&
        (_buffer.controlPrefix[2] == 70 || _buffer.controlPrefix[2] == 102) &&
        (_buffer.controlPrefix[3] == 79 || _buffer.controlPrefix[3] == 111) &&
        _buffer.controlPrefix[4] == 32;
    return isInfo ? maxInfoBytes + 5 : maxControlLineBytes;
  }

  ServerOperation? _parseControlLine(Uint8List bytes) {
    final line = _decodeUtf8(bytes, "control line");
    final separator = line.indexOf(" ");
    final operation = (separator == -1 ? line : line.substring(0, separator))
        .toUpperCase();
    final arguments = separator == -1 ? "" : line.substring(separator + 1);
    if (operation == "PING" && arguments.isEmpty) {
      return const ServerPingOperation();
    }
    if (operation == "PONG" && arguments.isEmpty) {
      return const ServerPongOperation();
    }
    if (operation == "+OK" && arguments.isEmpty) {
      return const ServerOkOperation();
    }
    if (operation == "-ERR") {
      return ServerErrorOperation(_parseError(arguments));
    }
    if (operation == "INFO" && arguments.isNotEmpty) {
      return _parseInfo(arguments);
    }
    if (operation == "MSG" && arguments.isNotEmpty) {
      _pending = _parseMessage(arguments, hasHeaders: false);
      return null;
    }
    if (operation == "HMSG" && arguments.isNotEmpty) {
      _pending = _parseMessage(arguments, hasHeaders: true);
      return null;
    }
    throw NatsProtocolException("Unknown server operation: $line");
  }

  ServerInfoOperation _parseInfo(String json) {
    if (utf8.encode(json).length > maxInfoBytes) {
      throw NatsProtocolException("INFO exceeds $maxInfoBytes bytes");
    }
    _validateInfoJsonNesting(json);
    Object? decoded;
    try {
      decoded = jsonDecode(json);
    } on FormatException {
      throw const NatsProtocolException("INFO contains invalid JSON");
    }
    if (decoded is! Map<String, Object?>) {
      throw const NatsProtocolException("INFO must contain a JSON object");
    }
    try {
      return ServerInfoOperation(freezeJsonObject(decoded));
    } on ArgumentError catch (error) {
      throw NatsProtocolException(
        "INFO contains an invalid value",
        cause: error,
      );
    }
  }

  void _validateInfoJsonNesting(String source) {
    var depth = 0;
    var inString = false;
    var escaped = false;
    for (final unit in source.codeUnits) {
      if (inString) {
        if (escaped) {
          escaped = false;
        } else if (unit == 92) {
          escaped = true;
        } else if (unit == 34) {
          inString = false;
        }
        continue;
      }
      if (unit == 34) {
        inString = true;
      } else if (unit == 123 || unit == 91) {
        depth += 1;
        if (depth > _maximumInfoJsonNesting) {
          throw const NatsProtocolException(
            "INFO JSON exceeds 64 nested container levels",
          );
        }
      } else if (unit == 125 || unit == 93) {
        depth -= 1;
      }
    }
  }

  _PendingMessage _parseMessage(String arguments, {required bool hasHeaders}) {
    final tokens = arguments.trim().split(RegExp(r"[ \t]+"));
    final expectedWithoutReply = hasHeaders ? 4 : 3;
    final expectedWithReply = expectedWithoutReply + 1;
    if (tokens.length != expectedWithoutReply &&
        tokens.length != expectedWithReply) {
      throw const NatsProtocolException("Message has invalid argument count");
    }

    final hasReply = tokens.length == expectedWithReply;
    final subject = tokens[0];
    _validateMessageSubject(subject, "message subject");
    final subscriptionId = _parsePositive(tokens[1], "subscription id");
    final replySubject = hasReply ? tokens[2] : null;
    if (replySubject != null) {
      _validateMessageSubject(replySubject, "reply subject");
    }
    final lengthIndex = hasReply ? 3 : 2;
    if (!hasHeaders) {
      final totalLength = _parseLength(tokens[lengthIndex], "payload length");
      return _PendingMessage(
        subject: subject,
        subscriptionId: subscriptionId,
        replySubject: replySubject,
        headerLength: null,
        totalLength: totalLength,
      );
    }

    final headerLength = _parseLength(tokens[lengthIndex], "header length");
    final totalLength = _parseLength(tokens[lengthIndex + 1], "total length");
    if (headerLength > totalLength) {
      throw const NatsProtocolException(
        "Header length exceeds total message length",
      );
    }
    return _PendingMessage(
      subject: subject,
      subscriptionId: subscriptionId,
      replySubject: replySubject,
      headerLength: headerLength,
      totalLength: totalLength,
    );
  }

  ServerMessageOperation? _consumePayload(_PendingMessage pending) {
    final framedLength = pending.totalLength + 2;
    if (_buffer.length < framedLength) {
      return null;
    }
    if (_buffer.byteAt(pending.totalLength) != 13 ||
        _buffer.byteAt(pending.totalLength + 1) != 10) {
      throw const NatsProtocolException("Message payload is missing CRLF");
    }
    final message = _take(pending.totalLength);
    _discard(2);
    _pending = null;

    final headerLength = pending.headerLength;
    NatsHeaders? headers;
    Uint8List payload;
    if (headerLength == null) {
      payload = message;
    } else {
      try {
        headers = NatsHeaders.parse(
          Uint8List.sublistView(message, 0, headerLength),
        );
      } on NatsHeaderException catch (error) {
        throw NatsProtocolException(
          "Invalid message headers: ${error.message}",
        );
      }
      payload = Uint8List.fromList(message.sublist(headerLength));
    }
    return ServerMessageOperation(
      subject: pending.subject,
      subscriptionId: pending.subscriptionId,
      replySubject: pending.replySubject,
      headers: headers,
      payload: payload,
    );
  }

  int _parseLength(String value, String field) {
    final result = _parseNonNegative(value, field);
    if (result > maxPayloadBytes) {
      throw NatsProtocolException("$field exceeds $maxPayloadBytes bytes");
    }
    return result;
  }

  static int _parseNonNegative(String value, String field) {
    if (value.isEmpty ||
        value.codeUnits.any((unit) => unit < 48 || unit > 57)) {
      throw NatsProtocolException("$field must be a nonnegative decimal");
    }
    final result = int.tryParse(value);
    if (result == null) {
      throw NatsProtocolException("$field exceeds the integer range");
    }
    return result;
  }

  static int _parsePositive(String value, String field) {
    final result = _parseNonNegative(value, field);
    if (result == 0) {
      throw NatsProtocolException("$field must be positive");
    }
    return result;
  }

  static void _validateMessageSubject(String value, String field) {
    try {
      validatePublishSubject(value);
    } on NatsSubjectException catch (error) {
      throw NatsProtocolException("Invalid $field", cause: error);
    }
  }

  static String _parseError(String arguments) {
    if (arguments.isEmpty) {
      return "Unknown server error";
    }
    final message = arguments.trim();
    if (message.length >= 2) {
      final first = message.codeUnitAt(0);
      final last = message.codeUnitAt(message.length - 1);
      if ((first == 39 && last == 39) || (first == 34 && last == 34)) {
        return message.substring(1, message.length - 1);
      }
    }
    return message;
  }

  static String _decodeUtf8(Uint8List bytes, String field) {
    try {
      return utf8.decode(bytes, allowMalformed: false);
    } on FormatException {
      throw NatsProtocolException("$field contains invalid UTF8 bytes");
    }
  }

  Uint8List _take(int length) {
    return _buffer.take(length);
  }

  void _discard(int length) {
    _buffer.discard(length);
  }
}
