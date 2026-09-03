import "dart:convert";
import "dart:typed_data";

/// Direction of operations crossing a NATS TCP connection.
enum NatsWireDirection { client, server }

/// One complete NATS operation with its original bytes preserved.
final class NatsWireFrame {
  const NatsWireFrame({
    required this.bytes,
    required this.command,
    required this.subject,
    required this.replySubject,
    required this.subscriptionId,
    required this.headerLength,
    required this.payload,
  });

  /// Original control line, payload, and trailing CRLF.
  final Uint8List bytes;

  /// Uppercase protocol operation name.
  final String command;

  /// Message or publication subject, when present.
  final String? subject;

  /// Reply subject, when present.
  final String? replySubject;

  /// Server message subscription identifier, when present.
  final int? subscriptionId;

  /// HMSG or HPUB header byte count, when present.
  final int? headerLength;

  /// Exact operation body, including headers for HMSG and HPUB.
  final Uint8List payload;

  /// Application payload after any NATS header block.
  Uint8List get body => Uint8List.sublistView(payload, headerLength ?? 0);

  /// NATS header bytes, or an empty view for an operation without headers.
  Uint8List get headerBytes =>
      Uint8List.sublistView(payload, 0, headerLength ?? 0);
}

/// Incrementally frames one direction of a raw NATS TCP stream.
final class NatsWireFrameParser {
  /// Creates a bounded parser for [direction].
  NatsWireFrameParser(
    this.direction, {
    this.maximumControlLineBytes = 1024 * 1024 + 5,
    this.maximumPayloadBytes = 8 * 1024 * 1024,
  });

  /// Protocol direction interpreted by this parser.
  final NatsWireDirection direction;

  /// Maximum retained control line size.
  final int maximumControlLineBytes;

  /// Maximum declared operation body size.
  final int maximumPayloadBytes;

  Uint8List _buffer = Uint8List(0);

  /// Adds arbitrary bytes and returns every newly complete operation.
  List<NatsWireFrame> add(List<int> chunk) {
    if (chunk.isNotEmpty) {
      final combined = Uint8List(_buffer.length + chunk.length)
        ..setRange(0, _buffer.length, _buffer)
        ..setRange(_buffer.length, _buffer.length + chunk.length, chunk);
      _buffer = combined;
    }

    final frames = <NatsWireFrame>[];
    while (true) {
      final lineEnd = _lineEnd();
      if (lineEnd == null) break;
      final line = utf8.decode(Uint8List.sublistView(_buffer, 0, lineEnd));
      final metadata = _metadata(line);
      final bodyLength = metadata.bodyLength;
      final frameLength =
          lineEnd + 2 + bodyLength + (metadata.hasPayloadFrame ? 2 : 0);
      if (_buffer.length < frameLength) break;
      if (metadata.hasPayloadFrame &&
          (_buffer[frameLength - 2] != 13 || _buffer[frameLength - 1] != 10)) {
        throw const FormatException("NATS payload is missing trailing CRLF");
      }

      final bytes = Uint8List.fromList(
        Uint8List.sublistView(_buffer, 0, frameLength),
      ).asUnmodifiableView();
      final payloadStart = lineEnd + 2;
      final payload = Uint8List.sublistView(
        bytes,
        payloadStart,
        payloadStart + bodyLength,
      );
      frames.add(
        NatsWireFrame(
          bytes: bytes,
          command: metadata.command,
          subject: metadata.subject,
          replySubject: metadata.replySubject,
          subscriptionId: metadata.subscriptionId,
          headerLength: metadata.headerLength,
          payload: payload,
        ),
      );
      _buffer = Uint8List.fromList(Uint8List.sublistView(_buffer, frameLength));
    }
    return frames;
  }

  int? _lineEnd() {
    for (var index = 0; index < _buffer.length; index += 1) {
      final byte = _buffer[index];
      if (byte == 10) {
        if (index == 0 || _buffer[index - 1] != 13) {
          throw const FormatException("NATS control line contains bare LF");
        }
        return index - 1;
      }
      if (byte == 13 &&
          index + 1 < _buffer.length &&
          _buffer[index + 1] != 10) {
        throw const FormatException("NATS control line contains bare CR");
      }
      if (index >= maximumControlLineBytes) {
        throw FormatException(
          "NATS control line exceeds $maximumControlLineBytes bytes",
        );
      }
    }
    return null;
  }

  _FrameMetadata _metadata(String line) {
    final tokens = line.trim().split(RegExp(r"[ \t]+"));
    if (tokens.length == 1 && tokens.first.isEmpty) {
      throw const FormatException("NATS control line is empty");
    }
    final command = tokens.first.toUpperCase();
    return switch ((direction, command)) {
      (NatsWireDirection.client, "PUB") => _clientPublish(tokens, false),
      (NatsWireDirection.client, "HPUB") => _clientPublish(tokens, true),
      (NatsWireDirection.server, "MSG") => _serverMessage(tokens, false),
      (NatsWireDirection.server, "HMSG") => _serverMessage(tokens, true),
      _ => _FrameMetadata(command: command),
    };
  }

  _FrameMetadata _clientPublish(List<String> tokens, bool headers) {
    final withoutReply = headers ? 4 : 3;
    final withReply = withoutReply + 1;
    if (tokens.length != withoutReply && tokens.length != withReply) {
      throw const FormatException("NATS publish has invalid arguments");
    }
    final hasReply = tokens.length == withReply;
    final bodyLength = _length(tokens.last);
    final headerLength = headers ? _length(tokens[tokens.length - 2]) : null;
    if (headerLength != null && headerLength > bodyLength) {
      throw const FormatException("NATS header length exceeds total length");
    }
    return _FrameMetadata(
      command: headers ? "HPUB" : "PUB",
      subject: tokens[1],
      replySubject: hasReply ? tokens[2] : null,
      headerLength: headerLength,
      bodyLength: bodyLength,
      hasPayloadFrame: true,
    );
  }

  _FrameMetadata _serverMessage(List<String> tokens, bool headers) {
    final withoutReply = headers ? 5 : 4;
    final withReply = withoutReply + 1;
    if (tokens.length != withoutReply && tokens.length != withReply) {
      throw const FormatException("NATS message has invalid arguments");
    }
    final hasReply = tokens.length == withReply;
    final bodyLength = _length(tokens.last);
    final headerLength = headers ? _length(tokens[tokens.length - 2]) : null;
    if (headerLength != null && headerLength > bodyLength) {
      throw const FormatException("NATS header length exceeds total length");
    }
    return _FrameMetadata(
      command: headers ? "HMSG" : "MSG",
      subject: tokens[1],
      replySubject: hasReply ? tokens[3] : null,
      subscriptionId: _nonnegative(tokens[2]),
      headerLength: headerLength,
      bodyLength: bodyLength,
      hasPayloadFrame: true,
    );
  }

  int _length(String value) {
    final length = _nonnegative(value);
    if (length > maximumPayloadBytes) {
      throw FormatException("NATS payload exceeds $maximumPayloadBytes bytes");
    }
    return length;
  }

  int _nonnegative(String value) {
    final parsed = int.tryParse(value);
    if (parsed == null || parsed < 0) {
      throw const FormatException("NATS decimal must be nonnegative");
    }
    return parsed;
  }
}

final class _FrameMetadata {
  const _FrameMetadata({
    required this.command,
    this.subject,
    this.replySubject,
    this.subscriptionId,
    this.headerLength,
    this.bodyLength = 0,
    this.hasPayloadFrame = false,
  });

  final String command;
  final String? subject;
  final String? replySubject;
  final int? subscriptionId;
  final int? headerLength;
  final int bodyLength;
  final bool hasPayloadFrame;
}
