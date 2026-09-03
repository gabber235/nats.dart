import "dart:convert";
import "dart:typed_data";

import "package:nats_core/src/error.dart";

const _preamble = "NATS/1.0";

/// Immutable NATS message headers.
final class NatsHeaders {
  /// Creates validated headers from ordered [entries].
  factory NatsHeaders({
    Iterable<MapEntry<String, String>> entries = const [],
    int? statusCode,
    String? statusDescription,
  }) {
    if (statusCode != null && (statusCode < 100 || statusCode > 999)) {
      throw const NatsHeaderException("Status code must have three digits");
    }
    if (statusCode == null && statusDescription != null) {
      throw const NatsHeaderException(
        "A status description requires a status code",
      );
    }
    if (statusDescription != null) {
      _validateValue(statusDescription);
    }
    final copied = <MapEntry<String, String>>[];
    for (final entry in entries) {
      _validateName(entry.key);
      _validateValue(entry.value);
      copied.add(MapEntry(entry.key, entry.value));
    }
    return NatsHeaders._(
      List.unmodifiable(copied),
      statusCode,
      statusDescription,
    );
  }

  const NatsHeaders._(this._entries, this.statusCode, this.statusDescription);

  /// Parses a complete NATS header block.
  factory NatsHeaders.parse(Uint8List bytes) {
    late final String value;
    try {
      value = utf8.decode(bytes, allowMalformed: false);
    } on FormatException {
      throw const NatsHeaderException("Headers must contain valid UTF 8 bytes");
    }
    if (!value.endsWith("\r\n\r\n")) {
      throw const NatsHeaderException(
        "Headers must end with an empty CRLF line",
      );
    }
    final lines = value.substring(0, value.length - 4).split("\r\n");
    if (lines.isEmpty || !lines.first.startsWith(_preamble)) {
      throw const NatsHeaderException("Missing NATS/1.0 preamble");
    }

    final status = _parseStatus(lines.first);
    final entries = <MapEntry<String, String>>[];
    for (final line in lines.skip(1)) {
      if (line.isEmpty) {
        throw const NatsHeaderException("Unexpected empty header line");
      }
      if (line.startsWith(" ") || line.startsWith("\t")) {
        throw const NatsHeaderException("Folded headers are not supported");
      }
      final separator = line.indexOf(":");
      if (separator <= 0) {
        throw const NatsHeaderException("Header line is missing a name");
      }
      final name = line.substring(0, separator);
      var headerValue = line.substring(separator + 1);
      if (headerValue.startsWith(" ")) {
        headerValue = headerValue.substring(1);
      }
      entries.add(MapEntry(name, headerValue));
    }
    return NatsHeaders(
      entries: entries,
      statusCode: status.$1,
      statusDescription: status.$2,
    );
  }

  /// Response status code, when present.
  final int? statusCode;

  /// Response status description, when present.
  final String? statusDescription;

  final List<MapEntry<String, String>> _entries;

  /// Entries in their wire order.
  List<MapEntry<String, String>> get entries => _entries;

  /// Whether no user header entries exist.
  bool get isEmpty => _entries.isEmpty;

  /// Returns all values for [name], using case insensitive matching.
  List<String> values(String name) => List.unmodifiable(
    _entries
        .where((entry) => _equalsName(entry.key, name))
        .map((entry) => entry.value),
  );

  /// Returns the first value for [name], or `null`.
  String? first(String name) {
    for (final entry in _entries) {
      if (_equalsName(entry.key, name)) {
        return entry.value;
      }
    }
    return null;
  }

  /// Returns a copy with [value] appended for [name].
  NatsHeaders add(String name, String value) => NatsHeaders(
    entries: [..._entries, MapEntry(name, value)],
    statusCode: statusCode,
    statusDescription: statusDescription,
  );

  /// Returns a copy with every existing [name] replaced by one [value].
  NatsHeaders set(String name, String value) => NatsHeaders(
    entries: [
      for (final entry in _entries)
        if (!_equalsName(entry.key, name)) entry,
      MapEntry(name, value),
    ],
    statusCode: statusCode,
    statusDescription: statusDescription,
  );

  /// Returns a copy without entries named [name].
  NatsHeaders remove(String name) => NatsHeaders(
    entries: _entries.where((entry) => !_equalsName(entry.key, name)),
    statusCode: statusCode,
    statusDescription: statusDescription,
  );

  /// Encodes this value as a complete NATS header block.
  Uint8List encode() {
    final output = StringBuffer(_preamble);
    if (statusCode case final code?) {
      output.write(" $code");
      if (statusDescription case final description?) {
        output.write(" $description");
      }
    }
    output.write("\r\n");
    for (final entry in _entries) {
      output
        ..write(entry.key)
        ..write(": ")
        ..write(entry.value)
        ..write("\r\n");
    }
    output.write("\r\n");
    return Uint8List.fromList(utf8.encode(output.toString()));
  }

  static (int?, String?) _parseStatus(String line) {
    if (line == _preamble) {
      return (null, null);
    }
    if (!line.startsWith("$_preamble ")) {
      throw const NatsHeaderException("Invalid NATS header preamble");
    }
    final remainder = line.substring(_preamble.length + 1);
    final separator = remainder.indexOf(" ");
    final codeText = separator == -1
        ? remainder
        : remainder.substring(0, separator);
    if (codeText.length != 3) {
      throw const NatsHeaderException("Status code must have three digits");
    }
    final code = int.tryParse(codeText);
    if (code == null) {
      throw const NatsHeaderException("Status code must be decimal");
    }
    final description = separator == -1
        ? null
        : remainder.substring(separator + 1);
    return (code, description);
  }

  static bool _equalsName(String left, String right) =>
      left.toLowerCase() == right.toLowerCase();

  static void _validateName(String name) {
    if (name.isEmpty) {
      throw const NatsHeaderException("Header name must not be empty");
    }
    for (final unit in name.codeUnits) {
      if (unit <= 32 || unit >= 127 || unit == 58) {
        throw const NatsHeaderException(
          "Header name contains a forbidden byte",
        );
      }
    }
  }

  static void _validateValue(String value) {
    for (final unit in value.codeUnits) {
      if (unit < 32 && unit != 9 || unit == 127) {
        throw const NatsHeaderException(
          "Header value contains a forbidden byte",
        );
      }
    }
    final encoded = utf8.encode(value);
    if (utf8.decode(encoded) != value) {
      throw const NatsHeaderException("Header value contains invalid Unicode");
    }
  }
}
