import "dart:collection";
import "dart:convert";
import "dart:typed_data";

const _maximumIntegerDigits = 20;
const _maximumNumberCharacters = 128;

/// Decodes JSON while preserving every integer token as an exact [BigInt].
Map<String, Object?> decodeExactJsonObject(
  List<int> bytes, {
  int maxBytes = 8 * 1024 * 1024,
}) {
  if (bytes.length > maxBytes) {
    throw const FormatException("JSON response exceeds the byte limit");
  }
  final source = utf8.decode(bytes, allowMalformed: false);
  final decoded = _ExactJsonParser(source).parse();
  if (decoded is! Map<String, Object?>) {
    throw const FormatException("JSON root must be an object");
  }
  return decoded;
}

/// Encodes JSON and preserves [BigInt] values as integer number tokens.
Uint8List encodeExactJson(Object? value) {
  final output = StringBuffer();
  _writeJson(output, value, 0);
  return Uint8List.fromList(utf8.encode(output.toString()));
}

/// Creates a deeply immutable exact JSON object snapshot.
Map<String, Object?> immutableExactJsonObject(Map<String, Object?> source) {
  return _immutableJsonObject(source, HashSet<Object>.identity(), 0);
}

Map<String, Object?> _immutableJsonObject(
  Map<Object?, Object?> source,
  Set<Object> ancestors,
  int depth,
) {
  _enterJsonContainer(source, ancestors, depth);
  try {
    final snapshot = <String, Object?>{};
    for (final entry in source.entries) {
      final key = entry.key;
      if (key is! String) {
        throw ArgumentError.value(
          key,
          "key",
          "JSON object keys must be strings",
        );
      }
      _requireExactUtf8Argument(key, "key");
      snapshot[key] = _immutableJson(entry.value, ancestors, depth + 1);
    }
    return Map.unmodifiable(snapshot);
  } finally {
    ancestors.remove(source);
  }
}

Object? _immutableJson(Object? value, Set<Object> ancestors, int depth) =>
    switch (value) {
      null || bool() || int() || BigInt() => value,
      final String text => _requireExactUtf8Argument(text, "value"),
      final double number when number.isFinite => number,
      double() => throw ArgumentError.value(
        value,
        "value",
        "JSON numbers must be finite",
      ),
      final List<Object?> list => _immutableJsonList(list, ancestors, depth),
      final Map<Object?, Object?> map => _immutableJsonObject(
        map,
        ancestors,
        depth,
      ),
      _ => throw ArgumentError.value(
        value,
        "value",
        "Must be an exact JSON value",
      ),
    };

List<Object?> _immutableJsonList(
  List<Object?> source,
  Set<Object> ancestors,
  int depth,
) {
  _enterJsonContainer(source, ancestors, depth);
  try {
    return List<Object?>.unmodifiable(
      source.map((value) => _immutableJson(value, ancestors, depth + 1)),
    );
  } finally {
    ancestors.remove(source);
  }
}

void _enterJsonContainer(Object source, Set<Object> ancestors, int depth) {
  if (depth > 64) {
    throw ArgumentError.value(
      source,
      "value",
      "JSON nesting exceeds 64 levels",
    );
  }
  if (!ancestors.add(source)) {
    throw ArgumentError.value(source, "value", "JSON values must not cycle");
  }
}

void _writeJson(StringBuffer output, Object? value, int depth) {
  if (depth > 64) {
    throw JsonUnsupportedObjectError("JSON nesting exceeds 64 levels");
  }
  switch (value) {
    case null:
      output.write("null");
    case bool() || int() || double():
      output.write(jsonEncode(value));
    case BigInt():
      if (value.abs().bitLength > 67 ||
          value.toString().replaceFirst("-", "").length >
              _maximumIntegerDigits) {
        throw JsonUnsupportedObjectError(
          "JSON integers may contain at most $_maximumIntegerDigits digits",
        );
      }
      output.write(value);
    case String():
      _requireExactUtf8Json(value);
      output.write(jsonEncode(value));
    case List<Object?>():
      output.write("[");
      for (var index = 0; index < value.length; index += 1) {
        if (index != 0) {
          output.write(",");
        }
        _writeJson(output, value[index], depth + 1);
      }
      output.write("]");
    case Map<String, Object?>():
      output.write("{");
      var first = true;
      for (final entry in value.entries) {
        _requireExactUtf8Json(entry.key);
        if (!first) {
          output.write(",");
        }
        first = false;
        output
          ..write(jsonEncode(entry.key))
          ..write(":");
        _writeJson(output, entry.value, depth + 1);
      }
      output.write("}");
    default:
      throw JsonUnsupportedObjectError(value);
  }
}

String _requireExactUtf8Argument(String value, String parameter) {
  if (!_hasExactUtf8Representation(value)) {
    throw ArgumentError.value(
      value,
      parameter,
      "JSON strings must contain exact UTF 8 text",
    );
  }
  return value;
}

void _requireExactUtf8Json(String value) {
  if (!_hasExactUtf8Representation(value)) {
    throw JsonUnsupportedObjectError(
      "JSON strings must contain exact UTF 8 text",
    );
  }
}

bool _hasExactUtf8Representation(String value) {
  final bytes = utf8.encode(value);
  return utf8.decode(bytes) == value;
}

final class _ExactJsonParser {
  _ExactJsonParser(this.source);

  final String source;
  var _index = 0;

  Object? parse() {
    final value = _value(0);
    _whitespace();
    if (_index != source.length) {
      throw FormatException("Unexpected trailing JSON", source, _index);
    }
    return value;
  }

  Object? _value(int depth) {
    if (depth > 64) {
      throw FormatException("JSON nesting exceeds 64 levels", source, _index);
    }
    _whitespace();
    if (_index == source.length) {
      throw FormatException("Unexpected end of JSON", source, _index);
    }
    return switch (source.codeUnitAt(_index)) {
      34 => _string(),
      91 => _array(depth + 1),
      123 => _object(depth + 1),
      102 => _literal("false", false),
      110 => _literal("null", null),
      116 => _literal("true", true),
      45 => _number(),
      final unit when unit >= 48 && unit <= 57 => _number(),
      _ => throw FormatException("Unexpected JSON token", source, _index),
    };
  }

  List<Object?> _array(int depth) {
    _index += 1;
    final result = <Object?>[];
    _whitespace();
    if (_take(93)) {
      return result;
    }
    while (true) {
      result.add(_value(depth));
      _whitespace();
      if (_take(93)) {
        return result;
      }
      _expect(44);
    }
  }

  Map<String, Object?> _object(int depth) {
    _index += 1;
    final result = <String, Object?>{};
    _whitespace();
    if (_take(125)) {
      return result;
    }
    while (true) {
      _whitespace();
      if (_index == source.length || source.codeUnitAt(_index) != 34) {
        throw FormatException("Object key must be a string", source, _index);
      }
      final key = _string();
      if (result.containsKey(key)) {
        throw FormatException("Duplicate object key: $key", source, _index);
      }
      _whitespace();
      _expect(58);
      result[key] = _value(depth);
      _whitespace();
      if (_take(125)) {
        return result;
      }
      _expect(44);
    }
  }

  String _string() {
    final start = _index;
    _index += 1;
    while (_index < source.length) {
      final unit = source.codeUnitAt(_index);
      if (unit == 34) {
        _index += 1;
        final decoded = jsonDecode(source.substring(start, _index));
        if (decoded is String) {
          if (!_hasExactUtf8Representation(decoded)) {
            throw FormatException(
              "JSON string is not exact UTF 8 text",
              source,
              start,
            );
          }
          return decoded;
        }
        throw FormatException("Invalid JSON string", source, start);
      }
      if (unit == 92) {
        _index += 2;
      } else {
        _index += 1;
      }
    }
    throw FormatException("Unterminated JSON string", source, start);
  }

  Object _number() {
    final start = _index;
    _take(45);
    if (_take(48)) {
      if (_isDigit(_peek())) {
        throw FormatException("Leading zero in JSON number", source, _index);
      }
    } else {
      _digits(required: true);
    }
    var integral = true;
    if (_take(46)) {
      integral = false;
      _digits(required: true);
    }
    final exponent = _peek();
    if (exponent == 69 || exponent == 101) {
      integral = false;
      _index += 1;
      final sign = _peek();
      if (sign == 43 || sign == 45) {
        _index += 1;
      }
      _digits(required: true);
    }
    final tokenLength = _index - start;
    if (tokenLength > _maximumNumberCharacters) {
      throw FormatException("JSON number is too long", source, start);
    }
    if (integral) {
      final signLength = source.codeUnitAt(start) == 45 ? 1 : 0;
      if (tokenLength - signLength > _maximumIntegerDigits) {
        throw FormatException(
          "JSON integer exceeds $_maximumIntegerDigits digits",
          source,
          start,
        );
      }
      final token = source.substring(start, _index);
      return BigInt.parse(token);
    }
    final token = source.substring(start, _index);
    final value = double.parse(token);
    if (!value.isFinite) {
      throw FormatException("JSON number is not finite", source, start);
    }
    return value;
  }

  T _literal<T>(String token, T value) {
    if (!source.startsWith(token, _index)) {
      throw FormatException("Invalid JSON literal", source, _index);
    }
    _index += token.length;
    return value;
  }

  void _digits({required bool required}) {
    final start = _index;
    while (_isDigit(_peek())) {
      _index += 1;
    }
    if (required && start == _index) {
      throw FormatException("Expected a decimal digit", source, _index);
    }
  }

  void _whitespace() {
    while (true) {
      final unit = _peek();
      if (unit != 9 && unit != 10 && unit != 13 && unit != 32) {
        return;
      }
      _index += 1;
    }
  }

  bool _take(int unit) {
    if (_peek() != unit) {
      return false;
    }
    _index += 1;
    return true;
  }

  void _expect(int unit) {
    if (!_take(unit)) {
      throw FormatException("Unexpected JSON token", source, _index);
    }
  }

  int _peek() => _index == source.length ? -1 : source.codeUnitAt(_index);

  bool _isDigit(int unit) => unit >= 48 && unit <= 57;
}
