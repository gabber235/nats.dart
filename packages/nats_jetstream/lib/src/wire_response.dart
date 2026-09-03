import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/timestamp.dart";

const _defaultMaximumDecimalDigits = 4096;
final _maximumUnsigned64 = (BigInt.one << 64) - BigInt.one;
final _maximumSigned64 = (BigInt.one << 63) - BigInt.one;
final _minimumSigned64 = -(BigInt.one << 63);

/// Strict decoded JetStream JSON response with exact integer semantics.
final class JetStreamWireResponse {
  JetStreamWireResponse._(this.subject, this.body);

  /// Decodes one response and translates every parser failure.
  factory JetStreamWireResponse.decode(List<int> payload, String subject) {
    try {
      return JetStreamWireResponse._(subject, decodeExactJsonObject(payload));
    } on FormatException catch (error) {
      throw JetStreamProtocolException(
        "Invalid JSON response",
        subject: subject,
        cause: error,
      );
    }
  }

  /// Request subject that produced this response.
  final String subject;

  /// Decoded root object.
  final Map<String, Object?> body;

  /// Throws the typed server API error embedded in this response.
  void throwApiError() {
    final error = optionalObject("error");
    if (error == null) {
      return;
    }
    throw JetStreamApiException(
      subject: subject,
      code: requiredBoundedInt(
        "error.code",
        minimum: 300,
        maximum: 699,
        source: error,
      ),
      errorCode: requiredBoundedInt(
        "error.err_code",
        minimum: 0,
        maximum: 65535,
        source: error,
      ),
      description: requiredString("error.description", source: error),
      responseType: optionalString("type"),
      safeResponse: _safeResponse(body),
    );
  }

  /// Reads a required object at [path].
  Map<String, Object?> requiredObject(
    String path, {
    Map<String, Object?>? source,
  }) {
    final value = _field(path, source);
    if (value is! Map<String, Object?>) {
      throw _fieldError(path, "must be an object");
    }
    return value;
  }

  /// Reads an optional object at [path].
  Map<String, Object?>? optionalObject(
    String path, {
    Map<String, Object?>? source,
  }) {
    final container = source ?? body;
    final key = _key(path);
    if (!container.containsKey(key)) {
      return null;
    }
    return requiredObject(path, source: container);
  }

  /// Reads a required string at [path].
  String requiredString(String path, {Map<String, Object?>? source}) {
    final value = _field(path, source);
    if (value is! String) {
      throw _fieldError(path, "must be a string");
    }
    return value;
  }

  /// Reads an optional string at [path].
  String? optionalString(String path, {Map<String, Object?>? source}) {
    final container = source ?? body;
    final key = _key(path);
    if (!container.containsKey(key)) {
      return null;
    }
    return requiredString(path, source: container);
  }

  /// Reads and validates one required RFC 3339 timestamp at [path].
  String requiredRfc3339(String path, {Map<String, Object?>? source}) {
    final value = requiredString(path, source: source);
    try {
      return validateJetStreamRfc3339(value, path);
    } on ArgumentError catch (error) {
      throw JetStreamProtocolException(
        "Field $path must be an RFC 3339 timestamp",
        subject: subject,
        fieldPath: path,
        cause: error,
      );
    }
  }

  /// Reads and validates one optional RFC 3339 timestamp at [path].
  String? optionalRfc3339(String path, {Map<String, Object?>? source}) {
    final container = source ?? body;
    final key = _key(path);
    if (!container.containsKey(key)) {
      return null;
    }
    return requiredRfc3339(path, source: container);
  }

  /// Reads a required integer within the complete uint64 range.
  BigInt requiredUnsigned(String path, {Map<String, Object?>? source}) {
    final value = _field(path, source);
    if (value is! BigInt || value.isNegative || value > _maximumUnsigned64) {
      throw _fieldError(path, "must be a uint64");
    }
    return value;
  }

  /// Reads a required integer within the complete uint64 range.
  BigInt requiredUint64(String path, {Map<String, Object?>? source}) =>
      requiredUnsigned(path, source: source);

  /// Reads a required positive integer within the uint64 range.
  BigInt requiredPositiveUint64(String path, {Map<String, Object?>? source}) {
    final value = requiredUint64(path, source: source);
    if (value == BigInt.zero) {
      throw _fieldError(path, "must be a positive uint64");
    }
    return value;
  }

  /// Reads a required integer within the complete signed int64 range.
  BigInt requiredSignedInt64(String path, {Map<String, Object?>? source}) {
    final value = _field(path, source);
    if (value is! BigInt ||
        value < _minimumSigned64 ||
        value > _maximumSigned64) {
      throw _fieldError(path, "must be a signed int64");
    }
    return value;
  }

  /// Reads a required nonnegative integer within the signed int64 range.
  BigInt requiredNonNegativeInt64(String path, {Map<String, Object?>? source}) {
    final value = requiredSignedInt64(path, source: source);
    if (value.isNegative) {
      throw _fieldError(path, "must be a nonnegative signed int64");
    }
    return value;
  }

  /// Reads an optional integer within the complete uint64 range.
  BigInt? optionalUnsigned(String path, {Map<String, Object?>? source}) {
    final container = source ?? body;
    final key = _key(path);
    if (!container.containsKey(key)) {
      return null;
    }
    return requiredUnsigned(path, source: container);
  }

  /// Reads an optional nonnegative signed int64.
  BigInt? optionalNonNegativeInt64(
    String path, {
    Map<String, Object?>? source,
  }) {
    final container = source ?? body;
    final key = _key(path);
    if (!container.containsKey(key)) {
      return null;
    }
    return requiredNonNegativeInt64(path, source: container);
  }

  /// Reads a required small integer at [path].
  int requiredInt(String path, {Map<String, Object?>? source}) {
    final value = requiredSignedInt64(path, source: source);
    final result = value.toInt();
    if (BigInt.from(result) != value) {
      throw _fieldError(path, "is outside the Dart integer range");
    }
    return result;
  }

  /// Reads a required signed int64 constrained to an exact small range.
  int requiredBoundedInt(
    String path, {
    required int minimum,
    required int maximum,
    Map<String, Object?>? source,
  }) {
    if (minimum > maximum) {
      throw ArgumentError.value(minimum, "minimum", "Must not exceed maximum");
    }
    final value = requiredSignedInt64(path, source: source);
    if (value < BigInt.from(minimum) || value > BigInt.from(maximum)) {
      throw _fieldError(path, "must be between $minimum and $maximum");
    }
    return value.toInt();
  }

  /// Reads an optional small integer.
  int? optionalInt(String path, {Map<String, Object?>? source}) {
    final container = source ?? body;
    final key = _key(path);
    if (!container.containsKey(key)) {
      return null;
    }
    return requiredInt(path, source: container);
  }

  /// Reads an optional nonnegative signed int64 as a Dart integer.
  int? optionalNonNegativeInt(String path, {Map<String, Object?>? source}) {
    final container = source ?? body;
    final key = _key(path);
    if (!container.containsKey(key)) {
      return null;
    }
    final value = requiredNonNegativeInt64(path, source: container);
    final result = value.toInt();
    if (BigInt.from(result) != value) {
      throw _fieldError(path, "is outside the Dart integer range");
    }
    return result;
  }

  /// Reads an optional signed int64 constrained to an exact small range.
  int? optionalBoundedInt(
    String path, {
    required int minimum,
    required int maximum,
    Map<String, Object?>? source,
  }) {
    final container = source ?? body;
    final key = _key(path);
    if (!container.containsKey(key)) {
      return null;
    }
    return requiredBoundedInt(
      path,
      minimum: minimum,
      maximum: maximum,
      source: container,
    );
  }

  /// Reads a required boolean.
  bool requiredBoolean(String path, {Map<String, Object?>? source}) {
    final value = _field(path, source);
    if (value is! bool) {
      throw _fieldError(path, "must be a boolean");
    }
    return value;
  }

  /// Reads an optional boolean at [path], defaulting only when absent.
  bool optionalBoolean(String path, {Map<String, Object?>? source}) {
    final container = source ?? body;
    final key = _key(path);
    if (!container.containsKey(key)) {
      return false;
    }
    final value = container[key];
    if (value is! bool) {
      throw _fieldError(path, "must be a boolean");
    }
    return value;
  }

  /// Reads an optional exact nanosecond duration at [path].
  JetStreamNanoseconds? optionalNanoseconds(
    String path, {
    Map<String, Object?>? source,
  }) {
    final container = source ?? body;
    final key = _key(path);
    if (!container.containsKey(key)) {
      return null;
    }
    return JetStreamNanoseconds(
      requiredNonNegativeInt64(path, source: container),
    );
  }

  /// Reads an optional positive signed int64 nanosecond duration.
  JetStreamNanoseconds? optionalPositiveNanoseconds(
    String path, {
    Map<String, Object?>? source,
  }) {
    final value = optionalNanoseconds(path, source: source);
    if (value != null && value.nanoseconds == BigInt.zero) {
      throw _fieldError(path, "must be a positive signed int64 duration");
    }
    return value;
  }

  /// Reads a required exact decimal string at [path].
  BigInt requiredDecimal(
    String path, {
    Map<String, Object?>? source,
    int maximumDigits = _defaultMaximumDecimalDigits,
  }) {
    if (maximumDigits < 1) {
      throw ArgumentError.value(
        maximumDigits,
        "maximumDigits",
        "Must be positive",
      );
    }
    final value = requiredString(path, source: source);
    try {
      final signLength =
          value.isNotEmpty &&
              (value.codeUnitAt(0) == 43 || value.codeUnitAt(0) == 45)
          ? 1
          : 0;
      final digitCount = value.length - signLength;
      if (digitCount < 1 || digitCount > maximumDigits) {
        throw const FormatException("Invalid decimal grammar");
      }
      for (var index = signLength; index < value.length; index += 1) {
        final unit = value.codeUnitAt(index);
        if (unit < 48 || unit > 57) {
          throw const FormatException("Invalid decimal grammar");
        }
      }
      return BigInt.parse(value);
    } on FormatException catch (error) {
      throw JetStreamProtocolException(
        "Field $path must be an exact decimal string",
        subject: subject,
        fieldPath: path,
        cause: error,
      );
    }
  }

  /// Reads a required string list at [path].
  List<String> requiredStringList(String path, {Map<String, Object?>? source}) {
    final value = _field(path, source);
    if (value is! List<Object?> || value.any((entry) => entry is! String)) {
      throw _fieldError(path, "must be a string list");
    }
    return List.unmodifiable(value.cast<String>());
  }

  /// Reads a required object list.
  List<Map<String, Object?>> requiredObjectList(
    String path, {
    Map<String, Object?>? source,
  }) {
    final value = _field(path, source);
    if (value is! List<Object?> || value.any((entry) => entry is! Map)) {
      throw _fieldError(path, "must be an object list");
    }
    return List.unmodifiable(value.cast<Map<String, Object?>>());
  }

  /// Reads an optional object list, defaulting only when absent.
  List<Map<String, Object?>> optionalObjectList(
    String path, {
    Map<String, Object?>? source,
  }) {
    final container = source ?? body;
    final key = _key(path);
    if (!container.containsKey(key)) {
      return const [];
    }
    return requiredObjectList(path, source: container);
  }

  /// Reads an optional uint64 list.
  List<BigInt> optionalUnsignedList(
    String path, {
    Map<String, Object?>? source,
  }) {
    final container = source ?? body;
    final key = _key(path);
    if (!container.containsKey(key)) {
      return const [];
    }
    final value = container[key];
    if (value is! List<Object?> ||
        value.any(
          (entry) =>
              entry is! BigInt ||
              entry.isNegative ||
              entry > _maximumUnsigned64,
        )) {
      throw _fieldError(path, "must be a uint64 list");
    }
    return List.unmodifiable(value.cast<BigInt>());
  }

  /// Reads an optional nonnegative signed int64 list.
  List<BigInt> optionalNonNegativeInt64List(
    String path, {
    Map<String, Object?>? source,
  }) {
    final container = source ?? body;
    final key = _key(path);
    if (!container.containsKey(key)) {
      return const [];
    }
    final value = container[key];
    if (value is! List<Object?> ||
        value.any(
          (entry) =>
              entry is! BigInt || entry.isNegative || entry > _maximumSigned64,
        )) {
      throw _fieldError(path, "must be a nonnegative signed int64 list");
    }
    return List.unmodifiable(value.cast<BigInt>());
  }

  /// Reads an optional map of uint64 values.
  Map<String, BigInt> optionalUnsignedMap(
    String path, {
    Map<String, Object?>? source,
  }) {
    final container = source ?? body;
    final key = _key(path);
    if (!container.containsKey(key)) {
      return const {};
    }
    final value = container[key];
    if (value is! Map<String, Object?> ||
        value.values.any(
          (entry) =>
              entry is! BigInt ||
              entry.isNegative ||
              entry > _maximumUnsigned64,
        )) {
      throw _fieldError(path, "must be a uint64 map");
    }
    return Map.unmodifiable(value.cast<String, BigInt>());
  }

  /// Reads an optional string list, defaulting only when absent.
  List<String> optionalStringList(String path, {Map<String, Object?>? source}) {
    final container = source ?? body;
    final key = _key(path);
    if (!container.containsKey(key)) {
      return const [];
    }
    return requiredStringList(path, source: container);
  }

  /// Reads an optional string map, defaulting only when absent.
  Map<String, String> optionalStringMap(
    String path, {
    Map<String, Object?>? source,
  }) {
    final container = source ?? body;
    final key = _key(path);
    if (!container.containsKey(key)) {
      return const {};
    }
    final value = container[key];
    if (value is! Map<String, Object?> ||
        value.values.any((entry) => entry is! String)) {
      throw _fieldError(path, "must be a string map");
    }
    return Map.unmodifiable(value.cast<String, String>());
  }

  Object? _field(String path, Map<String, Object?>? source) {
    final container = source ?? body;
    final key = _key(path);
    if (!container.containsKey(key)) {
      throw _fieldError(path, "is required");
    }
    return container[key];
  }

  String _key(String path) => path.substring(path.lastIndexOf(".") + 1);

  JetStreamProtocolException _fieldError(String path, String detail) {
    return JetStreamProtocolException(
      "Field $path $detail",
      subject: subject,
      fieldPath: path,
    );
  }
}

String _safeResponse(Map<String, Object?> body) {
  return _BoundedSafeJsonWriter(2048).encode(body);
}

final class _BoundedSafeJsonWriter {
  _BoundedSafeJsonWriter(this.limit);

  final int limit;
  final StringBuffer _output = StringBuffer();
  var _length = 0;

  String encode(Object? value) {
    _value(value);
    return _output.toString();
  }

  bool get _full => _length >= limit;

  void _value(Object? value) {
    if (_full) return;
    switch (value) {
      case null || bool() || int() || double() || BigInt():
        _write(value.toString());
      case String():
        _string(value);
      case List<Object?>():
        _write("[");
        for (var index = 0; index < value.length && !_full; index += 1) {
          if (index != 0) _write(",");
          _value(value[index]);
        }
        _write("]");
      case Map<String, Object?>():
        _write("{");
        var first = true;
        for (final entry in value.entries) {
          if (_full) break;
          if (!first) _write(",");
          first = false;
          _string(entry.key);
          _write(":");
          if (_isSensitive(entry.key)) {
            _string("<redacted>");
          } else {
            _value(entry.value);
          }
        }
        _write("}");
      default:
        _string("<unsupported>");
    }
  }

  void _string(String value) {
    _write("\"");
    for (var index = 0; index < value.length && !_full; index += 1) {
      final unit = value.codeUnitAt(index);
      switch (unit) {
        case 8:
          _write(r"\b");
        case 9:
          _write(r"\t");
        case 10:
          _write(r"\n");
        case 12:
          _write(r"\f");
        case 13:
          _write(r"\r");
        case 34:
          _write(String.fromCharCodes(const [92, 34]));
        case 92:
          _write(r"\\");
        default:
          if (unit < 32) {
            _write("\\u${unit.toRadixString(16).padLeft(4, "0")}");
          } else {
            _write(String.fromCharCode(unit));
          }
      }
    }
    _write("\"");
  }

  void _write(String value) {
    final remaining = limit - _length;
    if (remaining <= 0) return;
    final accepted = value.length <= remaining
        ? value
        : value.substring(0, remaining);
    _output.write(accepted);
    _length += accepted.length;
  }
}

bool _isSensitive(String key) {
  if (key.length > 128) return true;
  final normalized = key.toLowerCase().replaceAll(RegExp(r"[^a-z0-9]"), "");
  return normalized.contains("password") ||
      normalized.contains("token") ||
      normalized.contains("secret") ||
      normalized.contains("credential") ||
      normalized.contains("privatekey") ||
      normalized == "jwt" ||
      normalized == "seed" ||
      normalized == "nkey" ||
      normalized == "pass" ||
      normalized == "pwd" ||
      normalized == "sig" ||
      normalized == "signature";
}
