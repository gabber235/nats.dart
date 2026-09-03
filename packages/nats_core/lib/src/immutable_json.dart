import "dart:collection";
import "dart:convert";

const _maximumJsonNesting = 64;

/// Returns a deeply immutable snapshot of a JSON object.
Map<String, Object?> freezeJsonObject(Map<String, Object?> source) =>
    _freezeJsonObject(source, HashSet<Object>.identity(), 0);

/// Returns a deeply immutable snapshot of a JSON value.
Object? freezeJsonValue(Object? value) =>
    _freezeJsonValue(value, HashSet<Object>.identity(), 0);

/// Returns [value] after proving its UTF 16 representation has one exact UTF 8
/// encoding.
String exactUtf8String(String value, {String argumentName = "value"}) {
  final encoded = utf8.encode(value);
  if (utf8.decode(encoded, allowMalformed: false) != value) {
    throw ArgumentError.value(
      value,
      argumentName,
      "Must contain exact UTF 8 text",
    );
  }
  return value;
}

Map<String, Object?> _freezeJsonObject(
  Map<Object?, Object?> source,
  Set<Object> ancestors,
  int depth,
) {
  _enterContainer(source, ancestors, depth);
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
      snapshot[_freezeJsonString(key)] = _freezeJsonValue(
        entry.value,
        ancestors,
        depth + 1,
      );
    }
    return UnmodifiableMapView(snapshot);
  } finally {
    ancestors.remove(source);
  }
}

Object? _freezeJsonValue(Object? value, Set<Object> ancestors, int depth) =>
    switch (value) {
      null || int() || bool() => value,
      final String text => _freezeJsonString(text),
      final double number when number.isFinite => number,
      double() => throw ArgumentError.value(
        value,
        "value",
        "JSON numbers must be finite",
      ),
      final Map<Object?, Object?> map => _freezeJsonObject(
        map,
        ancestors,
        depth,
      ),
      final List<Object?> list => _freezeJsonList(list, ancestors, depth),
      _ => throw ArgumentError.value(value, "value", "Must be a JSON value"),
    };

String _freezeJsonString(String value) {
  return exactUtf8String(value);
}

List<Object?> _freezeJsonList(
  List<Object?> source,
  Set<Object> ancestors,
  int depth,
) {
  _enterContainer(source, ancestors, depth);
  try {
    return List<Object?>.unmodifiable(
      source.map((value) => _freezeJsonValue(value, ancestors, depth + 1)),
    );
  } finally {
    ancestors.remove(source);
  }
}

void _enterContainer(Object source, Set<Object> ancestors, int depth) {
  if (depth > _maximumJsonNesting) {
    throw ArgumentError.value(
      source,
      "value",
      "JSON nesting exceeds $_maximumJsonNesting levels",
    );
  }
  if (!ancestors.add(source)) {
    throw ArgumentError.value(source, "value", "JSON values must not cycle");
  }
}
