import "dart:convert";

const _maximumInt32 = 2147483647;

final _function = RegExp(r"^\{\{\s*([A-Za-z]+)\s*\((.*)\)\s*\}\}$");
final _separator = RegExp(r",\s*");
final _asciiInteger = RegExp(r"^[+-]?[0-9]+$");
final _subjectWhitespace = RegExp(r"[ \t\n\f\r]");

/// Validates the subject transform grammar implemented by NATS Server 2.14.
void validateJetStreamSubjectTransform(String source, String destination) {
  final sourceInfo = _validateSource(source.isEmpty ? ">" : source);
  if (destination.isEmpty) {
    return;
  }
  _validateUnicode(destination);
  final wildcardCount = sourceInfo.wildcardCount;
  final sourceHasFullWildcard = sourceInfo.hasFullWildcard;
  final destinationTokens = destination.split(".");
  var destinationHasFullWildcard = false;

  for (var index = 0; index < destinationTokens.length; index += 1) {
    final token = destinationTokens[index];
    if (token.isEmpty) {
      throw ArgumentError.value(
        destination,
        "destination",
        "Contains an empty token",
      );
    }
    if (token == ">") {
      if (index != destinationTokens.length - 1) {
        throw ArgumentError.value(
          destination,
          "destination",
          "Full wildcard must be the final token",
        );
      }
      destinationHasFullWildcard = true;
      continue;
    }
    if (token == "*") {
      throw ArgumentError.value(token, "destination", "Wildcard not allowed");
    }
    final transform = _parseToken(token);
    if (transform == null) {
      _validateLiteralToken(token, destination);
      continue;
    }
    if (wildcardCount == 0 &&
        transform.kind != _TransformKind.partition &&
        transform.kind != _TransformKind.random) {
      throw ArgumentError.value(
        token,
        "destination",
        "Transform requires a source wildcard",
      );
    }
    if (wildcardCount > 0 || sourceHasFullWildcard) {
      for (final wildcardIndex in transform.wildcardIndexes) {
        if (wildcardIndex > wildcardCount) {
          throw ArgumentError.value(
            token,
            "destination",
            "Wildcard index exceeds the source wildcard count",
          );
        }
      }
    }
  }

  if (sourceHasFullWildcard != destinationHasFullWildcard) {
    throw ArgumentError.value(
      destination,
      "destination",
      "Source and destination full wildcards must match",
    );
  }
}

_Transform? _parseToken(String token) {
  if (token.length > 1 && token.startsWith(r"$")) {
    final value = token.substring(1);
    final wildcardIndex = _asciiInteger.hasMatch(value)
        ? int.tryParse(value)
        : null;
    return wildcardIndex == null
        ? null
        : _Transform(_TransformKind.wildcard, [wildcardIndex]);
  }
  if (!token.startsWith("{{") || !token.endsWith("}}")) return null;
  final match = _function.firstMatch(token);
  if (match == null) {
    throw ArgumentError.value(token, "destination", "Invalid transform");
  }
  final name = _functionName(match.group(1)!, token);
  final arguments = match.group(2)!.split(_separator);
  return switch (name) {
    "wildcard" => _wildcard(token, arguments),
    "partition" => _partition(token, arguments),
    "splitfromleft" => _indexAndInt(
      token,
      arguments,
      _TransformKind.splitFromLeft,
    ),
    "splitfromright" => _indexAndInt(
      token,
      arguments,
      _TransformKind.splitFromRight,
    ),
    "slicefromleft" => _indexAndInt(
      token,
      arguments,
      _TransformKind.sliceFromLeft,
    ),
    "slicefromright" => _indexAndInt(
      token,
      arguments,
      _TransformKind.sliceFromRight,
    ),
    "left" => _indexAndInt(token, arguments, _TransformKind.left),
    "right" => _indexAndInt(token, arguments, _TransformKind.right),
    "split" => _split(token, arguments),
    "random" => _random(token, arguments),
    _ => throw ArgumentError.value(
      token,
      "destination",
      "Unknown transform function",
    ),
  };
}

String _functionName(String value, String token) {
  for (final (pattern, name) in <(RegExp, String)>[
    (RegExp(r"^[wW]ildcard$"), "wildcard"),
    (RegExp(r"^[pP]artition$"), "partition"),
    (RegExp(r"^[sS]plit[fF]rom[lL]eft$"), "splitfromleft"),
    (RegExp(r"^[sS]plit[fF]rom[rR]ight$"), "splitfromright"),
    (RegExp(r"^[sS]lice[fF]rom[lL]eft$"), "slicefromleft"),
    (RegExp(r"^[sS]lice[fF]rom[rR]ight$"), "slicefromright"),
    (RegExp(r"^[sS]plit$"), "split"),
    (RegExp(r"^[lL]eft$"), "left"),
    (RegExp(r"^[rR]ight$"), "right"),
    (RegExp(r"^[rR]andom$"), "random"),
  ]) {
    if (pattern.hasMatch(value)) return name;
  }
  throw ArgumentError.value(token, "destination", "Unknown transform function");
}

_Transform _wildcard(String token, List<String> arguments) {
  _requireArgumentCount(token, arguments, 1);
  return _Transform(_TransformKind.wildcard, [
    _integer(arguments.single, token),
  ]);
}

_Transform _partition(String token, List<String> arguments) {
  if (arguments.isEmpty) {
    throw ArgumentError.value(token, "destination", "Not enough arguments");
  }
  final partitions = _integer(arguments.first, token);
  if (partitions > _maximumInt32) {
    throw ArgumentError.value(token, "destination", "Invalid partition count");
  }
  return _Transform(
    _TransformKind.partition,
    arguments.skip(1).map((value) => _integer(value, token)).toList(),
  );
}

_Transform _indexAndInt(
  String token,
  List<String> arguments,
  _TransformKind kind,
) {
  _requireArgumentCount(token, arguments, 2);
  final wildcardIndex = _integer(arguments.first, token);
  _signedInt32(arguments.last, token);
  return _Transform(kind, [wildcardIndex]);
}

_Transform _split(String token, List<String> arguments) {
  _requireArgumentCount(token, arguments, 2);
  final wildcardIndex = _integer(arguments.first, token);
  final delimiter = arguments.last;
  if (delimiter.contains(" ") || delimiter.contains(".")) {
    throw ArgumentError.value(token, "destination", "Invalid split delimiter");
  }
  return _Transform(_TransformKind.split, [wildcardIndex]);
}

_Transform _random(String token, List<String> arguments) {
  _requireArgumentCount(token, arguments, 1);
  final maximum = _integer(arguments.single, token);
  if (maximum > _maximumInt32) {
    throw ArgumentError.value(token, "destination", "Invalid random maximum");
  }
  return const _Transform(_TransformKind.random, []);
}

void _requireArgumentCount(String token, List<String> arguments, int expected) {
  if (arguments.length != expected) {
    throw ArgumentError.value(token, "destination", "Invalid argument count");
  }
}

int _integer(String value, String token) {
  final normalized = _trimSpaces(value);
  final parsed = _asciiInteger.hasMatch(normalized)
      ? int.tryParse(normalized)
      : null;
  if (parsed == null) {
    throw ArgumentError.value(token, "destination", "Invalid integer argument");
  }
  return parsed;
}

String _trimSpaces(String value) {
  var start = 0;
  var end = value.length;
  while (start < end && value.codeUnitAt(start) == 32) {
    start += 1;
  }
  while (end > start && value.codeUnitAt(end - 1) == 32) {
    end -= 1;
  }
  return value.substring(start, end);
}

int _signedInt32(String value, String token) {
  final parsed = _integer(value, token);
  if (parsed < -2147483648 || parsed > _maximumInt32) {
    throw ArgumentError.value(token, "destination", "Integer must fit int32");
  }
  return parsed;
}

void _validateLiteralToken(String token, String destination) {
  if (_subjectWhitespace.hasMatch(token)) {
    throw ArgumentError.value(
      destination,
      "destination",
      "Literal tokens must not contain whitespace or control characters",
    );
  }
}

({int wildcardCount, bool hasFullWildcard}) _validateSource(String source) {
  _validateUnicode(source);
  final tokens = source.split(".");
  var wildcardCount = 0;
  var hasFullWildcard = false;
  for (var index = 0; index < tokens.length; index += 1) {
    final token = tokens[index];
    if (token.isEmpty || _subjectWhitespace.hasMatch(token)) {
      throw ArgumentError.value(source, "source", "Invalid source subject");
    }
    if (token == "*") wildcardCount += 1;
    if (token == ">") {
      if (index != tokens.length - 1) {
        throw ArgumentError.value(source, "source", "Invalid source subject");
      }
      hasFullWildcard = true;
    }
  }
  return (wildcardCount: wildcardCount, hasFullWildcard: hasFullWildcard);
}

void _validateUnicode(String destination) {
  try {
    final encoded = utf8.encode(destination);
    if (utf8.decode(encoded) != destination) {
      throw const FormatException();
    }
  } on FormatException {
    throw ArgumentError.value(
      destination,
      "destination",
      "Must be valid Unicode",
    );
  }
}

enum _TransformKind {
  wildcard,
  partition,
  splitFromLeft,
  splitFromRight,
  sliceFromLeft,
  sliceFromRight,
  split,
  left,
  right,
  random,
}

final class _Transform {
  const _Transform(this.kind, this.wildcardIndexes);

  final _TransformKind kind;
  final List<int> wildcardIndexes;
}
