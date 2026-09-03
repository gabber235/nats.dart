import "dart:convert";
import "dart:typed_data";

import "package:nats_jetstream/src/byte_snapshot.dart";

/// Transforms complete logical KV keys while retaining subject hierarchy.
///
/// Implementations must preserve `*` and `>` wildcard tokens. Encoding must be
/// deterministic because stored subjects are the persistent key identity.
abstract interface class JetStreamKeyValueKeyCodec {
  /// Encodes one logical key or filter into its stored representation.
  String encode(String key);

  /// Decodes one stored key into its logical representation.
  String decode(String key);
}

/// Transforms KV payload bytes before storage and after retrieval.
///
/// Implementations may compress or encrypt values. They must not retain or
/// mutate input bytes.
abstract interface class JetStreamKeyValueValueCodec {
  /// Encodes logical bytes into stored bytes.
  List<int> encode(List<int> value);

  /// Decodes stored bytes into logical bytes.
  List<int> decode(List<int> value);
}

/// Immutable key and value transformation policy for one KV handle.
final class JetStreamKeyValueCodecs {
  /// Creates independent key and value codecs.
  const JetStreamKeyValueCodecs({
    this.key = const JetStreamKeyValueIdentityKeyCodec(),
    this.value = const JetStreamKeyValueIdentityValueCodec(),
  });

  /// Codec applied to keys, filters, and returned entry keys.
  final JetStreamKeyValueKeyCodec key;

  /// Codec applied to stored and returned values.
  final JetStreamKeyValueValueCodec value;

  /// Encodes a logical key while snapshotting the result.
  String encodeKey(String logicalKey) => key.encode(logicalKey);

  /// Decodes a stored key while snapshotting the result.
  String decodeKey(String encodedKey) => key.decode(encodedKey);

  /// Encodes logical value bytes into an immutable snapshot.
  Uint8List encodeValue(List<int> bytes) {
    final input = checkedJetStreamByteSnapshot(bytes, name: "KV value");
    return checkedJetStreamByteSnapshot(
      value.encode(input),
      name: "encoded KV value",
    );
  }

  /// Decodes stored value bytes into an immutable snapshot.
  Uint8List decodeValue(List<int> bytes) {
    final input = checkedJetStreamByteSnapshot(bytes, name: "stored KV value");
    return checkedJetStreamByteSnapshot(
      value.decode(input),
      name: "decoded KV value",
    );
  }
}

/// Identity key transformation.
final class JetStreamKeyValueIdentityKeyCodec
    implements JetStreamKeyValueKeyCodec {
  /// Creates an identity key codec.
  const JetStreamKeyValueIdentityKeyCodec();

  @override
  String encode(String key) => key;

  @override
  String decode(String key) => key;
}

/// Identity value transformation.
final class JetStreamKeyValueIdentityValueCodec
    implements JetStreamKeyValueValueCodec {
  /// Creates an identity value codec.
  const JetStreamKeyValueIdentityValueCodec();

  @override
  List<int> encode(List<int> value) => value;

  @override
  List<int> decode(List<int> value) => value;
}

/// Standard Base64 key transformation compatible with nats.js 3.4.
///
/// Each dot delimited token is encoded independently. Wildcard tokens remain
/// unchanged, so watches and filtered snapshots preserve their semantics.
final class JetStreamKeyValueBase64KeyCodec
    implements JetStreamKeyValueKeyCodec {
  /// Creates a standard Base64 key codec.
  const JetStreamKeyValueBase64KeyCodec();

  @override
  String encode(String key) =>
      _mapTokens(key, (token) => base64.encode(utf8.encode(token)));

  @override
  String decode(String key) => _mapTokens(
    key,
    (token) => utf8.decode(base64.decode(token), allowMalformed: false),
  );
}

/// Standard Base64 payload transformation.
final class JetStreamKeyValueBase64ValueCodec
    implements JetStreamKeyValueValueCodec {
  /// Creates a standard Base64 value codec.
  const JetStreamKeyValueBase64ValueCodec();

  @override
  List<int> encode(List<int> value) => utf8.encode(base64.encode(value));

  @override
  List<int> decode(List<int> value) =>
      base64.decode(utf8.decode(value, allowMalformed: false));
}

/// Converts path separators to NATS key hierarchy tokens.
///
/// A leading separator becomes [rootToken]. Trailing separators are removed.
/// Subject sensitive path bytes and empty interior segments use reversible
/// percent encoding. Exact `*` and `>` segments remain wildcard tokens.
final class JetStreamKeyValuePathCodec implements JetStreamKeyValueKeyCodec {
  /// Creates a path codec for [separator].
  factory JetStreamKeyValuePathCodec({
    String separator = "/",
    String rootToken = "_root_",
  }) {
    if (separator.isEmpty || separator.contains(".")) {
      throw ArgumentError.value(
        separator,
        "separator",
        "Must be nonempty and must not contain a dot",
      );
    }
    if (rootToken.isEmpty ||
        rootToken.contains(".") ||
        rootToken == "%" ||
        rootToken == "*" ||
        rootToken == ">" ||
        rootToken.codeUnits.any((unit) => unit <= 32 || unit == 127)) {
      throw ArgumentError.value(rootToken, "rootToken", "Invalid root token");
    }
    return JetStreamKeyValuePathCodec._(separator, rootToken);
  }

  const JetStreamKeyValuePathCodec._(this.separator, this.rootToken);

  /// Logical path separator.
  final String separator;

  /// Stored token representing an absolute path root.
  final String rootToken;

  @override
  String encode(String key) {
    final rooted = key.startsWith(separator);
    var body = rooted ? key.substring(separator.length) : key;
    while (body.endsWith(separator) && body.isNotEmpty) {
      body = body.substring(0, body.length - separator.length);
    }
    if (body.isEmpty) {
      return rooted ? rootToken : "";
    }
    final encoded = body
        .split(separator)
        .indexed
        .map((entry) {
          final (index, segment) = entry;
          if (segment == "*" || segment == ">") {
            return segment;
          }
          if (segment.isEmpty) {
            return "%";
          }
          final escaped = Uri.encodeComponent(
            segment,
          ).replaceAll(".", "%2E").replaceAll("*", "%2A");
          if (!rooted && index == 0 && escaped == rootToken) {
            final first = escaped
                .codeUnitAt(0)
                .toRadixString(16)
                .padLeft(2, "0");
            return "%$first${escaped.substring(1)}";
          }
          return escaped;
        })
        .join(".");
    if (!rooted) return encoded;
    return "$rootToken.$encoded";
  }

  @override
  String decode(String key) {
    if (key == rootToken) return separator;
    final rootedPrefix = "$rootToken.";
    final rooted = key.startsWith(rootedPrefix);
    final body = rooted ? key.substring(rootedPrefix.length) : key;
    final decoded = body
        .split(".")
        .map((token) {
          if (token == "*" || token == ">") {
            return token;
          }
          return token == "%" ? "" : Uri.decodeComponent(token);
        })
        .join(separator);
    return rooted ? "$separator$decoded" : decoded;
  }
}

/// Applies key codecs in declaration order and decodes in reverse order.
final class JetStreamKeyValueKeyCodecChain
    implements JetStreamKeyValueKeyCodec {
  /// Creates a nonempty immutable key codec chain.
  factory JetStreamKeyValueKeyCodecChain(
    Iterable<JetStreamKeyValueKeyCodec> codecs,
  ) {
    final captured = List<JetStreamKeyValueKeyCodec>.unmodifiable(codecs);
    if (captured.isEmpty) {
      throw ArgumentError.value(codecs, "codecs", "Must not be empty");
    }
    return JetStreamKeyValueKeyCodecChain._(captured);
  }

  const JetStreamKeyValueKeyCodecChain._(this.codecs);

  /// Ordered immutable codecs.
  final List<JetStreamKeyValueKeyCodec> codecs;

  @override
  String encode(String key) => codecs.fold(key, (value, codec) {
    return codec.encode(value);
  });

  @override
  String decode(String key) => codecs.reversed.fold(key, (value, codec) {
    return codec.decode(value);
  });
}

/// Applies value codecs in declaration order and decodes in reverse order.
final class JetStreamKeyValueValueCodecChain
    implements JetStreamKeyValueValueCodec {
  /// Creates a nonempty immutable value codec chain.
  factory JetStreamKeyValueValueCodecChain(
    Iterable<JetStreamKeyValueValueCodec> codecs,
  ) {
    final captured = List<JetStreamKeyValueValueCodec>.unmodifiable(codecs);
    if (captured.isEmpty) {
      throw ArgumentError.value(codecs, "codecs", "Must not be empty");
    }
    return JetStreamKeyValueValueCodecChain._(captured);
  }

  const JetStreamKeyValueValueCodecChain._(this.codecs);

  /// Ordered immutable codecs.
  final List<JetStreamKeyValueValueCodec> codecs;

  @override
  List<int> encode(List<int> value) => codecs.fold(value, (bytes, codec) {
    return codec.encode(bytes);
  });

  @override
  List<int> decode(List<int> value) =>
      codecs.reversed.fold(value, (bytes, codec) => codec.decode(bytes));
}

String _mapTokens(String key, String Function(String token) transform) {
  return key
      .split(".")
      .map((token) => token == "*" || token == ">" ? token : transform(token))
      .join(".");
}
