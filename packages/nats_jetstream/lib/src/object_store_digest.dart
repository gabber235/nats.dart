import "dart:convert";

import "package:crypto/crypto.dart";
import "package:nats_jetstream/src/error.dart";

const _prefix = "SHA-256=";

/// Calculates the canonical Object Store digest for [bytes].
String objectDigest(List<int> bytes) =>
    "$_prefix${base64Url.encode(sha256.convert(bytes).bytes)}";

/// Formats one exact 32 byte SHA 256 result as a canonical Object Store digest.
String objectDigestFromHash(List<int> bytes) {
  if (bytes.length != 32) {
    throw ArgumentError.value(bytes.length, "bytes", "Must contain 32 bytes");
  }
  return "$_prefix${base64Url.encode(bytes)}";
}

/// Validates the strict Object Store SHA 256 digest grammar.
void validateObjectDigest(String digest, String subject) {
  if (!digest.startsWith(_prefix)) {
    throw JetStreamProtocolException(
      "Object digest uses an unsupported algorithm",
      subject: subject,
      fieldPath: "digest",
    );
  }
  final encoded = digest.substring(_prefix.length);
  try {
    final bytes = base64Url.decode(encoded);
    if (bytes.length != 32 || base64Url.encode(bytes) != encoded) {
      throw const FormatException("Digest is not canonical SHA 256");
    }
  } on FormatException catch (error) {
    throw JetStreamProtocolException(
      "Object digest is malformed",
      subject: subject,
      fieldPath: "digest",
      cause: error,
    );
  }
}

/// Compares a validated committed digest without content dependent timing.
bool objectDigestMatches(String expected, List<int> bytes) {
  final expectedBytes = base64Url.decode(expected.substring(_prefix.length));
  final actualBytes = sha256.convert(bytes).bytes;
  return objectHashMatches(expectedBytes, actualBytes);
}

/// Compares two exact digest byte arrays without content dependent timing.
bool objectHashMatches(List<int> expected, List<int> actual) {
  if (expected.length != actual.length) {
    return false;
  }
  var difference = 0;
  for (var index = 0; index < actual.length; index += 1) {
    difference |= actual[index] ^ expected[index];
  }
  return difference == 0;
}

/// Decodes the hash bytes from a previously validated Object Store digest.
List<int> objectDigestHash(String digest) =>
    base64Url.decode(digest.substring(_prefix.length));
