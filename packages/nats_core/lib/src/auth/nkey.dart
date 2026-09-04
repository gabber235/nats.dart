import "dart:convert";
import "dart:typed_data";

import "package:cryptography/cryptography.dart";
import "package:nats_core/src/error.dart";

const _seedPrefix = 18 << 3;
const _userPrefix = 20 << 3;
const _base32Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/// Internal NATS NKey challenge signer.
final class NKeySigner {
  NKeySigner._(this._seed, this._publicPrefix);

  /// Decodes and validates a decorated NKey [seed].
  factory NKeySigner.fromSeed(String seed) {
    final decoded = _decodeChecked(seed);
    if (decoded.length != 34) {
      throw const NatsAuthenticationException("NKey seed has invalid length");
    }
    final seedType = decoded[0] & 248;
    final publicPrefix = ((decoded[0] & 7) << 5) | ((decoded[1] & 248) >> 3);
    if (seedType != _seedPrefix || publicPrefix != _userPrefix) {
      throw const NatsAuthenticationException(
        "NKey seed must contain a user key",
      );
    }
    return NKeySigner._(Uint8List.fromList(decoded.sublist(2)), publicPrefix);
  }

  final Uint8List _seed;
  final int _publicPrefix;
  final Ed25519 _algorithm = Ed25519();

  Future<SimpleKeyPair> _keyPair() => _algorithm.newKeyPairFromSeed(_seed);

  /// Returns the decorated public NKey.
  Future<String> publicKey() async {
    final publicKey = await (await _keyPair()).extractPublicKey();
    return _encodeChecked([_publicPrefix, ...publicKey.bytes]);
  }

  /// Signs a server [nonce] and returns URL safe base64 without padding.
  Future<String> sign(String nonce) async {
    final signature = await _algorithm.sign(
      utf8.encode(nonce),
      keyPair: await _keyPair(),
    );
    return base64Url.encode(signature.bytes).replaceAll("=", "");
  }
}

Uint8List _decodeChecked(String value) {
  final raw = _decodeBase32(value);
  if (raw.length < 4) {
    throw const NatsAuthenticationException("NKey encoding is too short");
  }
  final body = Uint8List.fromList(raw.sublist(0, raw.length - 2));
  final expected = raw[raw.length - 2] | (raw.last << 8);
  if (_crc16(body) != expected) {
    throw const NatsAuthenticationException("NKey checksum is invalid");
  }
  return body;
}

String _encodeChecked(List<int> body) {
  final crc = _crc16(body);
  return _encodeBase32([...body, crc & 255, crc >> 8]);
}

Uint8List _decodeBase32(String value) {
  var buffer = 0;
  var bits = 0;
  final output = <int>[];
  for (final code in value.trim().toUpperCase().codeUnits) {
    final index = _base32Alphabet.codeUnits.indexOf(code);
    if (index == -1) {
      throw const NatsAuthenticationException(
        "NKey contains a non Base32 character",
      );
    }
    buffer = (buffer << 5) | index;
    bits += 5;
    if (bits >= 8) {
      bits -= 8;
      output.add((buffer >> bits) & 255);
      buffer &= (1 << bits) - 1;
    }
  }
  if (bits > 0 && buffer != 0) {
    throw const NatsAuthenticationException("NKey has nonzero padding bits");
  }
  return Uint8List.fromList(output);
}

String _encodeBase32(List<int> value) {
  var buffer = 0;
  var bits = 0;
  final output = StringBuffer();
  for (final byte in value) {
    buffer = (buffer << 8) | byte;
    bits += 8;
    while (bits >= 5) {
      bits -= 5;
      output.writeCharCode(_base32Alphabet.codeUnitAt((buffer >> bits) & 31));
      buffer &= (1 << bits) - 1;
    }
  }
  if (bits > 0) {
    output.writeCharCode(
      _base32Alphabet.codeUnitAt((buffer << (5 - bits)) & 31),
    );
  }
  return output.toString();
}

int _crc16(List<int> value) {
  var crc = 0;
  for (final byte in value) {
    crc ^= byte << 8;
    for (var bit = 0; bit < 8; bit += 1) {
      crc = (crc & 0x8000) == 0
          ? (crc << 1) & 0xffff
          : ((crc << 1) ^ 0x1021) & 0xffff;
    }
  }
  return crc;
}
