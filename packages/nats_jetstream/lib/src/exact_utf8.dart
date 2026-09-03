import "dart:convert";
import "dart:typed_data";

/// Encodes [value] only when Dart can preserve its exact Unicode text.
Uint8List encodeExactUtf8(String value, {String name = "value"}) {
  final bytes = Uint8List.fromList(utf8.encode(value));
  if (utf8.decode(bytes, allowMalformed: false) != value) {
    throw ArgumentError("Must contain exact UTF 8 text", name);
  }
  return bytes;
}
