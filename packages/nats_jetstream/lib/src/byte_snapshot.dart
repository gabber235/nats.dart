import "dart:typed_data";

/// Returns an immutable byte snapshot or rejects the first nonbyte value.
Uint8List checkedJetStreamByteSnapshot(
  List<int> source, {
  String name = "payload",
}) {
  for (var index = 0; index < source.length; index += 1) {
    final value = source[index];
    if (value < 0 || value > 255) {
      throw FormatException(
        "$name contains a nonbyte value at index $index: $value",
      );
    }
  }
  return Uint8List.fromList(source).asUnmodifiableView();
}
