import "dart:convert";

import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  test("Base64 key codec preserves hierarchy and wildcard tokens", () {
    const codec = JetStreamKeyValueBase64KeyCodec();

    expect(
      codec.encode("users.john.profile"),
      "dXNlcnM=.am9obg==.cHJvZmlsZQ==",
    );
    expect(codec.encode("users.*.>"), "dXNlcnM=.*.>");
    expect(codec.decode("7ZWc6riA.8J+Zgg=="), "한글.🙂");
    expect(codec.decode(codec.encode("한글.🙂")), "한글.🙂");
  });

  test("Base64 value codec preserves arbitrary bytes", () {
    const codec = JetStreamKeyValueBase64ValueCodec();
    final value = [0, 1, 127, 128, 255];

    final encoded = codec.encode(value);

    expect(utf8.decode(encoded), "AAF/gP8=");
    expect(codec.decode(encoded), value);
  });

  test("codec chains decode in reverse declaration order", () {
    final key = JetStreamKeyValueKeyCodecChain([
      const _PrefixKeyCodec("first:"),
      const _PrefixKeyCodec("second:"),
    ]);
    final value = JetStreamKeyValueValueCodecChain([
      const _OffsetValueCodec(1),
      const _OffsetValueCodec(2),
    ]);

    expect(key.encode("name"), "second:first:name");
    expect(key.decode("second:first:name"), "name");
    expect(value.encode([1, 2]), [4, 5]);
    expect(value.decode([4, 5]), [1, 2]);
    expect(() => JetStreamKeyValueKeyCodecChain(const []), throwsArgumentError);
    expect(
      () => JetStreamKeyValueValueCodecChain(const []),
      throwsArgumentError,
    );
  });

  test("path codec reversibly maps roots separators and subject bytes", () {
    final codec = JetStreamKeyValuePathCodec();

    expect(codec.encode("user/profile/settings/"), "user.profile.settings");
    expect(codec.decode("user.profile.settings"), "user/profile/settings");
    expect(codec.encode("dir/file.txt"), "dir.file%2Etxt");
    expect(codec.decode("dir.file%2Etxt"), "dir/file.txt");
    expect(codec.encode("user//settings"), "user.%.settings");
    expect(codec.decode("user.%.settings"), "user//settings");
    expect(codec.decode(codec.encode("symbols/%/*/>")), "symbols/%/*/>");
    expect(codec.encode("/user/*/>"), "_root_.user.*.>");
    expect(codec.decode("_root_.user.*.>"), "/user/*/>");
    expect(codec.encode("/"), "_root_");
    expect(codec.decode("_root_"), "/");
    expect(codec.decode(codec.encode("_root_/user")), "_root_/user");
    expect(
      () => JetStreamKeyValuePathCodec(rootToken: "%"),
      throwsArgumentError,
    );
  });

  test("codec policy snapshots input and output bytes", () {
    final output = <int>[4, 5, 6];
    final codecs = JetStreamKeyValueCodecs(value: _RetainingValueCodec(output));
    final caller = <int>[1, 2, 3];

    final encoded = codecs.encodeValue(caller);
    caller[0] = 9;
    output[0] = 9;

    expect(encoded, [4, 5, 6]);
    expect(() => encoded.add(7), throwsUnsupportedError);
    expect(() => codecs.encodeValue(const [-1]), throwsFormatException);
    expect(
      () => JetStreamKeyValueCodecs(
        value: _RetainingValueCodec(const [256]),
      ).encodeValue(const [0]),
      throwsFormatException,
    );
  });
}

final class _PrefixKeyCodec implements JetStreamKeyValueKeyCodec {
  const _PrefixKeyCodec(this.prefix);

  final String prefix;

  @override
  String encode(String key) => "$prefix$key";

  @override
  String decode(String key) {
    if (!key.startsWith(prefix)) throw const FormatException("Missing prefix");
    return key.substring(prefix.length);
  }
}

final class _OffsetValueCodec implements JetStreamKeyValueValueCodec {
  const _OffsetValueCodec(this.offset);

  final int offset;

  @override
  List<int> encode(List<int> value) {
    return value.map((byte) => byte + offset).toList(growable: false);
  }

  @override
  List<int> decode(List<int> value) {
    return value.map((byte) => byte - offset).toList(growable: false);
  }
}

final class _RetainingValueCodec implements JetStreamKeyValueValueCodec {
  _RetainingValueCodec(this.output);

  final List<int> output;

  @override
  List<int> encode(List<int> value) => output;

  @override
  List<int> decode(List<int> value) => output;
}
