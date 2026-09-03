import "dart:convert";

import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/object_store_watch_codec.dart";
import "package:test/test.dart";

void main() {
  test("watch options validate exact names, starts, and resource bounds", () {
    expect(
      () => JetStreamObjectWatchOptions(names: const {}),
      throwsArgumentError,
    );
    expect(
      () => JetStreamObjectWatchOptions(maximumPendingMessages: 0),
      throwsArgumentError,
    );
    expect(
      () => JetStreamObjectWatchOptions(maximumPendingBytes: 0),
      throwsArgumentError,
    );
    expect(
      () => JetStreamObjectWatchOptions(
        heartbeat: const Duration(milliseconds: 99),
      ),
      throwsArgumentError,
    );
    expect(
      () => JetStreamObjectWatchStart.revision(BigInt.zero),
      throwsArgumentError,
    );

    final duplicateNames = <String>{}
      ..add("alpha")
      ..add("alpha");
    final options = JetStreamObjectWatchOptions(names: duplicateNames);
    expect(options.names, {"alpha"});
    expect(() => options.names!.add("beta"), throwsUnsupportedError);
  });

  test("watch decoder requires canonical padded subject identity", () {
    final name = "한글 object";
    final encodedName = base64Url.encode(utf8.encode(name));
    final payload = _metadata(name);
    final decoded = decodeObjectWatchMessage(
      subject: "\$O.BUCKET.M.$encodedName",
      payload: payload,
      bucket: "BUCKET",
      streamSequence: BigInt.from(7),
      timestampNanoseconds: BigInt.from(1700000000123456789),
    );

    expect(decoded.info.name, name);
    expect(decoded.info.revision, BigInt.from(7));
    expect(decoded.info.modified, "2023-11-14T22:13:20.123456789Z");
    expect(decoded.encodedBytes, payload.length);

    expect(
      () => decodeObjectWatchMessage(
        subject: "\$O.BUCKET.M.${encodedName.replaceAll("=", "")}",
        payload: payload,
        bucket: "BUCKET",
        streamSequence: BigInt.one,
        timestampNanoseconds: BigInt.zero,
      ),
      throwsA(isA<JetStreamProtocolException>()),
    );
    expect(
      () => decodeObjectWatchMessage(
        subject: "\$O.BUCKET.M.${base64Url.encode(utf8.encode("other"))}",
        payload: payload,
        bucket: "BUCKET",
        streamSequence: BigInt.one,
        timestampNanoseconds: BigInt.zero,
      ),
      throwsA(
        isA<JetStreamProtocolException>().having(
          (error) => error.fieldPath,
          "fieldPath",
          "name",
        ),
      ),
    );
    expect(
      () => decodeObjectWatchMessage(
        subject: "\$O.BUCKET.M.$encodedName",
        payload: _metadata(name, bucket: "OTHER"),
        bucket: "BUCKET",
        streamSequence: BigInt.one,
        timestampNanoseconds: BigInt.zero,
      ),
      throwsA(
        isA<JetStreamProtocolException>().having(
          (error) => error.fieldPath,
          "fieldPath",
          "bucket",
        ),
      ),
    );
  });
}

List<int> _metadata(String name, {String bucket = "BUCKET"}) =>
    encodeExactJson({
      "name": name,
      "bucket": bucket,
      "nuid": "VALIDIDENTIFIER0000001",
      "size": BigInt.zero,
      "chunks": BigInt.zero,
      "digest": "SHA-256=47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU=",
    });
