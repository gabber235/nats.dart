import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/object_store_wire.dart";
import "package:test/test.dart";

void main() {
  test(
    "object metadata accepts only subject safe 22 character identifiers",
    () {
      Map<String, Object?> metadata(String identifier) => {
        "name": "object",
        "bucket": "BUCKET",
        "nuid": identifier,
        "size": BigInt.zero,
        "chunks": BigInt.zero,
        "digest": "SHA-256=47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU=",
      };

      final valid = decodeObjectInfo(
        metadata("VALIDIDENTIFIER0000001"),
        subject: r"$JS.API.STREAM.MSG.GET.OBJ_BUCKET",
        expectedBucket: "BUCKET",
        expectedName: "object",
        modified: "2026-09-01T00:00:00Z",
        revision: BigInt.one,
      );
      expect(valid.identifier, "VALIDIDENTIFIER0000001");

      expect(
        () => decodeObjectInfo(
          metadata("VALIDIDENTIFIER0000001"),
          subject: r"$JS.API.STREAM.MSG.GET.OBJ_BUCKET",
          expectedBucket: "BUCKET",
          expectedName: "object",
          modified: "2026-09-42T00:00:00Z",
          revision: BigInt.one,
        ),
        throwsA(
          isA<JetStreamProtocolException>().having(
            (error) => error.fieldPath,
            "fieldPath",
            "message.time",
          ),
        ),
      );

      for (final invalid in const ["*", ">", "contains.dot", "too-short"]) {
        expect(
          () => decodeObjectInfo(
            metadata(invalid),
            subject: r"$JS.API.STREAM.MSG.GET.OBJ_BUCKET",
            expectedBucket: "BUCKET",
            expectedName: "object",
            modified: "2026-09-01T00:00:00Z",
            revision: BigInt.one,
          ),
          throwsA(
            isA<JetStreamProtocolException>().having(
              (error) => error.fieldPath,
              "fieldPath",
              "nuid",
            ),
          ),
        );
      }

      expect(
        () => decodeObjectInfo(
          metadata("VALIDIDENTIFIER0000001"),
          subject: r"$JS.API.STREAM.MSG.GET.OBJ_BUCKET",
          expectedBucket: "OTHER",
          expectedName: "object",
          modified: "2026-09-01T00:00:00Z",
          revision: BigInt.one,
        ),
        throwsA(
          isA<JetStreamProtocolException>().having(
            (error) => error.fieldPath,
            "fieldPath",
            "bucket",
          ),
        ),
      );
    },
  );
}
