@TestOn("vm")
library;

import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  final serverUrl = Platform.environment["NATS_DOMAIN_URL"];
  final apiPrefix = Platform.environment["NATS_DOMAIN_API_PREFIX"];
  final skipReason = serverUrl == null || apiPrefix == null
      ? "NATS domain configuration is not available"
      : false;

  test(
    "domain routing covers KV and Object Store mutation round trips",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final kvName = "DK$suffix";
      final objectName = "DO$suffix";
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(serverUrl!)]),
      );
      final jetStream = JetStreamContext(connection, apiPrefix: apiPrefix!);
      var kvExists = false;
      var objectStoreExists = false;
      addTearDown(() async {
        if (kvExists) {
          try {
            await jetStream.keyValue.deleteBucket(kvName);
          } on JetStreamApiException {
            // Cleanup follows test owned state.
          }
        }
        if (objectStoreExists) {
          try {
            await jetStream.objectStore.deleteStore(objectName);
          } on JetStreamApiException {
            // Cleanup follows test owned state.
          }
        }
        await connection.close();
      });

      final kv = await jetStream.keyValue.createBucket(
        JetStreamKeyValueBucketConfig(name: kvName),
      );
      kvExists = true;
      final kvRevision = await kv.putString("key", "domain value");
      expect(await kv.getString("key"), "domain value");
      await kv.delete("key", expectedRevision: kvRevision);
      expect(
        (await kv.get("key"))?.operation,
        JetStreamKeyValueOperation.delete,
      );

      final objects = await jetStream.objectStore.createStore(
        JetStreamObjectStoreConfig(
          name: objectName,
          storage: JetStreamStorage.memory,
          experimentalFastIngest: true,
        ),
      );
      objectStoreExists = true;
      try {
        await objects.putString("object", "domain object");
      } on JetStreamObjectPutException catch (error) {
        fail("Sequential domain upload failed: ${error.cause}");
      }
      expect(await objects.getString("object"), "domain object");
      final updated = await objects.updateMetadata(
        "object",
        JetStreamObjectMetadataUpdate(
          name: "object",
          description: "updated through domain",
          metadata: const {"route": "domain"},
        ),
      );
      expect(updated.description, "updated through domain");
      expect(updated.metadata, const {"route": "domain"});
      try {
        await objects.putString(
          "fast",
          "domain fast object",
          mode: JetStreamObjectPutMode.experimentalFastIngest,
        );
      } on JetStreamObjectPutException catch (error) {
        final cause = error.cause;
        if (cause is JetStreamFastBatchException) {
          fail(
            "Fast domain upload failed: ${cause.message}; "
            "cause=${cause.cause}; sent=${cause.sentSequence}; "
            "acknowledged=${cause.acknowledgedSequence}",
          );
        }
        fail("Fast domain upload failed: $cause");
      }
      expect(await objects.getString("fast"), "domain fast object");
      await objects.delete("object");
      expect(await objects.getInfo("object"), isNull);

      await jetStream.keyValue.deleteBucket(kvName);
      kvExists = false;
      await jetStream.objectStore.deleteStore(objectName);
      objectStoreExists = false;
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}
