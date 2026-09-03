import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/object_store.dart";
import "package:nats_jetstream/src/object_store_bucket.dart";
import "package:nats_jetstream/src/object_store_protocol.dart";
import "package:nats_jetstream/src/object_store_stream.dart";
import "package:nats_jetstream/src/stream_wire.dart";

/// Administrative status operations for one Object Store bucket.
extension JetStreamObjectStoreStatusOperations on JetStreamObjectStore {
  /// Reads exact current configuration and storage state.
  Future<JetStreamObjectStoreStatus> status() async {
    final response = await _statusProtocol.streamInfo();
    verifyObjectStoreIdentity(response, name);
    return decodeObjectStoreStatus(
      decodeStreamInfo(response),
      subject: response.subject,
      expectedBucket: name,
    );
  }

  /// Permanently rejects mutations and preserves existing bytes from expiry.
  ///
  /// NATS normalizes maximum age to zero when sealing so stored bytes cannot
  /// expire after the operation. Other configured limits remain visible.
  Future<JetStreamObjectStoreStatus> seal() async {
    final current = await _statusProtocol.streamInfo();
    verifyObjectStoreIdentity(current, name);
    final currentConfig = current.requiredObject("config");
    if (current.optionalBoolean("config.sealed", source: currentConfig)) {
      return decodeObjectStoreStatus(
        decodeStreamInfo(current),
        subject: current.subject,
        expectedBucket: name,
      );
    }
    final config = Map<String, Object?>.from(currentConfig)..["sealed"] = true;
    final updated = await _statusProtocol.updateStream(config);
    verifyObjectStoreIdentity(updated, name);
    final result = decodeObjectStoreStatus(
      decodeStreamInfo(updated),
      subject: updated.subject,
      expectedBucket: name,
    );
    if (!result.sealed) {
      throw JetStreamProtocolException(
        "Object Store seal response is not sealed",
        subject: updated.subject,
        fieldPath: "config.sealed",
      );
    }
    return result;
  }

  JetStreamObjectProtocol get _statusProtocol =>
      JetStreamObjectProtocol(connection, apiPrefix, timeout, name);
}
