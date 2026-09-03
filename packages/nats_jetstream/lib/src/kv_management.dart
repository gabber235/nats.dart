import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/kv.dart";
import "package:nats_jetstream/src/kv_bucket.dart";
import "package:nats_jetstream/src/kv_history.dart";
import "package:nats_jetstream/src/kv_status.dart";
import "package:nats_jetstream/src/stream_manager.dart";
import "package:nats_jetstream/src/stream_operations.dart";
import "package:nats_jetstream/src/wire_integer.dart";

/// Status and bounded multi key management for one verified KV bucket.
extension JetStreamKeyValueManagement on JetStreamKeyValue {
  /// Reads and verifies current backing stream status.
  Future<JetStreamKeyValueStatus> status() async {
    final manager = JetStreamStreamManager(connection, apiPrefix, timeout);
    final subject = "$apiPrefix.STREAM.INFO.$backingStreamName";
    final info = await manager.info(backingStreamName);
    final status = decodeKeyValueStatus(info, subject: subject);
    if (status.bucket != name) {
      throw JetStreamProtocolException(
        "Stream is not the requested KV bucket",
        subject: subject,
        fieldPath: "config.name",
      );
    }
    return status;
  }

  /// Stores delete markers for every current live key matching [filter].
  Future<JetStreamKeyValueMutationResult> deleteMatching(
    String filter, {
    int maximumKeys = 100000,
    int maximumBytes = 16 * 1024 * 1024,
  }) {
    return _mutateMatching(
      filter,
      "delete",
      delete,
      maximumKeys: maximumKeys,
      maximumBytes: maximumBytes,
    );
  }

  /// Stores purge markers for every current live key matching [filter].
  Future<JetStreamKeyValueMutationResult> purgeMatching(
    String filter, {
    JetStreamKeyValueLifetime markerLifetime =
        const JetStreamKeyValueLifetime.inherit(),
    int maximumKeys = 100000,
    int maximumBytes = 16 * 1024 * 1024,
  }) {
    if (markerLifetime.wireValue != null && !allowPerKeyTtl) {
      throw StateError("KV bucket does not enable per key TTL");
    }
    return _mutateMatching(
      filter,
      "purge",
      (key) => purge(key, markerLifetime: markerLifetime),
      maximumKeys: maximumKeys,
      maximumBytes: maximumBytes,
    );
  }

  /// Removes history behind current markers and old markers themselves.
  ///
  /// Markers younger than [olderThan] remain as the sole key revision. Older
  /// markers and their history are removed completely. Recreated live keys are
  /// never selected from stale markers in retained history.
  Future<JetStreamKeyValuePurgeDeletesResult> purgeDeletes({
    Duration olderThan = const Duration(minutes: 30),
    int maximumEntries = 100000,
    int maximumBytes = 64 * 1024 * 1024,
  }) async {
    if (olderThan.isNegative) {
      throw ArgumentError.value(olderThan, "olderThan", "Must not be negative");
    }
    final entries = await historyMatching(
      const [">"],
      maximumEntries: maximumEntries,
      maximumBytes: maximumBytes,
    );
    final latest = <String, JetStreamKeyValueEntry>{};
    for (final entry in entries) {
      latest[entry.key] = entry;
    }
    final cutoff = DateTime.now().toUtc().subtract(olderThan);
    final markers = latest.values
        .where((entry) => entry.operation != JetStreamKeyValueOperation.put)
        .toList(growable: false);
    final manager = JetStreamStreamManager(connection, editApiPrefix, timeout);
    final completed = <String>[];
    var purged = BigInt.zero;
    var retainedMarkers = 0;
    var removedMarkers = 0;
    for (final marker in markers) {
      try {
        final created = _created(marker);
        final retain = !created.isBefore(cutoff);
        final result = await manager.purge(
          editStreamName,
          options: _markerPurgeOptions(
            editSubjectPrefix,
            encodeKey(marker.key),
            marker,
            retain,
          ),
        );
        purged += result.purged;
        completed.add(marker.key);
        if (retain) {
          retainedMarkers += 1;
        } else {
          removedMarkers += 1;
        }
      } on Object catch (error) {
        throw JetStreamKeyValuePurgeDeletesException(
          bucket: name,
          purged: purged,
          completedKeys: completed,
          cause: error,
        );
      }
    }
    return JetStreamKeyValuePurgeDeletesResult(
      purged: purged,
      processedMarkers: markers.length,
      retainedMarkers: retainedMarkers,
      removedMarkers: removedMarkers,
    );
  }

  Future<JetStreamKeyValueMutationResult> _mutateMatching(
    String filter,
    String operation,
    Future<BigInt> Function(String key) mutate, {
    required int maximumKeys,
    required int maximumBytes,
  }) async {
    encodeFilter(filter);
    final matching = await keys(
      filters: [filter],
      maximumKeys: maximumKeys,
      maximumBytes: maximumBytes,
    );
    final completed = <String, BigInt>{};
    for (final key in matching) {
      try {
        completed[key] = await mutate(key);
      } on Object catch (error) {
        throw JetStreamKeyValueMutationException(
          bucket: name,
          operation: operation,
          filter: filter,
          completed: completed,
          cause: error,
        );
      }
    }
    return JetStreamKeyValueMutationResult(completed);
  }
}

JetStreamPurgeOptions _markerPurgeOptions(
  String prefix,
  String encodedKey,
  JetStreamKeyValueEntry marker,
  bool retain,
) {
  final filter = "$prefix$encodedKey";
  if (retain) {
    return JetStreamPurgeOptions(filter: filter, sequence: marker.revision);
  }
  if (marker.revision == maximumJetStreamUint64) {
    return JetStreamPurgeOptions(filter: filter, keep: BigInt.zero);
  }
  return JetStreamPurgeOptions(
    filter: filter,
    sequence: marker.revision + BigInt.one,
  );
}

DateTime _created(JetStreamKeyValueEntry marker) {
  try {
    return DateTime.parse(marker.created).toUtc();
  } on FormatException catch (error) {
    throw JetStreamProtocolException(
      "KV entry has an invalid creation timestamp",
      fieldPath: "message.time",
      cause: error,
    );
  }
}
