import "dart:async";
import "dart:convert";
import "dart:typed_data";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/byte_snapshot.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/exact_utf8.dart";
import "package:nats_jetstream/src/object_store.dart";
import "package:nats_jetstream/src/object_store_download.dart";
import "package:nats_jetstream/src/object_store_encoding.dart";
import "package:nats_jetstream/src/object_store_error.dart";
import "package:nats_jetstream/src/object_store_protocol.dart";
import "package:nats_jetstream/src/object_store_upload.dart";
import "package:nats_jetstream/src/object_store_watch.dart";
import "package:nats_jetstream/src/object_store_watch_session.dart";
import "package:nats_jetstream/src/object_store_wire.dart";

/// One existing Object Store bucket.
final class JetStreamObjectStore {
  /// Creates a bucket handle after stream identity was verified.
  const JetStreamObjectStore(
    this.connection,
    this.apiPrefix,
    this.timeout,
    this.name,
  );

  /// Core connection used for operations.
  final NatsConnection connection;

  /// Domain aware JetStream API prefix.
  final String apiPrefix;

  /// Total deadline for one request.
  final Duration timeout;

  /// Public bucket name.
  final String name;

  /// Stores bytes under [name] with default metadata.
  Future<JetStreamObjectInfo> putBytes(
    String name,
    List<int> bytes, {
    JetStreamObjectPutMode mode = JetStreamObjectPutMode.sequential,
  }) => put(JetStreamObjectMetadata(name: name), bytes, mode: mode);

  /// Stores [value] using its exact UTF 8 encoding.
  Future<JetStreamObjectInfo> putString(
    String name,
    String value, {
    JetStreamObjectPutMode mode = JetStreamObjectPutMode.sequential,
  }) => putBytes(name, encodeExactUtf8(value), mode: mode);

  /// Stores immutable [bytes], then atomically exposes their metadata.
  Future<JetStreamObjectInfo> put(
    JetStreamObjectMetadata metadata,
    List<int> bytes, {
    JetStreamObjectPutMode mode = JetStreamObjectPutMode.sequential,
  }) {
    final snapshot = checkedJetStreamByteSnapshot(bytes, name: "object");
    return putStream(metadata, Stream.value(snapshot), mode: mode);
  }

  /// Streams bytes through bounded chunks, then atomically exposes metadata.
  ///
  /// Producers must treat emitted lists as immutable. Each event is copied
  /// immediately when delivered, before this client awaits network work.
  Future<JetStreamObjectInfo> putStream(
    JetStreamObjectMetadata metadata,
    Stream<List<int>> bytes, {
    JetStreamObjectPutMode mode = JetStreamObjectPutMode.sequential,
  }) => uploadJetStreamObject(
    connection: connection,
    apiPrefix: apiPrefix,
    timeout: timeout,
    bucket: name,
    metadata: metadata,
    bytes: bytes,
    getInfo: getInfo,
    mode: mode,
  );

  /// Reads current metadata, or `null` when absent or deleted.
  Future<JetStreamObjectInfo?> getInfo(
    String objectName, {
    bool showDeleted = false,
  }) async {
    validateJetStreamObjectName(objectName);
    final metaSubject = _protocol.metadataSubject(objectName);
    final stored = await _protocol.getStored({
      "last_by_subj": metaSubject,
    }, missingIsNull: true);
    if (stored == null) {
      return null;
    }
    if (stored.subject != metaSubject) {
      throw JetStreamProtocolException(
        "Object metadata response has the wrong subject",
        subject: stored.requestSubject,
        fieldPath: "message.subject",
      );
    }
    final info = decodeObjectInfoPayload(
      stored.payload,
      subject: stored.requestSubject,
      expectedBucket: name,
      expectedName: objectName,
      modified: stored.time,
      revision: stored.sequence,
    );
    return info.deleted && !showDeleted ? null : info;
  }

  /// Watches verified metadata through a bounded owned consumer.
  JetStreamObjectWatch watch({JetStreamObjectWatchOptions? options}) =>
      JetStreamObjectWatchSession(
        connection,
        apiPrefix,
        timeout,
        name,
        options ?? JetStreamObjectWatchOptions(),
      );

  /// Returns a deterministic bounded snapshot of current object metadata.
  Future<List<JetStreamObjectInfo>> list({
    bool includeDeleted = false,
    int maximumObjects = 100000,
    int maximumMetadataBytes = 64 * 1024 * 1024,
  }) async {
    if (maximumObjects < 1) {
      throw ArgumentError.value(
        maximumObjects,
        "maximumObjects",
        "Must be positive",
      );
    }
    if (maximumMetadataBytes < 1) {
      throw ArgumentError.value(
        maximumMetadataBytes,
        "maximumMetadataBytes",
        "Must be positive",
      );
    }
    final snapshot = <String, JetStreamObjectChanged>{};
    var retainedBytes = 0;
    final watch = this.watch(
      options: JetStreamObjectWatchOptions(ignoreDeletes: !includeDeleted),
    );
    final events = StreamIterator(watch.events);
    try {
      while (await events.moveNext()) {
        switch (events.current) {
          case JetStreamObjectChanged(:final info, :final encodedBytes):
            final previous = snapshot[info.name];
            if (previous != null && previous.info.revision >= info.revision) {
              continue;
            }
            retainedBytes -= previous?.encodedBytes ?? 0;
            snapshot[info.name] = events.current as JetStreamObjectChanged;
            retainedBytes += encodedBytes;
            if (snapshot.length > maximumObjects ||
                retainedBytes > maximumMetadataBytes) {
              throw JetStreamObjectListLimitException(
                bucket: name,
                maximumObjects: maximumObjects,
                maximumMetadataBytes: maximumMetadataBytes,
                observedObjects: snapshot.length,
                observedMetadataBytes: retainedBytes,
              );
            }
          case JetStreamObjectInitialStateComplete():
            final result = snapshot.values.map((event) => event.info).toList()
              ..sort((left, right) => left.name.compareTo(right.name));
            return List.unmodifiable(result);
        }
      }
      throw const JetStreamProtocolException(
        "Object Store watch closed before initial state completed",
      );
    } finally {
      await events.cancel();
      await watch.close();
    }
  }

  /// Opens a lazy one chunk at a time verified download.
  Future<JetStreamObjectDownload?> get(
    String objectName, {
    int maximumChunks = 1000000,
    int maximumLinkDepth = 32,
  }) async {
    if (maximumChunks < 1) {
      throw ArgumentError.value(
        maximumChunks,
        "maximumChunks",
        "Must be positive",
      );
    }
    _validateMaximumLinkDepth(maximumLinkDepth);
    final resolved = await _resolveObject(
      objectName,
      maximumLinkDepth: maximumLinkDepth,
    );
    if (resolved == null) {
      return null;
    }
    final (store, info) = resolved;
    if (info.chunks > BigInt.from(maximumChunks)) {
      throw JetStreamObjectIntegrityException(
        "Object exceeds the configured chunk bound",
        bucket: name,
        name: objectName,
      );
    }
    return openJetStreamObjectDownload(info, store._protocol);
  }

  /// Reads and verifies an object into bounded memory.
  ///
  /// [maximumBytes] bounds retained payload bytes. [maximumChunks] separately
  /// bounds the number of server reads accepted from object metadata.
  Future<Uint8List?> getBytes(
    String objectName, {
    int maximumBytes = 64 * 1024 * 1024,
    int maximumChunks = 1000000,
    int maximumLinkDepth = 32,
  }) async {
    if (maximumBytes < 0) {
      throw ArgumentError.value(
        maximumBytes,
        "maximumBytes",
        "Must not be negative",
      );
    }
    if (maximumChunks < 1) {
      throw ArgumentError.value(
        maximumChunks,
        "maximumChunks",
        "Must be positive",
      );
    }
    _validateMaximumLinkDepth(maximumLinkDepth);
    final resolved = await _resolveObject(
      objectName,
      maximumLinkDepth: maximumLinkDepth,
    );
    if (resolved == null) {
      return null;
    }
    final (store, info) = resolved;
    if (info.size > BigInt.from(maximumBytes) ||
        info.chunks > BigInt.from(maximumChunks)) {
      throw JetStreamObjectIntegrityException(
        "Object exceeds the configured in memory bound",
        bucket: name,
        name: objectName,
      );
    }
    final download = openJetStreamObjectDownload(info, store._protocol);
    final output = BytesBuilder(copy: false);
    await for (final chunk in download.chunks) {
      output.add(chunk);
      if (output.length > maximumBytes ||
          BigInt.from(output.length) > info.size) {
        throw JetStreamObjectIntegrityException(
          "Object chunks exceed committed size",
          bucket: name,
          name: objectName,
        );
      }
    }
    await download.verified;
    final value = output.takeBytes();
    return Uint8List.fromList(value).asUnmodifiableView();
  }

  /// Reads one object as strict UTF 8, or `null` when it is absent.
  ///
  /// Malformed UTF 8 throws [FormatException]. Byte, chunk, and link bounds
  /// match [getBytes].
  Future<String?> getString(
    String objectName, {
    int maximumBytes = 64 * 1024 * 1024,
    int maximumChunks = 1000000,
    int maximumLinkDepth = 32,
  }) async {
    final bytes = await getBytes(
      objectName,
      maximumBytes: maximumBytes,
      maximumChunks: maximumChunks,
      maximumLinkDepth: maximumLinkDepth,
    );
    return bytes == null ? null : utf8.decode(bytes, allowMalformed: false);
  }

  /// Commits a deletion marker, then purges the object's chunks.
  ///
  /// [expectedGeneration] can fence the complete mutation to a generation
  /// captured by a larger operation.
  Future<void> delete(String objectName, {int? expectedGeneration}) async {
    final generation = expectedGeneration ?? connection.generation;
    final info = await getInfo(objectName, showDeleted: true);
    if (info == null) {
      throw JetStreamObjectNotFoundException(name, objectName);
    }
    final metadataSubject = _protocol.metadataSubject(objectName);
    final deletionBody = encodeObjectDeletion(info);
    late final BigInt committedRevision;
    try {
      committedRevision = await _protocol.publish(
        metadataSubject,
        encodeExactJson(deletionBody),
        headers: NatsHeaders(
          entries: [
            const MapEntry("Nats-Rollup", "sub"),
            MapEntry("Nats-Expected-Last-Subject-Sequence", "${info.revision}"),
            MapEntry(
              "Nats-Expected-Last-Subject-Sequence-Subject",
              metadataSubject,
            ),
          ],
        ),
        generation: generation,
      );
    } on JetStreamApiException catch (error) {
      if (error.errorCode == 10163) {
        throw JetStreamObjectMetadataUnavailableException(
          name,
          objectName,
          error,
        );
      }
      if (error.errorCode == 10071 || error.errorCode == 10164) {
        throw JetStreamObjectMetadataConflictException(
          name,
          objectName,
          info.revision,
          error,
        );
      }
      rethrow;
    } on NatsPublicationPreflightException {
      rethrow;
    } on Object catch (error) {
      throw JetStreamObjectMutationAmbiguousException(name, objectName, error);
    }

    JetStreamObjectInfo? committed;
    Object? verificationFailure;
    try {
      final stored = await _protocol.getStored({"seq": committedRevision});
      if (stored == null ||
          stored.subject != metadataSubject ||
          stored.sequence != committedRevision) {
        throw JetStreamObjectIntegrityException(
          "Committed deletion revision could not be read back",
          bucket: name,
          name: objectName,
        );
      }
      final decoded = decodeObjectInfoPayload(
        stored.payload,
        subject: stored.requestSubject,
        expectedBucket: name,
        expectedName: objectName,
        modified: stored.time,
        revision: stored.sequence,
      );
      if (decoded.identifier != info.identifier ||
          !objectInfoMatchesEncoding(decoded, deletionBody)) {
        throw JetStreamObjectIntegrityException(
          "Committed deletion does not match the publication",
          bucket: name,
          name: objectName,
        );
      }
      committed = decoded;
    } on Object catch (error) {
      verificationFailure = error;
    }
    try {
      await _protocol.purgeSubject(_protocol.chunkSubject(info.identifier));
    } on Object catch (error) {
      final snapshot = committed;
      if (snapshot != null) {
        throw JetStreamObjectCleanupException(snapshot, error);
      }
      throw JetStreamObjectCommitVerificationException(
        name,
        objectName,
        committedRevision,
        verificationFailure!,
        cleanupCause: error,
      );
    }
    if (committed == null) {
      throw JetStreamObjectCommitVerificationException(
        name,
        objectName,
        committedRevision,
        verificationFailure!,
      );
    }
  }

  JetStreamObjectProtocol get _protocol =>
      JetStreamObjectProtocol(connection, apiPrefix, timeout, name);

  Future<(JetStreamObjectStore, JetStreamObjectInfo)?> _resolveObject(
    String objectName, {
    required int maximumLinkDepth,
  }) async {
    var store = this;
    var currentName = objectName;
    var followedLinks = 0;
    var resolvingLink = false;
    final visited = <(String, String)>{};
    while (true) {
      if (!visited.add((store.name, currentName))) {
        throw JetStreamObjectLinkException(
          store.name,
          currentName,
          JetStreamObjectLinkFailure.cycle,
        );
      }
      final info = await store.getInfo(currentName);
      if (info == null) {
        if (!resolvingLink) {
          return null;
        }
        throw JetStreamObjectNotFoundException(store.name, currentName);
      }
      final link = info.link;
      if (link == null) {
        return (store, info);
      }
      if (link.name == null) {
        throw JetStreamObjectLinkException(
          store.name,
          currentName,
          JetStreamObjectLinkFailure.bucketLink,
        );
      }
      if (followedLinks == maximumLinkDepth) {
        throw JetStreamObjectLinkException(
          store.name,
          currentName,
          JetStreamObjectLinkFailure.maximumDepthExceeded,
        );
      }
      followedLinks += 1;
      resolvingLink = true;
      if (link.bucket != store.name) {
        final target = JetStreamObjectStore(
          connection,
          apiPrefix,
          timeout,
          link.bucket,
        );
        await target._protocol.verifyBucket();
        store = target;
      }
      currentName = link.name!;
    }
  }
}

void _validateMaximumLinkDepth(int value) {
  if (value < 0 || value > 1024) {
    throw ArgumentError.value(
      value,
      "maximumLinkDepth",
      "Must be 0 through 1024",
    );
  }
}
