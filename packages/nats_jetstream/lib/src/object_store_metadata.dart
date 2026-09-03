import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/object_store.dart";
import "package:nats_jetstream/src/object_store_bucket.dart";
import "package:nats_jetstream/src/object_store_encoding.dart";
import "package:nats_jetstream/src/object_store_error.dart";
import "package:nats_jetstream/src/object_store_protocol.dart";
import "package:nats_jetstream/src/object_store_wire.dart";

/// Metadata mutation and link operations for one Object Store bucket.
extension JetStreamObjectStoreMetadataOperations on JetStreamObjectStore {
  /// Replaces user metadata while preserving chunk identity and integrity.
  ///
  /// [expectedGeneration] can fence the complete mutation to a generation
  /// captured by a larger operation.
  Future<JetStreamObjectInfo> updateMetadata(
    String currentName,
    JetStreamObjectMetadataUpdate update, {
    int? expectedGeneration,
  }) async {
    final generation = expectedGeneration ?? connection.generation;
    final current = await getInfo(currentName);
    if (current == null) {
      throw JetStreamObjectNotFoundException(name, currentName);
    }
    final destination = update.name == currentName
        ? current
        : await getInfo(update.name, showDeleted: true);
    if (update.name != currentName &&
        destination != null &&
        !destination.deleted) {
      throw JetStreamObjectAlreadyExistsException(name, update.name);
    }
    final commit = await _publishMetadata(
      update.name,
      encodeObjectMetadataUpdate(current, update),
      destination?.revision ?? BigInt.zero,
      expectedIdentifier: current.identifier,
      concurrentConflict: update.name == currentName,
      generation: generation,
    );
    if (update.name != currentName) {
      final committed = commit.requireInfo(name, update.name);
      final tombstoneIdentifier = randomObjectIdentifier();
      try {
        final tombstone = await _publishMetadata(
          currentName,
          encodeObjectRenameDeletion(current, tombstoneIdentifier),
          current.revision,
          expectedIdentifier: tombstoneIdentifier,
          concurrentConflict: true,
          generation: generation,
        );
        tombstone.requireInfo(name, currentName);
      } on JetStreamObjectMetadataConflictException {
        final remaining = await getInfo(currentName, showDeleted: true);
        if (remaining != null) {
          throw JetStreamObjectRenameConflictException(committed, remaining);
        }
        rethrow;
      } on Object catch (error) {
        throw JetStreamObjectCleanupException(committed, error);
      }
      return committed;
    }
    return commit.requireInfo(name, update.name);
  }

  /// Creates or replaces a link after verifying one current live target.
  Future<JetStreamObjectInfo> addObjectLink(
    String linkName, {
    required JetStreamObjectStore targetStore,
    required String targetName,
  }) {
    validateJetStreamObjectName(linkName);
    _requireLinkContext(linkName, targetStore);
    return _verifiedObjectLink(linkName, targetStore, targetName);
  }

  Future<JetStreamObjectInfo> _verifiedObjectLink(
    String linkName,
    JetStreamObjectStore targetStore,
    String targetName,
  ) async {
    final target = await targetStore.getInfo(targetName, showDeleted: true);
    if (target == null || target.deleted) {
      throw JetStreamObjectNotFoundException(targetStore.name, targetName);
    }
    if (target.link != null) {
      throw JetStreamObjectLinkException(
        targetStore.name,
        targetName,
        JetStreamObjectLinkFailure.targetIsLink,
      );
    }
    return _putLink(
      linkName,
      JetStreamObjectLink(bucket: target.bucket, name: target.name),
    );
  }

  /// Creates or replaces a link to an entire verified Object Store bucket.
  Future<JetStreamObjectInfo> addBucketLink(
    String linkName,
    JetStreamObjectStore targetStore,
  ) {
    validateJetStreamObjectName(linkName);
    _requireLinkContext(linkName, targetStore);
    return _verifiedBucketLink(linkName, targetStore);
  }

  void _requireLinkContext(String linkName, JetStreamObjectStore targetStore) {
    if (!identical(connection, targetStore.connection) ||
        apiPrefix != targetStore.apiPrefix) {
      throw JetStreamObjectLinkException(
        name,
        linkName,
        JetStreamObjectLinkFailure.contextMismatch,
      );
    }
  }

  Future<JetStreamObjectInfo> _verifiedBucketLink(
    String linkName,
    JetStreamObjectStore targetStore,
  ) async {
    await JetStreamObjectProtocol(
      targetStore.connection,
      targetStore.apiPrefix,
      targetStore.timeout,
      targetStore.name,
    ).verifyBucket();
    return _putLink(linkName, JetStreamObjectLink(bucket: targetStore.name));
  }

  Future<JetStreamObjectInfo> _putLink(
    String linkName,
    JetStreamObjectLink link,
  ) async {
    final generation = connection.generation;
    final previous = await getInfo(linkName, showDeleted: true);
    if (previous != null && previous.link == null) {
      throw JetStreamObjectAlreadyExistsException(name, linkName);
    }
    final identifier = randomObjectIdentifier();
    final commit = await _publishMetadata(
      linkName,
      encodeObjectLink(
        name: linkName,
        bucket: name,
        identifier: identifier,
        link: link,
      ),
      previous?.revision ?? BigInt.zero,
      expectedIdentifier: identifier,
      concurrentConflict: previous != null,
      generation: generation,
    );
    return commit.requireInfo(name, linkName);
  }

  Future<_ObjectMetadataCommit> _publishMetadata(
    String objectName,
    Map<String, Object?> body,
    BigInt expectedRevision, {
    required String expectedIdentifier,
    required bool concurrentConflict,
    required int generation,
  }) async {
    final subject = _objectProtocol.metadataSubject(objectName);
    late final BigInt committedRevision;
    try {
      committedRevision = await _objectProtocol.publish(
        subject,
        encodeExactJson(body),
        headers: NatsHeaders(
          entries: [
            const MapEntry("Nats-Rollup", "sub"),
            MapEntry(
              "Nats-Expected-Last-Subject-Sequence",
              "$expectedRevision",
            ),
            MapEntry("Nats-Expected-Last-Subject-Sequence-Subject", subject),
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
        if (concurrentConflict) {
          throw JetStreamObjectMetadataConflictException(
            name,
            objectName,
            expectedRevision,
            error,
          );
        }
        throw JetStreamObjectAlreadyExistsException(
          name,
          objectName,
          cause: error,
        );
      }
      rethrow;
    } on NatsPublicationPreflightException {
      rethrow;
    } on Object catch (error) {
      throw JetStreamObjectMutationAmbiguousException(name, objectName, error);
    }
    try {
      final stored = await _objectProtocol.getStored({
        "seq": committedRevision,
      });
      if (stored == null ||
          stored.subject != subject ||
          stored.sequence != committedRevision) {
        throw JetStreamObjectIntegrityException(
          "Committed metadata revision could not be read back",
          bucket: name,
          name: objectName,
        );
      }
      final committed = decodeObjectInfoPayload(
        stored.payload,
        subject: stored.requestSubject,
        expectedBucket: name,
        expectedName: objectName,
        modified: stored.time,
        revision: stored.sequence,
      );
      if (committed.identifier != expectedIdentifier ||
          !objectInfoMatchesEncoding(committed, body)) {
        throw JetStreamObjectIntegrityException(
          "Committed metadata identity does not match the publication",
          bucket: name,
          name: objectName,
        );
      }
      return _ObjectMetadataCommit(committedRevision, committed, null);
    } on Object catch (error) {
      return _ObjectMetadataCommit(committedRevision, null, error);
    }
  }

  JetStreamObjectProtocol get _objectProtocol =>
      JetStreamObjectProtocol(connection, apiPrefix, timeout, name);
}

final class _ObjectMetadataCommit {
  const _ObjectMetadataCommit(this.revision, this.info, this.failure);

  final BigInt revision;
  final JetStreamObjectInfo? info;
  final Object? failure;

  JetStreamObjectInfo requireInfo(
    String bucket,
    String name, {
    Object? cleanupCause,
  }) {
    final value = info;
    if (value != null) {
      return value;
    }
    throw JetStreamObjectCommitVerificationException(
      bucket,
      name,
      revision,
      failure!,
      cleanupCause: cleanupCause,
    );
  }
}
