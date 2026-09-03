import "package:nats_jetstream/src/fast_batch.dart";
import "package:nats_jetstream/src/object_store.dart";

/// Requested object metadata does not exist.
final class JetStreamObjectNotFoundException implements Exception {
  /// Creates a not found failure.
  const JetStreamObjectNotFoundException(this.bucket, this.name);

  /// Object bucket.
  final String bucket;

  /// Requested object name.
  final String name;

  @override
  String toString() => "JetStreamObjectNotFoundException: $bucket/$name";
}

/// Object bytes failed an integrity or structural check.
final class JetStreamObjectIntegrityException implements Exception {
  /// Creates an integrity failure without including object bytes.
  const JetStreamObjectIntegrityException(
    this.message, {
    required this.bucket,
    required this.name,
    this.expectedDigest,
    this.actualDigest,
  });

  /// Safe diagnostic.
  final String message;

  /// Object bucket.
  final String bucket;

  /// Object name.
  final String name;

  /// Committed digest, when available.
  final String? expectedDigest;

  /// Calculated digest, when available.
  final String? actualDigest;

  @override
  String toString() => "JetStreamObjectIntegrityException: $message";
}

/// Upload failed before or during the final metadata commit.
final class JetStreamObjectPutException implements Exception {
  /// Creates an upload failure.
  const JetStreamObjectPutException(
    this.message, {
    required this.cause,
    required this.metadataCommitAmbiguous,
    this.cleanupCause,
  });

  /// Safe diagnostic.
  final String message;

  /// Original publication failure.
  final Object cause;

  /// Whether metadata may have committed before the failure was observed.
  final bool metadataCommitAmbiguous;

  /// Partial chunk cleanup failure, when cleanup also failed.
  final Object? cleanupCause;

  /// Whether fast chunk ingest may have persisted an unproven suffix.
  bool get chunkUploadAmbiguous =>
      cause is JetStreamFastBatchException &&
      (cause as JetStreamFastBatchException).ambiguous;

  @override
  String toString() => "JetStreamObjectPutException: $message";
}

/// New object state committed, but superseded state cleanup failed.
final class JetStreamObjectCleanupException implements Exception {
  /// Creates a postcommit cleanup failure.
  const JetStreamObjectCleanupException(this.committed, this.cause);

  /// Successfully committed new object information.
  final JetStreamObjectInfo committed;

  /// Superseded chunk or metadata cleanup failure.
  final Object cause;

  @override
  String toString() =>
      "JetStreamObjectCleanupException: object committed, cleanup failed";
}

/// A streaming download ended before every byte could be verified.
final class JetStreamObjectDownloadCancelledException implements Exception {
  /// Creates an early cancellation failure without exposing object contents.
  const JetStreamObjectDownloadCancelledException(this.bucket, this.name);

  /// Object bucket.
  final String bucket;

  /// Object name.
  final String name;

  @override
  String toString() =>
      "JetStreamObjectDownloadCancelledException: $bucket/$name";
}

/// A metadata destination already contains a nonreplaceable object.
final class JetStreamObjectAlreadyExistsException implements Exception {
  /// Creates a collision with an optional server compare and set rejection.
  const JetStreamObjectAlreadyExistsException(
    this.bucket,
    this.name, {
    this.cause,
  });

  /// Object bucket.
  final String bucket;

  /// Colliding object name.
  final String name;

  /// Server rejection when the collision was concurrent.
  final Object? cause;

  @override
  String toString() => "JetStreamObjectAlreadyExistsException: $bucket/$name";
}

/// A compare and set protected metadata replacement lost a concurrent race.
final class JetStreamObjectMetadataConflictException implements Exception {
  /// Creates a concurrent metadata mutation failure.
  const JetStreamObjectMetadataConflictException(
    this.bucket,
    this.name,
    this.expectedRevision,
    this.cause,
  );

  /// Object bucket.
  final String bucket;

  /// Concurrently modified object name.
  final String name;

  /// Revision this operation attempted to replace.
  final BigInt expectedRevision;

  /// Server compare and set rejection.
  final Object cause;

  @override
  String toString() =>
      "JetStreamObjectMetadataConflictException: $bucket/$name";
}

/// Metadata publication lost its response before commitment was known.
final class JetStreamObjectMutationAmbiguousException implements Exception {
  /// Creates an ambiguous mutation outcome.
  const JetStreamObjectMutationAmbiguousException(
    this.bucket,
    this.name,
    this.cause,
  );

  /// Object bucket.
  final String bucket;

  /// Object name.
  final String name;

  /// Transport or timeout failure before an acknowledgment arrived.
  final Object cause;

  @override
  String toString() =>
      "JetStreamObjectMutationAmbiguousException: $bucket/$name";
}

/// Metadata committed, but its exact snapshot could not be verified.
final class JetStreamObjectCommitVerificationException implements Exception {
  /// Creates a definite commit with unavailable verification.
  const JetStreamObjectCommitVerificationException(
    this.bucket,
    this.name,
    this.revision,
    this.cause, {
    this.cleanupCause,
  });

  /// Object bucket.
  final String bucket;

  /// Object name.
  final String name;

  /// Acknowledged committed stream revision.
  final BigInt revision;

  /// Exact readback failure.
  final Object cause;

  /// Postcommit cleanup failure, when cleanup also failed.
  final Object? cleanupCause;

  @override
  String toString() =>
      "JetStreamObjectCommitVerificationException: $bucket/$name committed at $revision";
}

/// Metadata compare and set state is temporarily unavailable on the server.
final class JetStreamObjectMetadataUnavailableException implements Exception {
  /// Creates a retryable metadata state failure.
  const JetStreamObjectMetadataUnavailableException(
    this.bucket,
    this.name,
    this.cause,
  );

  /// Object bucket.
  final String bucket;

  /// Object name.
  final String name;

  /// Server rejection with error code 10163.
  final Object cause;

  @override
  String toString() =>
      "JetStreamObjectMetadataUnavailableException: $bucket/$name";
}

/// Rename committed its destination while a newer source remained live.
final class JetStreamObjectRenameConflictException implements Exception {
  /// Creates a partial rename result.
  const JetStreamObjectRenameConflictException(
    this.committed,
    this.remainingSource,
  );

  /// Successfully committed destination metadata.
  final JetStreamObjectInfo committed;

  /// Newer source metadata preserved by bounded cleanup.
  final JetStreamObjectInfo remainingSource;

  @override
  String toString() =>
      "JetStreamObjectRenameConflictException: "
      "destination committed, source changed";
}

/// Reason an Object Store link could not resolve to object bytes.
enum JetStreamObjectLinkFailure {
  /// The link targets an entire bucket rather than one object.
  bucketLink,

  /// Following object links revisited an earlier bucket and name.
  cycle,

  /// Following links would exceed the caller supplied safety bound.
  maximumDepthExceeded,

  /// Link creation selected another link rather than object bytes.
  targetIsLink,

  /// Link creation crossed a connection or JetStream API context boundary.
  contextMismatch,
}

/// Object Store link resolution stopped without selecting object bytes.
final class JetStreamObjectLinkException implements Exception {
  /// Creates a safe link resolution failure.
  const JetStreamObjectLinkException(this.bucket, this.name, this.failure);

  /// Bucket containing the link where resolution stopped.
  final String bucket;

  /// Link name where resolution stopped.
  final String name;

  /// Stable machine readable failure reason.
  final JetStreamObjectLinkFailure failure;

  @override
  String toString() =>
      "JetStreamObjectLinkException: $bucket/$name ${failure.name}";
}

/// A bounded Object Store list exceeded caller supplied memory limits.
final class JetStreamObjectListLimitException implements Exception {
  /// Creates an inspectable bounded list failure.
  const JetStreamObjectListLimitException({
    required this.bucket,
    required this.maximumObjects,
    required this.maximumMetadataBytes,
    required this.observedObjects,
    required this.observedMetadataBytes,
  });

  /// Object Store bucket.
  final String bucket;

  /// Configured object count bound.
  final int maximumObjects;

  /// Configured encoded metadata byte bound.
  final int maximumMetadataBytes;

  /// Distinct object names observed before failure.
  final int observedObjects;

  /// Encoded metadata bytes retained before failure.
  final int observedMetadataBytes;

  @override
  String toString() =>
      "JetStreamObjectListLimitException: $bucket exceeded list bounds";
}
