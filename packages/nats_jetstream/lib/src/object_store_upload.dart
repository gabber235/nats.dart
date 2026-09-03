import "dart:async";
import "dart:convert";
import "dart:typed_data";

import "package:crypto/crypto.dart";
import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/byte_snapshot.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/fast_batch.dart";
import "package:nats_jetstream/src/fast_batch_publisher.dart";
import "package:nats_jetstream/src/object_store.dart";
import "package:nats_jetstream/src/object_store_digest.dart";
import "package:nats_jetstream/src/object_store_encoding.dart";
import "package:nats_jetstream/src/object_store_error.dart";
import "package:nats_jetstream/src/object_store_protocol.dart";
import "package:nats_jetstream/src/object_store_stream.dart";
import "package:nats_jetstream/src/object_store_wire.dart";

/// Reads current metadata for an upload compare and set boundary.
typedef JetStreamObjectInfoReader =
    Future<JetStreamObjectInfo?> Function(
      String objectName, {
      bool showDeleted,
    });

/// Uploads bounded chunks and commits metadata only after the source completes.
Future<JetStreamObjectInfo> uploadJetStreamObject({
  required NatsConnection connection,
  required String apiPrefix,
  required Duration timeout,
  required String bucket,
  required JetStreamObjectMetadata metadata,
  required Stream<List<int>> bytes,
  required JetStreamObjectInfoReader getInfo,
  JetStreamObjectPutMode mode = JetStreamObjectPutMode.sequential,
}) async {
  final previous = await getInfo(metadata.name, showDeleted: true);
  final generation = connection.generation;
  final protocol = JetStreamObjectProtocol(
    connection,
    apiPrefix,
    timeout,
    bucket,
  );
  final identifier = randomObjectIdentifier();
  final chunkSubject = protocol.chunkSubject(identifier);
  final upload = _ObjectUploadAccumulator();

  try {
    await _publishObjectChunks(
      connection: connection,
      protocol: protocol,
      generation: generation,
      bucket: bucket,
      identifier: identifier,
      chunkSubject: chunkSubject,
      chunkSize: metadata.chunkSize,
      bytes: bytes,
      mode: mode,
      upload: upload,
    );
    upload.close();
  } on Object catch (error) {
    await _throwUploadFailure(protocol, chunkSubject, error);
  }

  final digest = objectDigestFromHash(upload.digest.bytes);
  final metadataSubject = protocol.metadataSubject(metadata.name);
  final metadataBody = encodeObjectInfo(
    metadata: metadata,
    bucket: bucket,
    identifier: identifier,
    size: upload.size,
    chunks: upload.chunks,
    digest: digest,
  );
  late final BigInt committedRevision;
  try {
    committedRevision = await protocol.publish(
      metadataSubject,
      encodeExactJson(metadataBody),
      headers: NatsHeaders(
        entries: [
          const MapEntry("Nats-Rollup", "sub"),
          MapEntry(
            "Nats-Expected-Last-Subject-Sequence",
            "${previous?.revision ?? BigInt.zero}",
          ),
          MapEntry(
            "Nats-Expected-Last-Subject-Sequence-Subject",
            metadataSubject,
          ),
        ],
      ),
      generation: generation,
    );
  } on JetStreamApiException catch (error) {
    await _throwCommitRejection(protocol, chunkSubject, error);
  } on NatsPublicationPreflightException catch (error) {
    await _throwCommitRejection(protocol, chunkSubject, error);
  } on NatsNoRespondersException catch (error) {
    await _throwCommitRejection(protocol, chunkSubject, error);
  } on Object catch (error) {
    throw JetStreamObjectPutException(
      "Object metadata commit outcome is unknown",
      cause: error,
      metadataCommitAmbiguous: true,
    );
  }

  JetStreamObjectInfo? committed;
  Object? verificationFailure;
  try {
    final stored = await protocol.getStored({"seq": committedRevision});
    if (stored == null ||
        stored.subject != metadataSubject ||
        stored.sequence != committedRevision) {
      throw JetStreamObjectIntegrityException(
        "Committed metadata revision could not be read back",
        bucket: bucket,
        name: metadata.name,
      );
    }
    final decoded = decodeObjectInfoPayload(
      stored.payload,
      subject: stored.requestSubject,
      expectedBucket: bucket,
      expectedName: metadata.name,
      modified: stored.time,
      revision: stored.sequence,
    );
    if (decoded.identifier != identifier ||
        !objectInfoMatchesEncoding(decoded, metadataBody)) {
      throw JetStreamObjectIntegrityException(
        "Committed metadata identity does not match the publication",
        bucket: bucket,
        name: metadata.name,
      );
    }
    committed = decoded;
  } on Object catch (error) {
    verificationFailure = error;
  }

  Object? cleanupFailure;
  if (previous != null && previous.identifier != identifier) {
    try {
      await protocol.purgeSubject(protocol.chunkSubject(previous.identifier));
    } on Object catch (error) {
      cleanupFailure = error;
    }
  }
  final snapshot = committed;
  if (snapshot == null) {
    throw JetStreamObjectCommitVerificationException(
      bucket,
      metadata.name,
      committedRevision,
      verificationFailure!,
      cleanupCause: cleanupFailure,
    );
  }
  if (cleanupFailure != null) {
    throw JetStreamObjectCleanupException(snapshot, cleanupFailure);
  }
  return snapshot;
}

Future<void> _publishObjectChunks({
  required NatsConnection connection,
  required JetStreamObjectProtocol protocol,
  required int generation,
  required String bucket,
  required String identifier,
  required String chunkSubject,
  required int chunkSize,
  required Stream<List<int>> bytes,
  required JetStreamObjectPutMode mode,
  required _ObjectUploadAccumulator upload,
}) async {
  final chunks = StreamIterator(_chunkObjectBytes(bytes, chunkSize));
  try {
    if (!await chunks.moveNext()) return;
    switch (mode) {
      case JetStreamObjectPutMode.sequential:
        do {
          final chunk = chunks.current;
          await protocol.publish(chunkSubject, chunk, generation: generation);
          upload.add(chunk);
        } while (await chunks.moveNext());
      case JetStreamObjectPutMode.experimentalFastIngest:
        final maximumOutstandingBytes = chunkSize < 8 * 1024 * 1024
            ? 8 * 1024 * 1024
            : chunkSize + 64 * 1024;
        final chunkHeaders = NatsHeaders(
          entries: [MapEntry("Nats-Expected-Stream", objectStreamName(bucket))],
        );
        final acknowledgement =
            await JetStreamFastBatchPublisher(
              connection,
              expectedGeneration: generation,
            ).publish(
              _fastChunkMessages(chunks, chunkSubject, chunkHeaders, upload),
              options: JetStreamFastBatchOptions(
                batchId: identifier,
                maximumOutstandingBytes: maximumOutstandingBytes,
                // ignore: avoid_redundant_argument_values
                gapPolicy: JetStreamFastBatchGapPolicy.fail,
              ),
            );
        final expectedChunks = upload.chunks;
        if (acknowledgement.stream != objectStreamName(bucket) ||
            acknowledgement.batchId != identifier ||
            acknowledgement.messageCount != expectedChunks ||
            acknowledgement.gaps.isNotEmpty ||
            acknowledgement.errors.isNotEmpty ||
            acknowledgement.totalGaps != 0 ||
            acknowledgement.totalErrors != 0) {
          throw JetStreamFastBatchException(
            message: "Object chunk fast batch returned incomplete evidence",
            batchId: identifier,
            sentSequence: expectedChunks,
            acknowledgedSequence: acknowledgement.acknowledgedBatchSequence,
            ambiguous: true,
            cause: acknowledgement,
          );
        }
    }
  } finally {
    await chunks.cancel();
  }
}

Stream<JetStreamFastBatchMessage> _fastChunkMessages(
  StreamIterator<Uint8List> chunks,
  String subject,
  NatsHeaders headers,
  _ObjectUploadAccumulator upload,
) async* {
  do {
    final chunk = chunks.current;
    upload.add(chunk);
    yield JetStreamFastBatchMessage(subject, chunk, headers: headers);
  } while (await chunks.moveNext());
}

Stream<Uint8List> _chunkObjectBytes(
  Stream<List<int>> bytes,
  int chunkSize,
) async* {
  var buffer = Uint8List(chunkSize);
  var buffered = 0;
  await for (final source in bytes) {
    final event = checkedJetStreamByteSnapshot(source, name: "object stream");
    var offset = 0;
    while (offset < event.length) {
      final copied = event.length - offset < buffer.length - buffered
          ? event.length - offset
          : buffer.length - buffered;
      buffer.setRange(buffered, buffered + copied, event, offset);
      buffered += copied;
      offset += copied;
      if (buffered == buffer.length) {
        final chunk = buffer;
        buffer = Uint8List(chunkSize);
        buffered = 0;
        yield chunk;
      }
    }
  }
  if (buffered != 0) {
    yield Uint8List.sublistView(buffer, 0, buffered);
  }
}

final class _ObjectUploadAccumulator {
  _ObjectUploadAccumulator() {
    // Sink ownership remains with this accumulator until close.
    // ignore: close_sinks
    _digestSink = sha256.startChunkedConversion(_result);
  }

  // Digest sink ownership transfers to the byte conversion sink.
  // ignore: close_sinks
  final _DigestResult _result = _DigestResult();
  late final ByteConversionSink _digestSink;
  var size = BigInt.zero;
  var chunks = BigInt.zero;

  Digest get digest => _result.value;

  void add(Uint8List chunk) {
    _digestSink.add(chunk);
    size += BigInt.from(chunk.length);
    chunks += BigInt.one;
  }

  void close() => _digestSink.close();
}

Future<Never> _throwUploadFailure(
  JetStreamObjectProtocol protocol,
  String chunkSubject,
  Object error,
) async {
  final cleanup = await _cleanup(protocol, chunkSubject);
  throw JetStreamObjectPutException(
    "Object chunk upload failed",
    cause: error,
    metadataCommitAmbiguous: false,
    cleanupCause: cleanup,
  );
}

Future<Never> _throwCommitRejection(
  JetStreamObjectProtocol protocol,
  String chunkSubject,
  Object error,
) async {
  final cleanup = await _cleanup(protocol, chunkSubject);
  throw JetStreamObjectPutException(
    "Object metadata commit was rejected",
    cause: error,
    metadataCommitAmbiguous: false,
    cleanupCause: cleanup,
  );
}

Future<Object?> _cleanup(
  JetStreamObjectProtocol protocol,
  String chunkSubject,
) async {
  try {
    await protocol.purgeSubject(chunkSubject);
    return null;
  } on Object catch (error) {
    return error;
  }
}

final class _DigestResult implements Sink<Digest> {
  Digest? _value;

  Digest get value => _value ?? (throw StateError("Digest is not complete"));

  @override
  void add(Digest data) {
    if (_value != null) {
      throw StateError("Digest completed more than once");
    }
    _value = data;
  }

  @override
  void close() {
    if (_value == null) {
      throw StateError("Digest did not complete");
    }
  }
}
