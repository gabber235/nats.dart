import "dart:typed_data";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/byte_snapshot.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/identifiers.dart";
import "package:nats_jetstream/src/stream_models.dart";
import "package:nats_jetstream/src/stream_transfer_models.dart";
import "package:nats_jetstream/src/stream_wire.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// One sequential bounded stream restore upload owner.
final class JetStreamRestoreTransfer {
  /// Creates a transfer owner.
  const JetStreamRestoreTransfer(
    this.connection,
    this.apiPrefix,
    this.managementTimeout,
  );

  /// Core connection used for management and upload requests.
  final NatsConnection connection;

  /// Domain aware JetStream API prefix.
  final String apiPrefix;

  /// Deadline for the initial restore request.
  final Duration managementTimeout;

  /// Restores one exact snapshot from a bounded source stream.
  ///
  /// Producers must treat emitted lists as immutable. Each event is copied
  /// immediately when delivered, before this client awaits network work.
  Future<JetStreamStreamInfo> restoreFrom(
    JetStreamSnapshotMetadata metadata,
    Stream<List<int>> source, {
    required BigInt maximumBytes,
    JetStreamRestoreOptions? options,
  }) async {
    validateJetStreamStreamName(metadata.stream);
    if (maximumBytes <= BigInt.zero) {
      throw ArgumentError.value(
        maximumBytes,
        "maximumBytes",
        "Must be positive",
      );
    }
    final effective = options ?? JetStreamRestoreOptions();
    final generation = connection.generation;
    final subject = "$apiPrefix.STREAM.RESTORE.${metadata.stream}";
    String? deliverSubject;
    var accepted = false;
    var bytes = BigInt.zero;
    try {
      final initial = await connection.requestOnGeneration(
        subject,
        encodeExactJson({"config": metadata.config, "state": metadata.state}),
        generation: generation,
        headers: NatsHeaders(
          entries: const [MapEntry("Nats-Required-Api-Level", "4")],
        ),
        timeout: managementTimeout,
      );
      final response = JetStreamWireResponse.decode(initial.payload, subject)
        ..throwApiError();
      deliverSubject = response.requiredString("deliver_subject");
      validatePublishSubject(deliverSubject);
      accepted = true;

      final buffer = Uint8List(effective.uploadChunkSize);
      var buffered = 0;
      await for (final sourceEvent in source) {
        final event = checkedJetStreamByteSnapshot(
          sourceEvent,
          name: "restore stream",
        );
        var offset = 0;
        while (offset < event.length) {
          final copied = _minimum(
            event.length - offset,
            effective.uploadChunkSize - buffered,
          );
          buffer.setRange(buffered, buffered + copied, event, offset);
          buffered += copied;
          offset += copied;
          if (buffered == effective.uploadChunkSize) {
            bytes = await _sendChunk(
              deliverSubject,
              Uint8List.fromList(buffer),
              bytes,
              maximumBytes,
              generation,
              effective.chunkTimeout,
            );
            buffered = 0;
          }
        }
      }
      if (buffered > 0) {
        bytes = await _sendChunk(
          deliverSubject,
          Uint8List.sublistView(buffer, 0, buffered),
          bytes,
          maximumBytes,
          generation,
          effective.chunkTimeout,
        );
      }

      final completion = await connection.requestOnGeneration(
        deliverSubject,
        const [],
        generation: generation,
        timeout: effective.completionTimeout,
      );
      final finalResponse = JetStreamWireResponse.decode(
        completion.payload,
        deliverSubject,
      )..throwApiError();
      return requireStreamIdentity(
        finalResponse,
        decodeStreamInfo(finalResponse),
        metadata.stream,
      );
    } on Object catch (error, stackTrace) {
      if (!accepted || deliverSubject == null) rethrow;
      try {
        await connection.publishOnGeneration(deliverSubject, const [
          0,
        ], generation: generation);
      } on Object {
        // The server watchdog owns cleanup after a lost generation.
      }
      Error.throwWithStackTrace(
        JetStreamRestoreException(partialBytes: bytes, cause: error),
        stackTrace,
      );
    }
  }

  Future<BigInt> _sendChunk(
    String subject,
    Uint8List chunk,
    BigInt bytes,
    BigInt maximumBytes,
    int generation,
    Duration timeout,
  ) async {
    final nextBytes = bytes + BigInt.from(chunk.length);
    if (nextBytes > maximumBytes) {
      throw StateError("Restore exceeds maximumBytes");
    }
    final acknowledgement = await connection.requestOnGeneration(
      subject,
      chunk,
      generation: generation,
      timeout: timeout,
    );
    if (acknowledgement.payload.isNotEmpty) {
      final response = JetStreamWireResponse.decode(
        acknowledgement.payload,
        subject,
      )..throwApiError();
      throw JetStreamProtocolException(
        "Restore chunk acknowledgement must be empty",
        subject: response.subject,
      );
    }
    return nextBytes;
  }
}

int _minimum(int left, int right) => left < right ? left : right;
