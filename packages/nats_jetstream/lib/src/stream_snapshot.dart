import "dart:async";
import "dart:typed_data";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/identifiers.dart";
import "package:nats_jetstream/src/stream_transfer_models.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// One bounded stream snapshot download owner.
final class JetStreamSnapshotTransfer {
  /// Creates a transfer owner.
  const JetStreamSnapshotTransfer(this.connection, this.apiPrefix);

  /// Core connection used for management, chunks, and flow acknowledgements.
  final NatsConnection connection;

  /// Domain aware JetStream API prefix.
  final String apiPrefix;

  /// Streams one snapshot into [write] without buffering the archive.
  Future<JetStreamSnapshotResult> snapshotTo(
    String stream,
    JetStreamSnapshotSink write, {
    required BigInt maximumBytes,
    JetStreamSnapshotOptions? options,
  }) async {
    validateJetStreamStreamName(stream);
    if (maximumBytes <= BigInt.zero) {
      throw ArgumentError.value(
        maximumBytes,
        "maximumBytes",
        "Must be positive",
      );
    }
    final effective = options ?? JetStreamSnapshotOptions();
    final timeout = effective.timeout;
    final clock = Stopwatch()..start();
    final generation = connection.generation;
    final inbox = connection.newInbox();
    final subject = "$apiPrefix.STREAM.SNAPSHOT.$stream";
    NatsSubscription? subscription;
    StreamIterator<NatsMessage>? iterator;
    var accepted = false;
    var bytes = BigInt.zero;
    var chunks = 0;
    try {
      subscription = await connection.subscribe(
        inbox,
        options: NatsSubscriptionOptions(
          maximumMessages: effective.maximumBufferedBytes ~/ 1024 + 2,
          maximumBytes: effective.maximumBufferedBytes,
          reconnect: NatsSubscriptionReconnectPolicy.close,
        ),
      );
      iterator = StreamIterator(subscription.messages);
      await connection.flush(timeout: _remaining(clock, timeout));
      _requireGeneration(generation, "before snapshot request");
      final initial = await connection.requestOnGeneration(
        subject,
        encodeExactJson({
          "deliver_subject": inbox,
          "no_consumers": !effective.includeConsumers,
          "chunk_size": effective.chunkSize,
          "window_size": effective.windowSize,
          "jsck": effective.checkMessages,
        }),
        generation: generation,
        headers: NatsHeaders(
          entries: const [MapEntry("Nats-Required-Api-Level", "4")],
        ),
        timeout: _remaining(clock, timeout),
      );
      final response = JetStreamWireResponse.decode(initial.payload, subject)
        ..throwApiError();
      final metadata = JetStreamSnapshotMetadata(
        config: response.requiredObject("config"),
        state: response.requiredObject("state"),
      );
      if (metadata.stream != stream) {
        throw JetStreamProtocolException(
          "Snapshot response stream identity does not match the request",
          subject: subject,
          fieldPath: "config.name",
        );
      }
      accepted = true;

      while (true) {
        final message = await _next(iterator, clock, timeout);
        final status = message.headers?.statusCode;
        if (message.payload.isEmpty && message.replySubject == null) {
          if (status == null || status == 204) {
            return JetStreamSnapshotResult(
              metadata: metadata,
              bytes: bytes,
              chunks: chunks,
            );
          }
          throw JetStreamSnapshotException(
            partialBytes: bytes,
            cause: JetStreamProtocolException(
              "Snapshot terminated with NATS status $status",
              subject: subject,
            ),
            statusCode: status,
            statusDescription: message.headers?.statusDescription,
          );
        }
        if (status != null || message.payload.isEmpty) {
          throw JetStreamProtocolException(
            "Snapshot data chunk has invalid status or payload",
            subject: subject,
          );
        }
        final nextBytes = bytes + BigInt.from(message.payload.length);
        if (nextBytes > maximumBytes) {
          throw StateError("Snapshot exceeds maximumBytes");
        }
        final chunk = Uint8List.fromList(message.payload).asUnmodifiableView();
        await Future<void>.sync(() => write(chunk)).timeout(
          _remaining(clock, timeout),
          onTimeout: () =>
              throw NatsTimeoutException("JetStream snapshot sink"),
        );
        bytes = nextBytes;
        chunks += 1;
        if (message.replySubject case final acknowledgement?) {
          await connection.publishOnGeneration(
            acknowledgement,
            const [],
            generation: generation,
          );
        }
      }
    } on JetStreamSnapshotException {
      rethrow;
    } on Object catch (error, stackTrace) {
      if (!accepted) rethrow;
      Error.throwWithStackTrace(
        JetStreamSnapshotException(partialBytes: bytes, cause: error),
        stackTrace,
      );
    } finally {
      await iterator?.cancel();
      await subscription?.unsubscribe();
    }
  }

  Future<NatsMessage> _next(
    StreamIterator<NatsMessage> iterator,
    Stopwatch clock,
    Duration timeout,
  ) async {
    late final bool available;
    try {
      available = await iterator.moveNext().timeout(_remaining(clock, timeout));
    } on TimeoutException {
      throw NatsTimeoutException("JetStream snapshot transfer");
    }
    if (!available) {
      throw const NatsConnectionException(
        "Snapshot delivery closed before its terminator",
      );
    }
    return iterator.current;
  }

  void _requireGeneration(int generation, String phase) {
    if (!connection.isConnected || connection.generation != generation) {
      throw NatsConnectionException("Connection generation changed $phase");
    }
  }
}

Duration _remaining(Stopwatch clock, Duration timeout) {
  final remaining = timeout - clock.elapsed;
  if (remaining <= Duration.zero) {
    throw NatsTimeoutException("JetStream snapshot transfer");
  }
  return remaining;
}
