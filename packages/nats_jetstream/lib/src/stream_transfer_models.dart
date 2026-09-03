import "dart:async";
import "dart:typed_data";

import "package:nats_jetstream/src/exact_json.dart";

/// Immutable metadata required to restore one stream snapshot.
final class JetStreamSnapshotMetadata {
  /// Creates an exact metadata snapshot.
  JetStreamSnapshotMetadata({
    required Map<String, Object?> config,
    required Map<String, Object?> state,
  }) : config = immutableExactJsonObject(config),
       state = immutableExactJsonObject(state) {
    final name = this.config["name"];
    if (name is! String || name.isEmpty) {
      throw ArgumentError.value(config, "config", "Stream name is required");
    }
  }

  /// Exact server stream configuration.
  final Map<String, Object?> config;

  /// Exact server stream state.
  final Map<String, Object?> state;

  /// Stream identity stored in the snapshot.
  String get stream => config["name"]! as String;
}

/// Successful bounded snapshot transfer summary.
final class JetStreamSnapshotResult {
  /// Creates an immutable snapshot result.
  const JetStreamSnapshotResult({
    required this.metadata,
    required this.bytes,
    required this.chunks,
  });

  /// Exact metadata used for restore.
  final JetStreamSnapshotMetadata metadata;

  /// Archive bytes accepted by the sink.
  final BigInt bytes;

  /// Data chunks accepted by the sink.
  final int chunks;
}

/// Server snapshot production and flow control settings.
final class JetStreamSnapshotOptions {
  /// Creates validated snapshot settings.
  factory JetStreamSnapshotOptions({
    bool includeConsumers = true,
    bool checkMessages = false,
    int chunkSize = 128 * 1024,
    int windowSize = 8 * 1024 * 1024,
    int maximumBufferedBytes = 16 * 1024 * 1024,
    Duration timeout = const Duration(seconds: 30),
  }) {
    if (chunkSize < 1024 || chunkSize > 1024 * 1024) {
      throw ArgumentError.value(
        chunkSize,
        "chunkSize",
        "Must be between 1024 and 1048576 bytes",
      );
    }
    if (windowSize < 1024 || windowSize > 32 * 1024 * 1024) {
      throw ArgumentError.value(
        windowSize,
        "windowSize",
        "Must be between 1024 and 33554432 bytes",
      );
    }
    if (windowSize < chunkSize) {
      throw ArgumentError.value(
        windowSize,
        "windowSize",
        "Must not be smaller than chunkSize",
      );
    }
    if (maximumBufferedBytes < windowSize) {
      throw ArgumentError.value(
        maximumBufferedBytes,
        "maximumBufferedBytes",
        "Must not be smaller than windowSize",
      );
    }
    if (timeout <= Duration.zero) {
      throw ArgumentError.value(timeout, "timeout", "Must be positive");
    }
    return JetStreamSnapshotOptions._(
      includeConsumers,
      checkMessages,
      chunkSize,
      windowSize,
      maximumBufferedBytes,
      timeout,
    );
  }

  const JetStreamSnapshotOptions._(
    this.includeConsumers,
    this.checkMessages,
    this.chunkSize,
    this.windowSize,
    this.maximumBufferedBytes,
    this.timeout,
  );

  /// Whether consumer state is included.
  final bool includeConsumers;

  /// Whether the server verifies stored message checksums.
  final bool checkMessages;

  /// Requested server chunk size.
  final int chunkSize;

  /// Requested server flow control window.
  final int windowSize;

  /// Local subscription retention bound.
  final int maximumBufferedBytes;

  /// Total snapshot operation deadline.
  final Duration timeout;
}

/// Sequential restore upload settings.
final class JetStreamRestoreOptions {
  /// Creates validated restore settings.
  factory JetStreamRestoreOptions({
    int uploadChunkSize = 128 * 1024,
    Duration chunkTimeout = const Duration(seconds: 4),
    Duration completionTimeout = const Duration(seconds: 30),
  }) {
    if (uploadChunkSize <= 0 || uploadChunkSize > 1024 * 1024) {
      throw ArgumentError.value(
        uploadChunkSize,
        "uploadChunkSize",
        "Must be between 1 and 1048576 bytes",
      );
    }
    if (chunkTimeout <= Duration.zero ||
        chunkTimeout >= const Duration(seconds: 5)) {
      throw ArgumentError.value(
        chunkTimeout,
        "chunkTimeout",
        "Must be positive and shorter than five seconds",
      );
    }
    if (completionTimeout <= Duration.zero) {
      throw ArgumentError.value(
        completionTimeout,
        "completionTimeout",
        "Must be positive",
      );
    }
    return JetStreamRestoreOptions._(
      uploadChunkSize,
      chunkTimeout,
      completionTimeout,
    );
  }

  const JetStreamRestoreOptions._(
    this.uploadChunkSize,
    this.chunkTimeout,
    this.completionTimeout,
  );

  /// Maximum raw bytes in one restore request.
  final int uploadChunkSize;

  /// Deadline for one chunk write and acknowledgement.
  final Duration chunkTimeout;

  /// Deadline for final archive validation and stream creation.
  final Duration completionTimeout;
}

/// Snapshot transfer failed after initial acceptance.
final class JetStreamSnapshotException implements Exception {
  /// Creates a failure with exact accepted progress.
  const JetStreamSnapshotException({
    required this.partialBytes,
    required this.cause,
    this.statusCode,
    this.statusDescription,
  });

  /// Bytes already accepted by the sink.
  final BigInt partialBytes;

  /// Underlying typed failure.
  final Object cause;

  /// Terminal NATS status, when supplied.
  final int? statusCode;

  /// Terminal NATS status description, when supplied.
  final String? statusDescription;

  @override
  String toString() {
    final status = statusCode == null ? "" : " with status $statusCode";
    return "JetStream snapshot failed after $partialBytes bytes$status";
  }
}

/// Restore transfer failed after initial acceptance.
final class JetStreamRestoreException implements Exception {
  /// Creates a failure with exact uploaded progress.
  const JetStreamRestoreException({
    required this.partialBytes,
    required this.cause,
  });

  /// Source bytes acknowledged by the server.
  final BigInt partialBytes;

  /// Underlying typed failure.
  final Object cause;

  @override
  String toString() => "JetStream restore failed after $partialBytes bytes";
}

/// Archive sink callback.
typedef JetStreamSnapshotSink = FutureOr<void> Function(Uint8List chunk);
