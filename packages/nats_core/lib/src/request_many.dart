import "package:nats_core/src/message.dart";

/// Validated bounds and completion rules for a multiple response request.
final class NatsRequestManyOptions {
  /// Creates independent total, stall, count, sentinel, and buffer bounds.
  factory NatsRequestManyOptions({
    Duration timeout = const Duration(seconds: 2),
    Duration? stallTimeout,
    int? maximumReplies,
    bool completeOnEmptyPayload = false,
    int maximumBufferedMessages = 256,
    int maximumBufferedBytes = 8 * 1024 * 1024,
  }) {
    if (timeout <= Duration.zero) {
      throw ArgumentError.value(timeout, "timeout", "Must be positive");
    }
    if (stallTimeout != null && stallTimeout <= Duration.zero) {
      throw ArgumentError.value(
        stallTimeout,
        "stallTimeout",
        "Must be positive",
      );
    }
    if (stallTimeout != null && stallTimeout >= timeout) {
      throw ArgumentError.value(
        stallTimeout,
        "stallTimeout",
        "Must be shorter than the total timeout",
      );
    }
    if (maximumReplies != null && maximumReplies < 1) {
      throw ArgumentError.value(
        maximumReplies,
        "maximumReplies",
        "Must be positive",
      );
    }
    if (maximumBufferedMessages < 1 || maximumBufferedBytes < 0) {
      throw ArgumentError("Request reply buffer bounds are invalid");
    }
    return NatsRequestManyOptions._(
      timeout,
      stallTimeout,
      maximumReplies,
      completeOnEmptyPayload,
      maximumBufferedMessages,
      maximumBufferedBytes,
    );
  }

  const NatsRequestManyOptions._(
    this.timeout,
    this.stallTimeout,
    this.maximumReplies,
    this.completeOnEmptyPayload,
    this.maximumBufferedMessages,
    this.maximumBufferedBytes,
  );

  /// Absolute lifetime from request acceptance through completion.
  final Duration timeout;

  /// Optional maximum silence after the first or previous reply.
  final Duration? stallTimeout;

  /// Optional number of replies delivered before normal completion.
  final int? maximumReplies;

  /// Whether an empty payload completes without delivering that sentinel.
  final bool completeOnEmptyPayload;

  /// Maximum replies retained while application delivery is paused.
  final int maximumBufferedMessages;

  /// Maximum retained reply bytes, including subjects and headers.
  final int maximumBufferedBytes;
}

/// One multiple response request owned by its accepting connection generation.
abstract interface class NatsRequestMany {
  /// Ordered replies excluding an enabled empty payload sentinel.
  Stream<NatsMessage> get messages;

  /// Completes when reply collection terminates.
  Future<void> get done;

  /// Stops collection, discards buffered replies, and releases mux state.
  Future<void> stop();
}
