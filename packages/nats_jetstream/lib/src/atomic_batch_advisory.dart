import "dart:async";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/identifiers.dart";
import "package:nats_jetstream/src/timestamp.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// Prefix used by every atomic batch abandonment advisory.
const jetStreamAtomicBatchAbandonedAdvisoryPrefix =
    r"$JS.EVENT.ADVISORY.STREAM.BATCH_ABANDONED";

/// Stable abandonment categories understood by this client version.
enum JetStreamAtomicBatchAbandonKind {
  /// No batch traffic arrived within the server inactivity deadline.
  timeout,

  /// The batch exceeded a server resource or message limit.
  large,

  /// The commit sequence proved that messages were missing.
  incomplete,

  /// A message used requirements unsupported by atomic batches.
  unsupported,
}

/// Forward compatible wire reason for an abandoned atomic batch.
final class JetStreamAtomicBatchAbandonReason {
  const JetStreamAtomicBatchAbandonReason._(this.wireValue, this.knownKind);

  /// Parses a server reason while retaining values added by newer servers.
  factory JetStreamAtomicBatchAbandonReason.fromWire(String value) {
    if (value.isEmpty) {
      throw ArgumentError.value(value, "value", "Must not be empty");
    }
    return switch (value) {
      "timeout" => timeout,
      "large" => large,
      "incomplete" => incomplete,
      "unsupported" => unsupported,
      _ => JetStreamAtomicBatchAbandonReason._(value, null),
    };
  }

  /// The server inactivity deadline expired.
  static const timeout = JetStreamAtomicBatchAbandonReason._(
    "timeout",
    JetStreamAtomicBatchAbandonKind.timeout,
  );

  /// A server batch limit was exceeded.
  static const large = JetStreamAtomicBatchAbandonReason._(
    "large",
    JetStreamAtomicBatchAbandonKind.large,
  );

  /// The batch sequence was incomplete.
  static const incomplete = JetStreamAtomicBatchAbandonReason._(
    "incomplete",
    JetStreamAtomicBatchAbandonKind.incomplete,
  );

  /// Atomic batch requirements were not met.
  static const unsupported = JetStreamAtomicBatchAbandonReason._(
    "unsupported",
    JetStreamAtomicBatchAbandonKind.unsupported,
  );

  /// Exact server value, including values added by newer servers.
  final String wireValue;

  /// Known category, or `null` for a newer server value.
  final JetStreamAtomicBatchAbandonKind? knownKind;

  @override
  bool operator ==(Object other) =>
      other is JetStreamAtomicBatchAbandonReason &&
      other.wireValue == wireValue;

  @override
  int get hashCode => wireValue.hashCode;

  @override
  String toString() => wireValue;
}

/// Typed proof that one stream discarded an atomic batch without committing it.
final class JetStreamAtomicBatchAbandonedAdvisory {
  JetStreamAtomicBatchAbandonedAdvisory._({
    required this.type,
    required this.id,
    required this.timestamp,
    required this.account,
    required this.domain,
    required this.stream,
    required this.batchId,
    required this.reason,
    required Map<String, Object?> raw,
  }) : raw = immutableExactJsonObject(raw);

  /// Exact NATS event schema type.
  final String type;

  /// Server generated event identity.
  final String id;

  /// Exact RFC 3339 event timestamp.
  final String timestamp;

  /// Publishing account, when supplied by the server.
  final String? account;

  /// JetStream domain, when supplied by the server.
  final String? domain;

  /// Stream that owned the discarded batch.
  final String stream;

  /// Discarded batch identity.
  final String batchId;

  /// Exact forward compatible abandonment reason.
  final JetStreamAtomicBatchAbandonReason reason;

  /// Deeply immutable exact advisory JSON.
  final Map<String, Object?> raw;

  /// Whether this advisory proves the outcome of one exact operation.
  bool matches({required String stream, required String batchId}) =>
      this.stream == stream && this.batchId == batchId;
}

/// Definitive proof that an attempted atomic batch did not commit.
final class JetStreamAtomicBatchAbandonedException implements Exception {
  /// Creates a noncommit failure from a matching server advisory.
  const JetStreamAtomicBatchAbandonedException({
    required this.advisory,
    required this.cause,
    required this.causeStackTrace,
  });

  /// Matching server proof of abandonment.
  final JetStreamAtomicBatchAbandonedAdvisory advisory;

  /// Publication failure observed before confirmation.
  final Object cause;

  /// Original publication failure stack trace.
  final StackTrace causeStackTrace;

  @override
  String toString() => "Atomic batch was definitively abandoned: $cause";
}

/// Atomic batch outcome that cannot be proven committed or abandoned.
final class JetStreamAtomicBatchAmbiguousException implements Exception {
  /// Creates an ambiguous outcome while preserving both failure channels.
  const JetStreamAtomicBatchAmbiguousException({
    required this.stream,
    required this.batchId,
    required this.confirmationTimeout,
    required this.cause,
    required this.causeStackTrace,
    this.observationCause,
    this.observationCauseStackTrace,
  });

  /// Stream that may have committed the batch.
  final String stream;

  /// Stable batch identity used for reconciliation.
  final String batchId;

  /// Time spent waiting for definitive abandonment proof.
  final Duration confirmationTimeout;

  /// Publication failure that made the outcome ambiguous.
  final Object cause;

  /// Original publication failure stack trace.
  final StackTrace causeStackTrace;

  /// Malformed advisory or watcher failure, when one was observed.
  final Object? observationCause;

  /// Original watcher failure stack trace.
  final StackTrace? observationCauseStackTrace;

  @override
  String toString() =>
      "Atomic batch $stream/$batchId has an ambiguous outcome: $cause";
}

/// Decodes one atomic batch abandonment advisory and verifies its subject.
JetStreamAtomicBatchAbandonedAdvisory
decodeJetStreamAtomicBatchAbandonedAdvisory(NatsMessage message) {
  final expectedPrefix = "$jetStreamAtomicBatchAbandonedAdvisoryPrefix.";
  if (!message.subject.startsWith(expectedPrefix)) {
    throw JetStreamProtocolException(
      "Unexpected atomic batch abandonment advisory subject",
      subject: message.subject,
    );
  }
  final subjectStream = message.subject.substring(expectedPrefix.length);
  try {
    validateJetStreamStreamName(subjectStream);
  } on ArgumentError catch (error) {
    throw JetStreamProtocolException(
      "Invalid stream in atomic batch abandonment advisory subject",
      subject: message.subject,
      cause: error,
    );
  }

  final response = JetStreamWireResponse.decode(
    message.payload,
    message.subject,
  );
  final type = response.requiredString("type");
  if (type != "io.nats.jetstream.advisory.v1.stream_batch_abandoned") {
    throw JetStreamProtocolException(
      "Unexpected atomic batch abandonment advisory type",
      subject: message.subject,
      fieldPath: "type",
    );
  }
  final id = _requiredNonEmpty(response, "id");
  final timestamp = _requiredNonEmpty(response, "timestamp");
  try {
    validateJetStreamRfc3339(timestamp, "timestamp");
  } on ArgumentError catch (error) {
    throw JetStreamProtocolException(
      "Atomic batch abandonment timestamp must be RFC 3339",
      subject: message.subject,
      fieldPath: "timestamp",
      cause: error,
    );
  }
  final stream = _requiredNonEmpty(response, "stream");
  try {
    validateJetStreamStreamName(stream);
  } on ArgumentError catch (error) {
    throw JetStreamProtocolException(
      "Invalid stream in atomic batch abandonment advisory",
      subject: message.subject,
      fieldPath: "stream",
      cause: error,
    );
  }
  if (stream != subjectStream) {
    throw JetStreamProtocolException(
      "Atomic batch abandonment stream does not match its subject",
      subject: message.subject,
      fieldPath: "stream",
    );
  }
  final batchId = _requiredNonEmpty(response, "batch");
  final reasonValue = _requiredNonEmpty(response, "reason");
  return JetStreamAtomicBatchAbandonedAdvisory._(
    type: type,
    id: id,
    timestamp: timestamp,
    account: response.optionalString("account"),
    domain: response.optionalString("domain"),
    stream: stream,
    batchId: batchId,
    reason: JetStreamAtomicBatchAbandonReason.fromWire(reasonValue),
    raw: response.body,
  );
}

/// One bounded Core subscription for atomic batch abandonment advisories.
///
/// [advisories] has one listener. Cancel that listener or call [close] to
/// release server interest. Reconnects replay the subscription before the new
/// connection generation becomes visible.
final class JetStreamAtomicBatchAdvisoryWatcher {
  const JetStreamAtomicBatchAdvisoryWatcher._(this.subject, this._subscription);

  /// Opens a global or stream scoped advisory subscription.
  ///
  /// A server subscription permission rejection is detected before this
  /// future succeeds and is thrown as [NatsPermissionException].
  static Future<JetStreamAtomicBatchAdvisoryWatcher> connect(
    NatsConnection connection, {
    String? stream,
    Duration timeout = const Duration(seconds: 5),
  }) async {
    if (timeout <= Duration.zero) {
      throw ArgumentError.value(timeout, "timeout", "Must be positive");
    }
    if (stream != null) validateJetStreamStreamName(stream);
    final subject = stream == null
        ? "$jetStreamAtomicBatchAbandonedAdvisoryPrefix.*"
        : "$jetStreamAtomicBatchAbandonedAdvisoryPrefix.$stream";
    NatsPermissionException? permissionFailure;
    final serverEvents = connection.events.listen((event) {
      if (event case NatsServerError(error: final NatsPermissionException error)
          when error.operation == NatsPermissionOperation.subscription &&
              error.subject == subject) {
        permissionFailure = error;
      }
    });
    NatsSubscription? subscription;
    try {
      subscription = await connection.subscribe(subject);
      await connection.flush(timeout: timeout);
      if (permissionFailure != null) throw permissionFailure!;
      return JetStreamAtomicBatchAdvisoryWatcher._(subject, subscription);
    } on Object {
      await subscription?.unsubscribe();
      rethrow;
    } finally {
      await serverEvents.cancel();
    }
  }

  /// Exact Core subscription subject.
  final String subject;

  final NatsSubscription _subscription;

  /// Ordered typed advisories. Malformed payloads are emitted as stream errors.
  Stream<JetStreamAtomicBatchAbandonedAdvisory> get advisories =>
      _subscription.messages.map(decodeJetStreamAtomicBatchAbandonedAdvisory);

  /// Completes after the underlying subscription terminates.
  Future<void> get done => _subscription.done;

  /// Idempotently releases server interest and queued advisories.
  Future<void> close() => _subscription.unsubscribe();
}

String _requiredNonEmpty(JetStreamWireResponse response, String field) {
  final value = response.requiredString(field);
  if (value.isEmpty) {
    throw JetStreamProtocolException(
      "Field $field must not be empty",
      subject: response.subject,
      fieldPath: field,
    );
  }
  return value;
}
