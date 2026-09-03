import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/stream_models.dart";

/// Whether removing the stream leader is permitted.
enum JetStreamLeaderRemoval {
  /// Reject a request that currently targets the stream leader.
  refuse,

  /// Permit a request that currently targets the stream leader.
  allow,
}

/// Topology observed after one stream peer removal mutation.
enum JetStreamPeerRemovalClassification {
  /// The removed server is absent and the configured replica count is restored.
  replaced,

  /// The removed server is absent and membership is below its configured size.
  removedWithoutReplacement,

  /// Available topology cannot prove either replacement outcome.
  unknown,
}

/// Exact result of the single peer removal mutation request.
sealed class JetStreamPeerRemovalMutationResult {
  const JetStreamPeerRemovalMutationResult();

  /// Whether NATS explicitly returned `success: true`.
  bool get serverReportedSuccess;

  /// Peer remap API rejection after a potentially partial mutation.
  JetStreamApiException? get mutationError;

  /// Transport failure that made mutation acceptance ambiguous.
  Object? get ambiguityCause;

  /// Stack trace associated with [ambiguityCause].
  StackTrace? get ambiguityCauseStackTrace;
}

/// NATS explicitly accepted the peer removal request.
final class JetStreamPeerRemovalAccepted
    extends JetStreamPeerRemovalMutationResult {
  /// Creates an accepted mutation result.
  const JetStreamPeerRemovalAccepted();

  @override
  bool get serverReportedSuccess => true;

  @override
  JetStreamApiException? get mutationError => null;

  @override
  Object? get ambiguityCause => null;

  @override
  StackTrace? get ambiguityCauseStackTrace => null;
}

/// NATS returned peer remap error `10075` after possible partial mutation.
final class JetStreamPeerRemovalRemapFailed
    extends JetStreamPeerRemovalMutationResult {
  /// Creates a partial mutation result from the exact server rejection.
  const JetStreamPeerRemovalRemapFailed(this.error);

  /// Exact `10075` API rejection returned by NATS.
  final JetStreamApiException error;

  @override
  bool get serverReportedSuccess => false;

  @override
  JetStreamApiException get mutationError => error;

  @override
  Object? get ambiguityCause => null;

  @override
  StackTrace? get ambiguityCauseStackTrace => null;
}

/// Transport failure after publication made mutation acceptance unknowable.
final class JetStreamPeerRemovalAmbiguous
    extends JetStreamPeerRemovalMutationResult {
  /// Creates an ambiguous mutation result while preserving its cause.
  const JetStreamPeerRemovalAmbiguous(this.cause, this.causeStackTrace);

  /// Underlying Core transport or timeout failure.
  final Object cause;

  /// Stack trace associated with [cause].
  final StackTrace causeStackTrace;

  @override
  bool get serverReportedSuccess => false;

  @override
  JetStreamApiException? get mutationError => null;

  @override
  Object get ambiguityCause => cause;

  @override
  StackTrace get ambiguityCauseStackTrace => causeStackTrace;
}

/// Reconciled result of one nonretrying stream peer removal mutation.
final class JetStreamPeerRemovalResult {
  /// Creates an immutable peer removal result.
  const JetStreamPeerRemovalResult({
    required this.stream,
    required this.removedServer,
    required this.classification,
    required this.mutation,
    required this.before,
    required this.lastInfo,
    required this.maximumWait,
    required this.reconciliationCause,
    required this.reconciliationCauseStackTrace,
  });

  /// Stream whose assignment was mutated.
  final String stream;

  /// Server name sent in the mutation request.
  final String removedServer;

  /// Strongest topology conclusion available before the deadline.
  final JetStreamPeerRemovalClassification classification;

  /// Exact mutation response category and underlying failure, if any.
  final JetStreamPeerRemovalMutationResult mutation;

  /// Topology used to validate the mutation.
  final JetStreamStreamInfo before;

  /// Last successful topology read, falling back to [before].
  final JetStreamStreamInfo lastInfo;

  /// Total reconciliation deadline requested by the caller.
  final Duration maximumWait;

  /// Last reconciliation read failure when no newer read succeeded.
  final Object? reconciliationCause;

  /// Stack trace associated with [reconciliationCause].
  final StackTrace? reconciliationCauseStackTrace;

  /// Whether NATS explicitly returned `success: true`.
  bool get serverReportedSuccess => mutation.serverReportedSuccess;

  /// Peer remap API rejection after a potentially partial mutation.
  JetStreamApiException? get mutationError => mutation.mutationError;

  /// Transport failure that made mutation acceptance ambiguous.
  Object? get ambiguityCause => mutation.ambiguityCause;

  /// Stack trace associated with [ambiguityCause].
  StackTrace? get ambiguityCauseStackTrace => mutation.ambiguityCauseStackTrace;
}
