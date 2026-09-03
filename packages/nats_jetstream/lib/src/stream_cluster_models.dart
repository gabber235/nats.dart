import "package:nats_jetstream/src/stream_models.dart";

/// Typed target for one stream leader stepdown request.
sealed class JetStreamLeaderStepDownSelection {
  const JetStreamLeaderStepDownSelection();

  /// Exact management request body for this selection.
  Map<String, Object?> toJson();
}

/// Lets the server choose any eligible stream replica.
final class JetStreamAnyLeader extends JetStreamLeaderStepDownSelection {
  /// Creates an unconstrained leader selection.
  const JetStreamAnyLeader();

  @override
  Map<String, Object?> toJson() => const {};
}

/// Requests one exact stream replica as the next leader.
final class JetStreamPreferredLeader extends JetStreamLeaderStepDownSelection {
  /// Creates a preferred server selection.
  factory JetStreamPreferredLeader(String server) {
    _validateNonEmpty(server, "server");
    return JetStreamPreferredLeader._(server);
  }

  const JetStreamPreferredLeader._(this.server);

  /// Preferred NATS server name.
  final String server;

  @override
  Map<String, Object?> toJson() => {
    "placement": {"preferred": server},
  };
}

/// Constrains the next leader by cluster and server tags.
final class JetStreamLeaderPlacement extends JetStreamLeaderStepDownSelection {
  /// Creates a placement constraint with at least one selector.
  factory JetStreamLeaderPlacement({
    String? cluster,
    List<String> tags = const [],
  }) {
    if (cluster == null && tags.isEmpty) {
      throw ArgumentError("Cluster or tags must select a placement");
    }
    if (cluster != null) _validateNonEmpty(cluster, "cluster");
    for (final tag in tags) {
      _validateNonEmpty(tag, "tags");
    }
    return JetStreamLeaderPlacement._(
      cluster: cluster,
      tags: List.unmodifiable(tags),
    );
  }

  const JetStreamLeaderPlacement._({required this.cluster, required this.tags});

  /// Required NATS cluster name when constrained.
  final String? cluster;

  /// Required server tags.
  final List<String> tags;

  @override
  Map<String, Object?> toJson() => {
    "placement": {"cluster": ?cluster, if (tags.isNotEmpty) "tags": tags},
  };
}

/// Server acceptance of exactly one leader stepdown mutation.
final class JetStreamLeaderStepDownReceipt {
  /// Creates an immutable accepted mutation receipt.
  const JetStreamLeaderStepDownReceipt({
    required this.stream,
    required this.previousLeader,
    required this.previousLeaderSince,
    required this.selection,
  });

  /// Stream whose leader accepted the stepdown request.
  final String stream;

  /// Leader observed immediately before the mutation.
  final String previousLeader;

  /// Election timestamp observed before the mutation when supplied by NATS.
  final String? previousLeaderSince;

  /// Target selection sent with the mutation.
  final JetStreamLeaderStepDownSelection selection;
}

/// Accepted stepdown that did not converge within the caller deadline.
final class JetStreamLeaderConvergenceException implements Exception {
  /// Creates a diagnostic convergence failure.
  const JetStreamLeaderConvergenceException({
    required this.receipt,
    required this.maximumWait,
    required this.lastInfo,
    this.cause,
    this.causeStackTrace,
  });

  /// Receipt proving the mutation was accepted.
  final JetStreamLeaderStepDownReceipt receipt;

  /// Total convergence deadline requested by the caller.
  final Duration maximumWait;

  /// Last topology read that completed successfully.
  final JetStreamStreamInfo? lastInfo;

  /// Last read failure when no newer topology was available.
  final Object? cause;

  /// Stack trace for [cause].
  final StackTrace? causeStackTrace;

  @override
  String toString() {
    final observed = lastInfo?.cluster?.leader ?? "unknown";
    return "JetStream leader stepdown for ${receipt.stream} was accepted, "
        "but leader convergence exceeded $maximumWait. "
        "Previous leader: ${receipt.previousLeader}. "
        "Last observed leader: $observed";
  }
}

void _validateNonEmpty(String value, String name) {
  if (value.isEmpty || value.trim() != value) {
    throw ArgumentError.value(value, name, "Must be nonempty and trimmed");
  }
}
