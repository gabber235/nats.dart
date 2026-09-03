import "package:nats_jetstream/src/kv_bucket.dart";
import "package:nats_jetstream/src/kv_watch.dart";
import "package:nats_jetstream/src/kv_watch_session.dart";

/// Continuous watch creation for one verified KV bucket.
extension JetStreamKeyValueWatching on JetStreamKeyValue {
  /// Creates a lazy watch. Server state is opened on first event subscription.
  JetStreamKeyValueWatch watch({JetStreamKeyValueWatchOptions? options}) {
    return JetStreamKeyValueWatchSession(
      connection,
      apiPrefix,
      timeout,
      name,
      backingStreamName,
      readSubjectPrefix,
      codecs,
      options ?? JetStreamKeyValueWatchOptions(),
    );
  }
}
