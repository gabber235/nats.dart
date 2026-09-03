library;

import "dart:async";
import "dart:convert";
import "dart:typed_data";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/byte_snapshot.dart";
import "package:nats_jetstream/src/context.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/publish_headers.dart";
import "package:nats_jetstream/src/publish_options.dart";
import "package:nats_jetstream/src/wire_response.dart";

part "async_publish_models.dart";
part "async_publish_owner.dart";

/// Asynchronous publisher creation.
extension JetStreamAsyncPublisherCreation on JetStreamContext {
  /// Creates one bounded publisher owner.
  JetStreamAsyncPublisher createAsyncPublisher({
    JetStreamAsyncPublisherOptions? options,
  }) => JetStreamAsyncPublisher(
    connection,
    options ?? JetStreamAsyncPublisherOptions(),
  );
}
