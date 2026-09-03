import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/atomic_batch.dart";
import "package:nats_jetstream/src/atomic_batch_advisory.dart";
import "package:nats_jetstream/src/atomic_batch_publisher.dart";
import "package:nats_jetstream/src/consumer_management.dart";
import "package:nats_jetstream/src/direct_get.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/fast_batch.dart";
import "package:nats_jetstream/src/fast_batch_publisher.dart";
import "package:nats_jetstream/src/identifiers.dart";
import "package:nats_jetstream/src/kv_manager.dart";
import "package:nats_jetstream/src/management.dart";
import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/object_store_manager.dart";
import "package:nats_jetstream/src/publish_headers.dart";
import "package:nats_jetstream/src/publish_options.dart";
import "package:nats_jetstream/src/schedule.dart";
import "package:nats_jetstream/src/stream_cluster_models.dart";
import "package:nats_jetstream/src/stream_config.dart";
import "package:nats_jetstream/src/stream_manager.dart";
import "package:nats_jetstream/src/stream_models.dart";
import "package:nats_jetstream/src/stream_operations.dart";
import "package:nats_jetstream/src/stream_peer_models.dart";
import "package:nats_jetstream/src/stream_transfer_models.dart";
import "package:nats_jetstream/src/wire_integer.dart";
import "package:nats_jetstream/src/wire_response.dart";

/// Default decimal digit ceiling for one arbitrary precision counter value.
const jetStreamDefaultMaximumCounterDigits = 4096;

/// Typed JetStream management and acknowledged publication context.
final class JetStreamContext {
  /// Creates a validated context over one reconnecting Core connection.
  factory JetStreamContext(
    NatsConnection connection, {
    String apiPrefix = r"$JS.API",
    Duration timeout = const Duration(seconds: 5),
    int maximumCounterDigits = jetStreamDefaultMaximumCounterDigits,
  }) {
    validateJetStreamApiPrefix(apiPrefix);
    if (timeout <= Duration.zero) {
      throw ArgumentError.value(timeout, "timeout", "Must be positive");
    }
    if (maximumCounterDigits < 1) {
      throw ArgumentError.value(
        maximumCounterDigits,
        "maximumCounterDigits",
        "Must be positive",
      );
    }
    return JetStreamContext._(
      connection,
      apiPrefix,
      timeout,
      maximumCounterDigits,
    );
  }

  const JetStreamContext._(
    this.connection,
    this.apiPrefix,
    this.timeout,
    this.maximumCounterDigits,
  );

  /// Core connection used for every request.
  final NatsConnection connection;

  /// Account or domain specific management prefix.
  final String apiPrefix;

  /// Total deadline for one management request.
  final Duration timeout;

  /// CPU and allocation ceiling for one counter decimal value.
  ///
  /// The active connection payload ceiling may impose a smaller wire limit.
  final int maximumCounterDigits;

  /// Typed consumer management and delivery entry point.
  JetStreamConsumerManager get consumers =>
      JetStreamConsumerManager(connection, apiPrefix, timeout);

  /// Typed KV bucket management and operations.
  JetStreamKeyValueManager get keyValue =>
      JetStreamKeyValueManager(connection, apiPrefix, timeout);

  /// Typed Object Store management and operations.
  JetStreamObjectStoreManager get objectStore =>
      JetStreamObjectStoreManager(connection, apiPrefix, timeout);

  /// Complete stream management entry point.
  JetStreamStreamManager get streams =>
      JetStreamStreamManager(connection, apiPrefix, timeout);

  /// Direct replica reads for streams with direct access enabled.
  JetStreamDirectReader get direct =>
      JetStreamDirectReader(connection, apiPrefix, timeout);

  /// Sends exactly one validated stream leader stepdown mutation.
  ///
  /// Success confirms acceptance, not convergence. Ambiguous failures are never
  /// retried because NATS may already have applied the mutation.
  Future<JetStreamLeaderStepDownReceipt> requestStreamLeaderStepDown(
    String stream, {
    JetStreamLeaderStepDownSelection selection = const JetStreamAnyLeader(),
  }) => streams.requestLeaderStepDown(stream, selection: selection);

  /// Waits for an accepted stream leader stepdown to converge.
  ///
  /// Failure preserves the receipt and last observed topology in
  /// [JetStreamLeaderConvergenceException].
  Future<JetStreamStreamInfo> awaitStreamLeaderChange(
    JetStreamLeaderStepDownReceipt receipt, {
    Duration maximumWait = const Duration(seconds: 30),
    Duration pollInterval = const Duration(milliseconds: 100),
  }) => streams.awaitLeaderChange(
    receipt,
    maximumWait: maximumWait,
    pollInterval: pollInterval,
  );

  /// Requests one stream leader stepdown and waits for convergence.
  ///
  /// The mutation is sent once. Only safe topology reads repeat while waiting.
  Future<JetStreamStreamInfo> stepDownStreamLeader(
    String stream, {
    JetStreamLeaderStepDownSelection selection = const JetStreamAnyLeader(),
    Duration maximumWait = const Duration(seconds: 30),
    Duration pollInterval = const Duration(milliseconds: 100),
  }) => streams.stepDownLeader(
    stream,
    selection: selection,
    maximumWait: maximumWait,
    pollInterval: pollInterval,
  );

  /// Removes one named server and returns the reconciled stream topology.
  ///
  /// The mutation is sent exactly once. Ambiguous or partial outcomes retain
  /// their original cause in [JetStreamPeerRemovalResult].
  Future<JetStreamPeerRemovalResult> removeStreamPeer(
    String stream,
    String server, {
    JetStreamLeaderRemoval leaderRemoval = JetStreamLeaderRemoval.refuse,
    Duration maximumWait = const Duration(seconds: 30),
    Duration pollInterval = const Duration(milliseconds: 100),
  }) => streams.removePeer(
    stream,
    server,
    leaderRemoval: leaderRemoval,
    maximumWait: maximumWait,
    pollInterval: pollInterval,
  );

  /// Streams one bounded snapshot into an application sink.
  Future<JetStreamSnapshotResult> snapshotStreamTo(
    String stream,
    JetStreamSnapshotSink write, {
    required BigInt maximumBytes,
    JetStreamSnapshotOptions? options,
  }) => streams.snapshotTo(
    stream,
    write,
    maximumBytes: maximumBytes,
    options: options,
  );

  /// Restores one exact snapshot from a bounded source stream.
  Future<JetStreamStreamInfo> restoreStreamFrom(
    JetStreamSnapshotMetadata metadata,
    Stream<List<int>> source, {
    required BigInt maximumBytes,
    JetStreamRestoreOptions? options,
  }) => streams.restoreFrom(
    metadata,
    source,
    maximumBytes: maximumBytes,
    options: options,
  );

  /// Reads account resources and the supported API level.
  Future<JetStreamAccountInfo> accountInfo() => _management.accountInfo();

  /// Creates a stream and returns the effective server configuration.
  Future<JetStreamStreamInfo> createStream(JetStreamStreamConfig config) =>
      streams.create(config);

  /// Updates a stream from a complete configuration snapshot.
  Future<JetStreamStreamInfo> updateStream(JetStreamStreamConfig config) =>
      streams.update(config);

  /// Updates a stream, or creates it only when it is exactly absent.
  Future<JetStreamStreamInfo> createOrUpdateStream(
    JetStreamStreamConfig config,
  ) => streams.createOrUpdate(config);

  /// Reads one stream.
  Future<JetStreamStreamInfo> streamInfo(String name) => streams.info(name);

  /// Reads one exact stream information page.
  Future<JetStreamStreamPage> streamListPage({
    BigInt? offset,
    String? subject,
  }) => streams.listPage(offset: offset, subject: subject);

  /// Reads one exact stream name page.
  Future<JetStreamStreamNamePage> streamNamesPage({
    BigInt? offset,
    String? subject,
  }) => streams.namesPage(offset: offset, subject: subject);

  /// Iterates all matching stream information.
  Stream<JetStreamStreamInfo> listStreams({String? subject}) =>
      streams.list(subject: subject);

  /// Iterates all matching stream names.
  Stream<String> streamNames({String? subject}) =>
      streams.names(subject: subject);

  /// Finds the stream bound to a literal subject.
  Future<String> streamNameBySubject(String subject) =>
      streams.nameBySubject(subject);

  /// Retrieves one stored message by exact stream [sequence].
  Future<JetStreamStoredMessage> getMessage(String stream, BigInt sequence) =>
      streams.getMessage(stream, sequence);

  /// Retrieves the newest message matching one subject pattern.
  Future<JetStreamStoredMessage> getLastMessage(
    String stream,
    String subject,
  ) => streams.getLastMessage(stream, subject);

  /// Retrieves the first matching message at or after a sequence.
  Future<JetStreamStoredMessage> getNextMessage(
    String stream,
    String subject,
    BigInt startSequence,
  ) => streams.getNextMessage(stream, subject, startSequence);

  /// Purges matching stream messages.
  Future<JetStreamPurgeResult> purgeStream(
    String stream, {
    JetStreamPurgeOptions? options,
  }) => streams.purge(stream, options: options);

  /// Deletes one stored message.
  Future<void> deleteMessage(
    String stream,
    BigInt sequence, {
    bool secure = false,
  }) => streams.deleteMessage(stream, sequence, secure: secure);

  /// Deletes one stream and confirms the server response.
  Future<void> deleteStream(String name) => streams.delete(name);

  JetStreamManagement get _management =>
      JetStreamManagement(connection, apiPrefix, timeout);

  /// Publishes one message and waits for its JetStream acknowledgement.
  Future<JetStreamPublishAck> publish(
    String subject,
    List<int> payload, {
    NatsHeaders? headers,
    JetStreamPublishOptions? options,
  }) async {
    if (headers?.first("Nats-Incr") != null) {
      throw ArgumentError(
        "Nats-Incr requires the typed incrementCounter operation",
      );
    }
    final effectiveHeaders = applyJetStreamPublishOptions(headers, options);
    final response = await _publishRequest(subject, payload, effectiveHeaders);
    return _publicPublishAck(
      _decodePublishAck(
        response,
        expectedStream: effectiveHeaders?.first("Nats-Expected-Stream"),
      ),
    );
  }

  /// Atomically increments one distributed counter and returns its new value.
  Future<JetStreamCounterAck> incrementCounter(
    String subject,
    BigInt increment, {
    NatsHeaders? headers,
  }) async {
    final magnitude = increment.abs().toString();
    final maximumDigits = _effectiveMaximumCounterDigits;
    if (magnitude.length > maximumDigits) {
      throw ArgumentError.value(
        increment,
        "increment",
        "Must contain at most $maximumDigits decimal digits",
      );
    }
    var effectiveHeaders = setJetStreamManagedHeader(
      headers,
      "Nats-Incr",
      increment.isNegative ? "-$magnitude" : "+$magnitude",
    );
    effectiveHeaders = setJetStreamManagedHeader(
      effectiveHeaders,
      "Nats-Required-Api-Level",
      "2",
    );
    final response = await _publishRequest(subject, const [], effectiveHeaders);
    final wire = _decodePublishAck(
      response,
      requireCounter: true,
      expectedStream: effectiveHeaders.first("Nats-Expected-Stream"),
      maximumCounterDigits: maximumDigits,
    );
    return JetStreamCounterAck(
      stream: wire.stream,
      sequence: wire.sequence,
      value: wire.counterValue!,
      domain: wire.domain,
    );
  }

  /// Atomically commits between one and one thousand messages.
  Future<JetStreamAtomicBatchAck> publishAtomicBatch(
    Iterable<JetStreamAtomicMessage> messages, {
    String? batchId,
  }) {
    return JetStreamAtomicBatchPublisher(
      connection,
      timeout,
    ).publish(messages, batchId: batchId);
  }

  /// Observes abandonment while publishing one exact atomic batch.
  ///
  /// A matching advisory throws [JetStreamAtomicBatchAbandonedException] and
  /// proves that no message committed. A lost acknowledgement without a
  /// matching advisory throws [JetStreamAtomicBatchAmbiguousException].
  Future<JetStreamAtomicBatchAck> publishAtomicBatchConfirmed(
    String stream,
    Iterable<JetStreamAtomicMessage> messages, {
    String? batchId,
    Duration confirmationTimeout = const Duration(seconds: 12),
  }) {
    return JetStreamAtomicBatchPublisher(connection, timeout).publishConfirmed(
      stream,
      messages,
      batchId: batchId,
      confirmationTimeout: confirmationTimeout,
    );
  }

  /// Opens a global or stream scoped atomic batch abandonment watcher.
  Future<JetStreamAtomicBatchAdvisoryWatcher> watchAtomicBatchAbandonments({
    String? stream,
  }) {
    return JetStreamAtomicBatchAdvisoryWatcher.connect(
      connection,
      stream: stream,
      timeout: timeout,
    );
  }

  /// Publishes a non atomic stream through the NATS 2.14 fast ingest protocol.
  Future<JetStreamFastBatchAck> publishFastBatch(
    Stream<JetStreamFastBatchMessage> messages, {
    JetStreamFastBatchOptions? options,
  }) {
    return JetStreamFastBatchPublisher(
      connection,
    ).publish(messages, options: options);
  }

  /// Convenience wrapper for an in memory fast ingest batch.
  Future<JetStreamFastBatchAck> publishFastBatchMessages(
    Iterable<JetStreamFastBatchMessage> messages, {
    JetStreamFastBatchOptions? options,
  }) {
    return publishFastBatch(Stream.fromIterable(messages), options: options);
  }

  /// Persists one schedule definition and waits for its acknowledgement.
  Future<JetStreamPublishAck> scheduleMessage(
    String scheduleSubject,
    List<int> payload, {
    required JetStreamScheduleOptions options,
    NatsHeaders? headers,
  }) async {
    validatePublishSubject(scheduleSubject);
    if (scheduleSubject == options.target ||
        scheduleSubject == options.source) {
      throw ArgumentError.value(
        scheduleSubject,
        "scheduleSubject",
        "Must differ from the target and source",
      );
    }
    var effectiveHeaders = setJetStreamManagedHeader(
      headers,
      "Nats-Schedule",
      options.schedule.pattern,
    );
    effectiveHeaders = setJetStreamManagedHeader(
      effectiveHeaders,
      "Nats-Schedule-Target",
      options.target,
    );
    if (options.source case final source?) {
      effectiveHeaders = setJetStreamManagedHeader(
        effectiveHeaders,
        "Nats-Schedule-Source",
        source,
      );
    }
    if (options.generatedMessageTtl case final ttl?) {
      effectiveHeaders = setJetStreamManagedHeader(
        effectiveHeaders,
        "Nats-Schedule-TTL",
        ttl.wireValue,
      );
    }
    if (options.schedule.timeZone case final timeZone?) {
      effectiveHeaders = setJetStreamManagedHeader(
        effectiveHeaders,
        "Nats-Schedule-Time-Zone",
        timeZone,
      );
    }
    if (options.rollupGeneratedSubject) {
      effectiveHeaders = setJetStreamManagedHeader(
        effectiveHeaders,
        "Nats-Schedule-Rollup",
        "sub",
      );
    }
    effectiveHeaders = setJetStreamManagedHeader(
      effectiveHeaders,
      "Nats-Required-Api-Level",
      "${options.minimumApiLevel}",
    );
    final response = await _publishRequest(
      scheduleSubject,
      payload,
      effectiveHeaders,
    );
    return _publicPublishAck(
      _decodePublishAck(
        response,
        expectedStream: effectiveHeaders.first("Nats-Expected-Stream"),
      ),
    );
  }

  /// Atomically cancels a schedule while persisting a separate message.
  Future<JetStreamPublishAck> cancelSchedule(
    String publishSubject,
    String scheduleSubject, {
    required BigInt expectedScheduleSequence,
    List<int> payload = const [],
    NatsHeaders? headers,
  }) async {
    validatePublishSubject(publishSubject);
    validatePublishSubject(scheduleSubject);
    if (publishSubject == scheduleSubject) {
      throw ArgumentError.value(
        publishSubject,
        "publishSubject",
        "Must differ from the schedule subject",
      );
    }
    validatePositiveJetStreamUint64(
      expectedScheduleSequence,
      "expectedScheduleSequence",
    );
    var effectiveHeaders = setJetStreamManagedHeader(
      headers,
      "Nats-Schedule-Next",
      "purge",
    );
    effectiveHeaders = setJetStreamManagedHeader(
      effectiveHeaders,
      "Nats-Scheduler",
      scheduleSubject,
    );
    effectiveHeaders = setJetStreamManagedHeader(
      effectiveHeaders,
      "Nats-Expected-Last-Subject-Sequence",
      "$expectedScheduleSequence",
    );
    effectiveHeaders = setJetStreamManagedHeader(
      effectiveHeaders,
      "Nats-Expected-Last-Subject-Sequence-Subject",
      scheduleSubject,
    );
    effectiveHeaders = setJetStreamManagedHeader(
      effectiveHeaders,
      "Nats-Required-Api-Level",
      "4",
    );
    final response = await _publishRequest(
      publishSubject,
      payload,
      effectiveHeaders,
    );
    return _publicPublishAck(
      _decodePublishAck(
        response,
        expectedStream: effectiveHeaders.first("Nats-Expected-Stream"),
      ),
    );
  }

  Future<JetStreamWireResponse> _publishRequest(
    String subject,
    List<int> payload,
    NatsHeaders? headers,
  ) async {
    final message = await connection.request(
      subject,
      payload,
      headers: headers,
      timeout: timeout,
    );
    final response = JetStreamWireResponse.decode(message.payload, subject)
      ..throwApiError();
    return response;
  }

  int get _effectiveMaximumCounterDigits {
    final maximumPayloadBytes = connection.serverInfo.maximumPayloadBytes;
    return maximumCounterDigits < maximumPayloadBytes
        ? maximumCounterDigits
        : maximumPayloadBytes;
  }
}

_WirePublishAck _decodePublishAck(
  JetStreamWireResponse response, {
  bool requireCounter = false,
  String? expectedStream,
  int maximumCounterDigits = jetStreamDefaultMaximumCounterDigits,
}) {
  final stream = response.requiredString("stream");
  if (expectedStream != null && stream != expectedStream) {
    throw JetStreamProtocolException(
      "Publish acknowledgement has the wrong stream identity",
      subject: response.subject,
      fieldPath: "stream",
    );
  }
  final counter = requireCounter
      ? response.requiredDecimal("val", maximumDigits: maximumCounterDigits)
      : null;
  return _WirePublishAck(
    stream: stream,
    sequence: response.requiredPositiveUint64("seq"),
    duplicate: response.optionalBoolean("duplicate"),
    domain: response.optionalString("domain"),
    counterValue: counter,
  );
}

JetStreamPublishAck _publicPublishAck(_WirePublishAck wire) {
  return JetStreamPublishAck(
    stream: wire.stream,
    sequence: wire.sequence,
    duplicate: wire.duplicate,
    domain: wire.domain,
  );
}

final class _WirePublishAck {
  const _WirePublishAck({
    required this.stream,
    required this.sequence,
    required this.duplicate,
    required this.domain,
    required this.counterValue,
  });

  final String stream;
  final BigInt sequence;
  final bool duplicate;
  final String? domain;
  final BigInt? counterValue;
}
