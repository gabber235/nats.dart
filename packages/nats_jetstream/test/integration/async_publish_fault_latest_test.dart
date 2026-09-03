@TestOn("vm")
library;

import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

import "../support/jetstream_publish_fault_proxy.dart";

void main() {
  final serverUrl = Platform.environment["NATS_URL"];
  final skipReason = serverUrl == null ? "NATS_URL is not configured" : false;

  test(
    "deduplicated publish retries after stored ack is dropped and TCP resets",
    () async {
      final fixture = await _PublishFaultFixture.start(
        serverUrl!,
        "RESET",
        JetStreamPublishFault.resetAfterFirstAck,
      );
      addTearDown(fixture.close);
      await fixture.createStream();
      final publisher = fixture.createPublisher(
        JetStreamAsyncPublisherOptions(
          attemptTimeout: const Duration(seconds: 2),
          retryPolicy: JetStreamAsyncPublishRetryPolicy.deduplicated,
          retryDelay: Duration.zero,
        ),
      );

      final pending = publisher.publish(fixture.subject, const [
        0,
        255,
        13,
        10,
      ], options: JetStreamPublishOptions(messageId: "reset-message"));
      final acknowledgement = await pending.acknowledgement.timeout(
        const Duration(seconds: 5),
      );
      await publisher.drain();

      expect(fixture.publishConnection.generation, 2);
      expect(pending.attempts, 2);
      expect(acknowledgement.sequence, BigInt.one);
      expect(acknowledgement.duplicate, isTrue);
      await _expectStored(fixture, const [0, 255, 13, 10]);
      expect(fixture.proxy.downstreamConnections, 2);
      expect(fixture.proxy.targetPublishes, 2);
      expect(fixture.proxy.acknowledgementsObserved, 2);
      expect(fixture.proxy.acknowledgementsDropped, 1);
      expect(fixture.proxy.connectionResets, 1);
      expect(publisher.pendingMessages, 0);
      expect(publisher.pendingBytes, 0);
      expect(fixture.proxy.failure, isNull);
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 20)),
  );

  test(
    "stored deduplicated publish becomes ambiguous when every ack is dropped",
    () async {
      final fixture = await _PublishFaultFixture.start(
        serverUrl!,
        "DEADLINE",
        JetStreamPublishFault.dropAllAcks,
      );
      addTearDown(fixture.close);
      await fixture.createStream();
      final publisher = fixture.createPublisher(
        JetStreamAsyncPublisherOptions(
          acknowledgementTimeout: const Duration(milliseconds: 450),
          attemptTimeout: const Duration(milliseconds: 100),
          retryPolicy: JetStreamAsyncPublishRetryPolicy.deduplicated,
          maximumAttempts: 8,
          retryDelay: Duration.zero,
        ),
      );

      final pending = publisher.publish(fixture.subject, const [
        2,
        4,
        6,
      ], options: JetStreamPublishOptions(messageId: "deadline-message"));
      final failure = await _publishFailure(pending);
      final drainFailure = await _drainFailure(publisher);

      expect(failure.cause, isA<NatsTimeoutException>());
      expect(failure.disposition, JetStreamPublishDisposition.possiblyStored);
      expect(failure.messageId, "deadline-message");
      expect(failure.attempts, greaterThan(1));
      expect(failure.attempts, lessThanOrEqualTo(8));
      expect(fixture.proxy.targetPublishes, greaterThan(1));
      expect(
        fixture.proxy.targetPublishes,
        lessThanOrEqualTo(failure.attempts),
      );
      expect(
        failure.attempts - fixture.proxy.targetPublishes,
        lessThanOrEqualTo(1),
      );
      expect(fixture.proxy.targetMessageIds, everyElement("deadline-message"));
      expect(
        fixture.proxy.acknowledgementsObserved,
        fixture.proxy.targetPublishes,
      );
      expect(
        fixture.proxy.acknowledgementsDropped,
        fixture.proxy.targetPublishes,
      );
      expect(fixture.publishConnection.generation, 1);
      expect(fixture.publishConnection.isConnected, isTrue);
      await fixture.publishConnection.flush();
      await _expectStored(fixture, const [2, 4, 6]);
      expect(publisher.pendingMessages, 0);
      expect(publisher.pendingBytes, 0);
      expect(drainFailure.acknowledged, 0);
      expect(drainFailure.definitelyRejected, 0);
      expect(drainFailure.ambiguous, 1);
      expect(drainFailure.firstCause, isA<NatsTimeoutException>());
      expect(fixture.proxy.failure, isNull);
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 20)),
  );

  test(
    "duplicate in process retries with one ID and succeeds",
    () async {
      final fixture = await _PublishFaultFixture.start(
        serverUrl!,
        "CONFLICTSUCCESS",
        JetStreamPublishFault.duplicateInProcessFirstAck,
      );
      addTearDown(fixture.close);
      await fixture.createStream();
      final publisher = fixture.createPublisher(
        JetStreamAsyncPublisherOptions(
          acknowledgementTimeout: const Duration(seconds: 2),
          attemptTimeout: const Duration(seconds: 1),
          retryPolicy: JetStreamAsyncPublishRetryPolicy.deduplicated,
          maximumAttempts: 2,
          retryDelay: Duration.zero,
        ),
      );

      final pending = publisher.publish(fixture.subject, const [
        13,
        21,
        34,
      ], options: JetStreamPublishOptions(messageId: "conflict-success"));
      final acknowledgement = await pending.acknowledgement;
      await publisher.drain();

      expect(pending.attempts, 2);
      expect(acknowledgement.sequence, BigInt.one);
      expect(acknowledgement.duplicate, isTrue);
      expect(fixture.proxy.targetMessageIds, const [
        "conflict-success",
        "conflict-success",
      ]);
      expect(fixture.proxy.acknowledgementsObserved, 2);
      expect(fixture.proxy.acknowledgementsReplaced, 1);
      await _expectStored(fixture, const [13, 21, 34]);
      expect(fixture.proxy.failure, isNull);
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 20)),
  );

  test(
    "persistent duplicate in process remains ambiguous at attempt bound",
    () async {
      final fixture = await _PublishFaultFixture.start(
        serverUrl!,
        "CONFLICT",
        JetStreamPublishFault.duplicateInProcessEveryAck,
      );
      addTearDown(fixture.close);
      await fixture.createStream();
      final publisher = fixture.createPublisher(
        JetStreamAsyncPublisherOptions(
          acknowledgementTimeout: const Duration(seconds: 2),
          attemptTimeout: const Duration(seconds: 1),
          retryPolicy: JetStreamAsyncPublishRetryPolicy.deduplicated,
          maximumAttempts: 2,
          retryDelay: Duration.zero,
        ),
      );

      final pending = publisher.publish(fixture.subject, const [
        21,
        34,
        55,
      ], options: JetStreamPublishOptions(messageId: "conflict-message"));
      final failure = await _publishFailure(pending);
      final drainFailure = await _drainFailure(publisher);

      final apiError = failure.cause as JetStreamApiException;
      expect(apiError.code, 409);
      expect(apiError.errorCode, 10158);
      expect(failure.disposition, JetStreamPublishDisposition.possiblyStored);
      expect(failure.attempts, 2);
      expect(failure.messageId, "conflict-message");
      expect(fixture.proxy.targetPublishes, 2);
      expect(fixture.proxy.targetMessageIds, const [
        "conflict-message",
        "conflict-message",
      ]);
      expect(fixture.proxy.acknowledgementsObserved, 2);
      expect(fixture.proxy.acknowledgementsReplaced, 2);
      await _expectStored(fixture, const [21, 34, 55]);
      expect(drainFailure.acknowledged, 0);
      expect(drainFailure.definitelyRejected, 0);
      expect(drainFailure.ambiguous, 1);
      expect(drainFailure.firstCause, isA<JetStreamApiException>());
      expect(fixture.proxy.failure, isNull);
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 20)),
  );

  for (final retryPolicy in [
    JetStreamAsyncPublishRetryPolicy.none,
    JetStreamAsyncPublishRetryPolicy.noResponders,
  ]) {
    test(
      "duplicate in process does not broaden ${retryPolicy.name} policy",
      () async {
        final fixture = await _PublishFaultFixture.start(
          serverUrl!,
          "CONFLICT${retryPolicy.name.toUpperCase()}",
          JetStreamPublishFault.duplicateInProcessFirstAck,
        );
        addTearDown(fixture.close);
        await fixture.createStream();
        final publisher = fixture.createPublisher(
          JetStreamAsyncPublisherOptions(
            acknowledgementTimeout: const Duration(seconds: 2),
            attemptTimeout: const Duration(seconds: 1),
            retryPolicy: retryPolicy,
            maximumAttempts: 2,
            retryDelay: Duration.zero,
          ),
        );

        final messageId = "conflict-${retryPolicy.name}";
        final pending = publisher.publish(fixture.subject, const [
          89,
          144,
        ], options: JetStreamPublishOptions(messageId: messageId));
        final failure = await _publishFailure(pending);
        final drainFailure = await _drainFailure(publisher);

        final apiError = failure.cause as JetStreamApiException;
        expect(apiError.errorCode, 10158);
        expect(failure.disposition, JetStreamPublishDisposition.possiblyStored);
        expect(failure.attempts, 1);
        expect(fixture.proxy.targetMessageIds, [messageId]);
        expect(fixture.proxy.acknowledgementsObserved, 1);
        expect(fixture.proxy.acknowledgementsReplaced, 1);
        await _expectStored(fixture, const [89, 144]);
        expect(drainFailure.definitelyRejected, 0);
        expect(drainFailure.ambiguous, 1);
        expect(fixture.proxy.failure, isNull);
      },
      skip: skipReason,
      timeout: const Timeout(Duration(seconds: 20)),
    );
  }

  test(
    "malformed ack after storage is ambiguous without closing Core",
    () async {
      final fixture = await _PublishFaultFixture.start(
        serverUrl!,
        "MALFORMED",
        JetStreamPublishFault.malformedFirstAck,
      );
      addTearDown(fixture.close);
      await fixture.createStream();
      final publisher = fixture.createPublisher(
        JetStreamAsyncPublisherOptions(
          acknowledgementTimeout: const Duration(seconds: 2),
          attemptTimeout: const Duration(seconds: 2),
          retryPolicy: JetStreamAsyncPublishRetryPolicy.none,
          maximumAttempts: 1,
          retryDelay: Duration.zero,
        ),
      );

      final pending = publisher.publish(fixture.subject, const [3, 1, 4]);
      final failure = await _publishFailure(pending);
      final drainFailure = await _drainFailure(publisher);

      expect(failure.cause, isA<JetStreamProtocolException>());
      expect(
        (failure.cause as JetStreamProtocolException).message,
        "Invalid JSON response",
      );
      expect(failure.disposition, JetStreamPublishDisposition.possiblyStored);
      expect(failure.attempts, 1);
      expect(fixture.publishConnection.generation, 1);
      expect(fixture.publishConnection.isConnected, isTrue);
      await fixture.publishConnection.flush();
      await _expectStored(fixture, const [3, 1, 4]);
      expect(fixture.proxy.acknowledgementsObserved, 1);
      expect(fixture.proxy.acknowledgementsReplaced, 1);
      expect(publisher.pendingMessages, 0);
      expect(publisher.pendingBytes, 0);
      expect(drainFailure.definitelyRejected, 0);
      expect(drainFailure.ambiguous, 1);
      expect(drainFailure.firstCause, isA<JetStreamProtocolException>());
      expect(fixture.proxy.failure, isNull);
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 20)),
  );

  test(
    "no responders retry waits for a real stream without a fixed delay",
    () async {
      final fixture = await _PublishFaultFixture.start(
        serverUrl!,
        "RESPONDER",
        JetStreamPublishFault.holdRetryAfterNoResponders,
      );
      addTearDown(fixture.close);
      final publisher = fixture.createPublisher(
        JetStreamAsyncPublisherOptions(
          attemptTimeout: const Duration(seconds: 2),
          maximumAttempts: 2,
          retryDelay: Duration.zero,
        ),
      );

      final pending = publisher.publish(fixture.subject, const [5, 8, 13]);
      await fixture.proxy.firstNoRespondersForwarded.timeout(
        const Duration(seconds: 2),
      );
      await fixture.proxy.retryHeld.timeout(const Duration(seconds: 2));
      await fixture.createStream();
      await fixture.proxy.releaseHeldRetry();
      final acknowledgement = await pending.acknowledgement.timeout(
        const Duration(seconds: 2),
      );
      await publisher.drain();

      expect(pending.messageId, isNull);
      expect(pending.attempts, 2);
      expect(acknowledgement.sequence, BigInt.one);
      expect(acknowledgement.duplicate, isFalse);
      await _expectStored(fixture, const [5, 8, 13]);
      expect(fixture.proxy.noResponders, 1);
      expect(fixture.proxy.heldRetries, 1);
      expect(fixture.proxy.releasedRetries, 1);
      expect(fixture.proxy.targetPublishes, 2);
      expect(publisher.pendingMessages, 0);
      expect(publisher.pendingBytes, 0);
      expect(fixture.proxy.failure, isNull);
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 20)),
  );
}

Future<void> _expectStored(
  _PublishFaultFixture fixture,
  List<int> payload,
) async {
  final info = await fixture.management.streamInfo(fixture.stream);
  expect(info.messages, BigInt.one);
  expect(
    (await fixture.management.getMessage(fixture.stream, BigInt.one)).payload,
    payload,
  );
}

Future<JetStreamAsyncPublishException> _publishFailure(
  JetStreamPendingPublish pending,
) async {
  try {
    await pending.acknowledgement;
  } on JetStreamAsyncPublishException catch (error) {
    return error;
  }
  throw StateError("JetStream publication unexpectedly succeeded");
}

Future<JetStreamAsyncPublisherDrainException> _drainFailure(
  JetStreamAsyncPublisher publisher,
) async {
  try {
    await publisher.drain();
  } on JetStreamAsyncPublisherDrainException catch (error) {
    return error;
  }
  throw StateError("JetStream publisher drain unexpectedly succeeded");
}

final class _PublishFaultFixture {
  _PublishFaultFixture._({
    required this.stream,
    required this.subject,
    required this.managementConnection,
    required this.management,
    required this.proxy,
    required this.publishConnection,
  });

  final String stream;
  final String subject;
  final NatsConnection managementConnection;
  final JetStreamContext management;
  final JetStreamPublishFaultProxy proxy;
  final NatsConnection publishConnection;
  final List<JetStreamAsyncPublisher> _publishers = [];
  var _streamCreated = false;

  static Future<_PublishFaultFixture> start(
    String serverUrl,
    String label,
    JetStreamPublishFault fault,
  ) async {
    final suffix = DateTime.now().microsecondsSinceEpoch;
    final stream = "$label$suffix";
    final subject = "fault.${label.toLowerCase()}.$suffix";
    final upstream = Uri.parse(serverUrl);
    final managementConnection = await NatsConnection.connect(
      NatsOptions(
        servers: [NatsServer.parse(serverUrl)],
        name: "nats-dart-fault-management-$suffix",
      ),
    );
    JetStreamPublishFaultProxy? proxy;
    NatsConnection? publishConnection;
    try {
      proxy = await JetStreamPublishFaultProxy.start(
        upstream: upstream,
        publishSubject: subject,
        expectedStream: stream,
        fault: fault,
      );
      publishConnection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(proxy.clientUri.toString())],
          name: "nats-dart-fault-publisher-$suffix",
          reconnect: NatsReconnectPolicy(
            maximumAttempts: 10,
            baseDelay: const Duration(milliseconds: 20),
            maximumDelay: const Duration(milliseconds: 50),
            jitter: 0,
            bufferBytes: 0,
          ),
        ),
      );
      return _PublishFaultFixture._(
        stream: stream,
        subject: subject,
        managementConnection: managementConnection,
        management: JetStreamContext(managementConnection),
        proxy: proxy,
        publishConnection: publishConnection,
      );
    } on Object {
      await publishConnection?.close();
      await proxy?.close();
      await managementConnection.close();
      rethrow;
    }
  }

  JetStreamAsyncPublisher createPublisher(
    JetStreamAsyncPublisherOptions options,
  ) {
    final publisher = JetStreamContext(
      publishConnection,
    ).createAsyncPublisher(options: options);
    _publishers.add(publisher);
    return publisher;
  }

  Future<void> createStream() async {
    await management.createStream(
      JetStreamStreamConfig(
        name: stream,
        subjects: [subject],
        duplicateWindow: JetStreamNanoseconds.fromDuration(
          const Duration(minutes: 2),
        ),
      ),
    );
    _streamCreated = true;
  }

  Future<void> close() async {
    for (final publisher in _publishers) {
      try {
        await publisher.close().timeout(const Duration(seconds: 2));
      } on Object {
        // Tests assert expected publisher failures before teardown.
      }
    }
    await publishConnection.close();
    await proxy.close();
    if (_streamCreated) {
      try {
        await management.deleteStream(stream);
      } on JetStreamApiException {
        // A prior assertion may observe server cleanup first.
      }
    }
    await managementConnection.close();
  }
}
