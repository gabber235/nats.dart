@TestOn("vm")
library;

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

import "support/scripted_nats_peer.dart";

void main() {
  for (final scenario in [
    _PreflightScenario(
      name: "unsupported headers",
      headersSupported: false,
      maximumPayloadBytes: 1024,
      payload: const [1],
      expectedCause: isA<NatsHeaderException>(),
    ),
    _PreflightScenario(
      name: "oversized payload",
      headersSupported: true,
      maximumPayloadBytes: 4,
      payload: const [1, 2, 3, 4, 5],
      expectedCause: isA<NatsMaxPayloadException>(),
    ),
  ]) {
    test(
      "${scenario.name} is definitely rejected before publication",
      () async {
        final peer = await ScriptedNatsPeer.start(
          headers: scenario.headersSupported,
          maximumPayloadBytes: scenario.maximumPayloadBytes,
        );
        final connection = await NatsConnection.connect(
          NatsOptions(
            servers: [NatsServer.parse(peer.uri.toString())],
            reconnect: NatsReconnectPolicy(maximumAttempts: 0),
            pingInterval: const Duration(minutes: 10),
          ),
        );
        addTearDown(() async {
          await connection.close();
          await peer.close();
        });
        final publisher = JetStreamContext(connection).createAsyncPublisher(
          options: JetStreamAsyncPublisherOptions(
            retryPolicy: JetStreamAsyncPublishRetryPolicy.deduplicated,
          ),
        );

        final pending = publisher.publish(
          "events.created",
          scenario.payload,
          options: JetStreamPublishOptions(messageId: "preflight-id"),
        );
        final failure = await _publishFailure(
          pending,
        ).timeout(const Duration(seconds: 2));
        final drainFailure = await _drainFailure(
          publisher,
        ).timeout(const Duration(seconds: 2));

        expect(failure.cause, scenario.expectedCause);
        expect(
          failure.disposition,
          JetStreamPublishDisposition.definitelyNotStored,
        );
        expect(failure.attempts, 1);
        expect(peer.publications, isEmpty);
        expect(connection.statistics.activeRequests, 0);
        expect(connection.statistics.messagesPublished, 0);
        expect(drainFailure.acknowledged, 0);
        expect(drainFailure.definitelyRejected, 1);
        expect(drainFailure.ambiguous, 0);
        expect(drainFailure.firstCause, scenario.expectedCause);
      },
    );
  }
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

final class _PreflightScenario {
  const _PreflightScenario({
    required this.name,
    required this.headersSupported,
    required this.maximumPayloadBytes,
    required this.payload,
    required this.expectedCause,
  });

  final String name;
  final bool headersSupported;
  final int maximumPayloadBytes;
  final List<int> payload;
  final Matcher expectedCause;
}
