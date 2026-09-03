@TestOn("vm")
library;

import "dart:async";
import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:test/test.dart";

import "../support/nats_monitor_client.dart";

void main() {
  final serverUrl = Platform.environment["NATS_URL"];
  final monitorUrl = Platform.environment["NATS_MONITOR_URL"];
  final skipReason = serverUrl == null ? "NATS_URL is not configured" : false;

  test(
    "connection drain preserves accepted delivery, replies, and publications",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final clientName = "nats.dart.drain.$suffix";
      final server = NatsServer.parse(serverUrl!);
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [server], name: clientName),
      );
      final peer = await NatsConnection.connect(
        NatsOptions(servers: [server], name: "nats.dart.drain.peer.$suffix"),
      );
      addTearDown(() async {
        await connection.close();
        await peer.close();
      });

      final events = <NatsConnectionEvent>[];
      final closedEvent = Completer<void>();
      final eventListener = connection.events.listen((event) {
        events.add(event);
        if (event is NatsClosed && !closedEvent.isCompleted) {
          closedEvent.complete();
        }
      });
      final received = <int>[];
      var deliveryStreamClosed = false;
      final delivery = await connection.subscribe("drain.$suffix.delivery");
      final deliveryListener = delivery.messages.listen(
        (message) => received.add(message.payload.single),
        onDone: () => deliveryStreamClosed = true,
      )..pause();

      final replyGate = Completer<void>();
      final requestSeen = Completer<void>();
      final responder = await peer.subscribe("drain.$suffix.service");
      final responderListener = responder.messages.listen((request) async {
        if (!requestSeen.isCompleted) {
          requestSeen.complete();
        }
        await replyGate.future;
        await peer.reply(request, const [9, 8, 7]);
      });
      final outbound = await peer.subscribe("drain.$suffix.outbound");
      final outboundDelivery = outbound.messages.first;

      await connection.publish("drain.$suffix.delivery", const [1]);
      await connection.publish("drain.$suffix.delivery", const [2]);
      await connection.flush();
      final pendingReply = connection.request("drain.$suffix.service", const [
        7,
        8,
        9,
      ]);
      await requestSeen.future.timeout(const Duration(seconds: 2));

      final drain = connection.drain();
      expect(connection.isDraining, isTrue);
      await expectLater(
        connection.subscribe("drain.$suffix.rejected"),
        throwsA(isA<NatsDrainingException>()),
      );
      await expectLater(
        connection.request("drain.$suffix.rejected", const []),
        throwsA(isA<NatsDrainingException>()),
      );

      await connection.publish("drain.$suffix.outbound", const [3]);
      await connection.flush();
      expect(
        (await outboundDelivery.timeout(const Duration(seconds: 2))).payload,
        [3],
      );

      deliveryListener.resume();
      await delivery.done;
      replyGate.complete();
      expect((await pendingReply.timeout(const Duration(seconds: 2))).payload, [
        9,
        8,
        7,
      ]);
      await drain;
      await closedEvent.future;

      expect(received, [1, 2]);
      expect(deliveryStreamClosed, isTrue);
      expect(connection.isConnected, isFalse);
      expect(connection.isDraining, isFalse);
      expect(events.whereType<NatsDraining>(), hasLength(1));
      expect(events.whereType<NatsClosed>(), hasLength(1));
      expect(events.whereType<NatsClosed>().single.error, isNull);
      await connection.drain();
      await connection.close();

      await deliveryListener.cancel();
      await responder.unsubscribe();
      await responderListener.cancel();
      await outbound.unsubscribe();
      await eventListener.cancel();
      if (monitorUrl != null) {
        await NatsMonitorClient(
          Uri.parse(monitorUrl),
        ).waitUntilClientGone(clientName);
      }
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "connection drain timeout force closes every retained subscription",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final clientName = "nats.dart.drain.timeout.$suffix";
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(serverUrl!)], name: clientName),
      );
      addTearDown(connection.close);

      final events = <NatsConnectionEvent>[];
      final closedEvent = Completer<void>();
      final eventListener = connection.events.listen((event) {
        events.add(event);
        if (event is NatsClosed && !closedEvent.isCompleted) {
          closedEvent.complete();
        }
      });
      final errors = <Object>[];
      final streamClosed = Completer<void>();
      final subscription = await connection.subscribe("drain.$suffix.timeout");
      final listener = subscription.messages.listen(
        (_) {},
        onError: errors.add,
        onDone: streamClosed.complete,
      )..pause();
      final doneExpectation = expectLater(
        subscription.done,
        throwsA(isA<NatsDrainTimeoutException>()),
      );

      await connection.publish("drain.$suffix.timeout", const [1]);
      await connection.flush();
      final drain = connection.drain(
        timeout: const Duration(milliseconds: 200),
      );
      await expectLater(drain, throwsA(isA<NatsDrainTimeoutException>()));
      await connection.closed;
      await closedEvent.future;

      expect(connection.isConnected, isFalse);
      expect(connection.isDraining, isFalse);
      listener.resume();
      await doneExpectation;
      await streamClosed.future;
      expect(errors, [isA<NatsDrainTimeoutException>()]);
      expect(events.whereType<NatsDraining>(), hasLength(1));
      expect(events.whereType<NatsClosed>(), hasLength(1));
      expect(
        events.whereType<NatsClosed>().single.error,
        isA<NatsDrainTimeoutException>(),
      );
      await expectLater(drain, throwsA(isA<NatsDrainTimeoutException>()));
      await connection.close();

      await listener.cancel();
      await eventListener.cancel();
      if (monitorUrl != null) {
        await NatsMonitorClient(
          Uri.parse(monitorUrl),
        ).waitUntilClientGone(clientName);
      }
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}
