@TestOn("vm")
library;

import "dart:async";
import "dart:io";
import "dart:typed_data";

import "package:nats_core/nats_core.dart";
import "package:test/test.dart";

import "../support/integration_capabilities.dart";
import "../support/nats_monitor_client.dart";

void main() {
  final serverUrl = Platform.environment["NATS_URL"];
  final monitorUrl = Platform.environment["NATS_MONITOR_URL"];
  final skipReason = serverUrl == null ? "NATS_URL is not configured" : false;
  final expectedServerAuthority = serverUrl == null
      ? null
      : Uri.parse(serverUrl).authority;
  final messageTracingSupported = serverUrl == null
      ? false
      : requiredIntegrationCapability(
          Platform.environment,
          "NATS_EXPECTS_MESSAGE_TRACING",
        );
  final messageTracingSkipReason = skipReason != false
      ? skipReason
      : messageTracingSupported
      ? false
      : "The compatibility server does not support message tracing";

  test(
    "server observes publish, headers, requests, unsubscribe, and close",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final clientName = "nats-dart-통합-$suffix";
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(serverUrl!)], name: clientName),
      );
      final subject = "integration.$suffix.events";
      final subscription = await connection.subscribe(subject);

      await expectLater(
        connection.publish(subject, const [-1]),
        throwsFormatException,
      );
      await expectLater(
        connection.publish(subject, const [256]),
        throwsFormatException,
      );

      final received = subscription.messages.first;
      final payload = [0, 255, 13, 10, 7];
      await connection.publish(
        subject,
        payload,
        headers: NatsHeaders(
          entries: const [
            MapEntry("Traceparent", "first"),
            MapEntry("traceparent", "second"),
          ],
        ),
      );
      payload[0] = 99;
      await connection.flush();

      final message = await received.timeout(const Duration(seconds: 2));
      expect(message.payload, [0, 255, 13, 10, 7]);
      expect(message.headers?.values("TRACEPARENT"), ["first", "second"]);

      final unicodeSubject = "integration.$suffix.상태.변경";
      final unicodeSubscription = await connection.subscribe(
        unicodeSubject,
        queue: "작업자",
      );
      final unicodeDelivery = unicodeSubscription.messages.first;
      await connection.publish(unicodeSubject, const [4, 5, 6]);
      await connection.flush();
      final unicodeMessage = await unicodeDelivery.timeout(
        const Duration(seconds: 2),
      );
      expect(unicodeMessage.subject, unicodeSubject);
      expect(unicodeMessage.payload, [4, 5, 6]);

      final automaticSubject = "integration.$suffix.automatic";
      final automatic = await connection.subscribe(automaticSubject);
      final automaticMessages = automatic.messages
          .map((message) => message.payload.single)
          .toList();
      await automatic.autoUnsubscribe(2);
      await connection.publish(automaticSubject, const [1]);
      await connection.publish(automaticSubject, const [2]);
      await connection.publish(automaticSubject, const [3]);
      await connection.flush();
      expect(await automaticMessages.timeout(const Duration(seconds: 2)), [
        1,
        2,
      ]);
      await automatic.done;

      final serviceSubject = "integration.$suffix.service";
      final responder = await connection.subscribe(serviceSubject);
      final responderListener = responder.messages.listen((request) async {
        await connection.reply(
          request,
          request.payload.reversed.toList(),
          headers: NatsHeaders(
            entries: const [MapEntry("X-Responder", "nats.dart")],
          ),
        );
      });

      final replies = await Future.wait([
        for (var index = 0; index < 20; index += 1)
          connection.request(serviceSubject, [index, index + 1]),
      ]);
      for (var index = 0; index < replies.length; index += 1) {
        expect(replies[index].payload, [index + 1, index]);
        expect(replies[index].headers?.values("X-Responder"), ["nats.dart"]);
      }
      final publicationsBeforeMissingReply =
          connection.statistics.messagesPublished;
      expect(
        () => connection.reply(
          NatsMessage(subject: serviceSubject, payload: const []),
          const [],
        ),
        throwsA(isA<NatsMissingReplySubjectException>()),
      );
      expect(
        connection.statistics.messagesPublished,
        publicationsBeforeMissingReply,
      );

      final blackholeSubject = "integration.$suffix.blackhole";
      final blackhole = await connection.subscribe(blackholeSubject);
      await Future.wait([
        for (var index = 0; index < 10; index += 1)
          expectLater(
            connection.request(blackholeSubject, [
              index,
            ], timeout: const Duration(milliseconds: 50)),
            throwsA(isA<NatsTimeoutException>()),
          ),
      ]);
      final replyAfterTimeouts = await connection.request(
        serviceSubject,
        const [1, 2, 3],
      );
      expect(replyAfterTimeouts.payload, [3, 2, 1]);

      await expectLater(
        connection.request("integration.$suffix.no.responders", const []),
        throwsA(
          isA<NatsNoRespondersException>().having(
            (error) => error.subject,
            "subject",
            "integration.$suffix.no.responders",
          ),
        ),
      );

      await subscription.unsubscribe();
      await unicodeSubscription.unsubscribe();
      await responder.unsubscribe();
      await blackhole.unsubscribe();
      await responderListener.cancel();
      await connection.flush();
      await connection.close();
      expect(connection.isConnected, isFalse);
      await connection.closed;

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
    "slow subscription fails without poisoning the connection",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(serverUrl!)],
          name: "nats-dart-slow-consumer-$suffix",
          subscriptionQueueMessages: 1,
          subscriptionQueueBytes: 1024,
        ),
      );
      final subscription = await connection.subscribe(
        "integration.$suffix.slow",
      );

      await connection.publish("integration.$suffix.slow", const [1]);
      await connection.publish("integration.$suffix.slow", const [2]);
      await connection.flush();

      await expectLater(
        subscription.messages,
        emitsError(isA<NatsSlowConsumerException>()),
      );
      expect(connection.statistics.slowConsumers, 1);
      expect(connection.statistics.droppedMessages, 2);
      await connection.flush();
      await connection.close();
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "client ceiling caps publication and permits explicitly larger delivery",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final server = NatsServer.parse(serverUrl!);
      final bounded = await NatsConnection.connect(
        NatsOptions(servers: [server]),
      );
      final aboveDefault = Uint8List(8 * 1024 * 1024 + 1);

      await expectLater(
        bounded.publish("integration.$suffix.bounded", aboveDefault),
        throwsA(
          isA<NatsMaxPayloadException>()
              .having((error) => error.actual, "actual", aboveDefault.length)
              .having((error) => error.maximum, "maximum", 8 * 1024 * 1024),
        ),
      );
      await bounded.close();

      const largerBytes = 9 * 1024 * 1024;
      final larger = await NatsConnection.connect(
        NatsOptions(
          servers: [server],
          maximumInboundPayloadBytes: 10 * 1024 * 1024,
          subscriptionQueueBytes: 10 * 1024 * 1024,
        ),
      );
      final subscription = await larger.subscribe("integration.$suffix.larger");
      final delivery = subscription.messages.first;
      final payload = Uint8List(largerBytes)
        ..first = 1
        ..last = 2;

      await larger.publish("integration.$suffix.larger", payload);
      await larger.flush();
      final message = await delivery.timeout(const Duration(seconds: 10));

      expect(message.payload, hasLength(largerBytes));
      expect(message.payload.first, 1);
      expect(message.payload.last, 2);
      await subscription.unsubscribe();
      await larger.close();
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "multiple replies, sentinel, no responders, RTT, and statistics",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(serverUrl!)]),
      );
      final serviceSubject = "integration.$suffix.many";
      final responder = await connection.subscribe(serviceSubject);
      final responderListener = responder.messages.listen((request) async {
        for (var value = 1; value <= 3; value += 1) {
          await connection.publish(request.replySubject!, [value]);
        }
        await connection.publish(request.replySubject!, const []);
      });
      final before = connection.statistics;

      final request = await connection.requestMany(
        serviceSubject,
        const [9],
        options: NatsRequestManyOptions(
          timeout: const Duration(seconds: 5),
          stallTimeout: const Duration(seconds: 1),
          completeOnEmptyPayload: true,
          maximumBufferedMessages: 4,
          maximumBufferedBytes: 1024,
        ),
      );
      final replies = await request.messages.toList();

      expect(replies.map((message) => message.payload.single), [1, 2, 3]);
      final missing = await connection.requestMany(
        "integration.$suffix.missing",
        const [],
      );
      await expectLater(
        missing.messages,
        emitsError(isA<NatsNoRespondersException>()),
      );
      final latency = await connection.rtt();
      final after = connection.statistics;

      expect(latency, isNot(Duration.zero));
      expect(after.generation, 1);
      expect(after.activeRequests, 0);
      expect(after.messagesPublished, greaterThan(before.messagesPublished));
      expect(after.messagesReceived, greaterThan(before.messagesReceived));
      expect(after.payloadBytesPublished, greaterThan(0));
      expect(after.payloadBytesReceived, greaterThan(0));
      expect(after.serverAuthority, expectedServerAuthority);
      expect(before.messagesPublished, 0);

      await responderListener.cancel();
      await responder.unsubscribe();
      final blackhole = await connection.subscribe(
        "integration.$suffix.blackhole.many",
      );
      final blackholeListener = blackhole.messages.listen((_) {});
      final pending = await connection.requestMany(
        "integration.$suffix.blackhole.many",
        const [],
        options: NatsRequestManyOptions(timeout: const Duration(seconds: 20)),
      );
      final pendingReplies = pending.messages.toList();
      expect(connection.statistics.activeRequests, 1);

      await connection.drain();

      expect(await pendingReplies, isEmpty);
      await blackhole.done;
      await blackholeListener.cancel();
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "typed trace options cover publish and request surfaces",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final connection = await NatsConnection.connect(
        NatsOptions(servers: [NatsServer.parse(serverUrl!)]),
      );
      final info = connection.serverInfo;
      expect(info.serverAuthority, expectedServerAuthority);
      expect(info.headersSupported, isTrue);
      expect(info.maximumPayloadBytes, 8 * 1024 * 1024);
      expect(info.rawFields["version"], isA<String>());
      expect(info.isLameDuck, isFalse);

      final traceSubject = "integration.$suffix.trace";
      final traceSubscription = await connection.subscribe(traceSubject);
      final target = await connection.subscribe("integration.$suffix.target");
      final requestResponder = await connection.subscribe(
        "integration.$suffix.request.trace",
      );
      final manyResponder = await connection.subscribe(
        "integration.$suffix.many.trace",
      );
      var targetDeliveries = 0;
      final targetListener = target.messages.listen((_) {
        targetDeliveries += 1;
      });
      final firstTrace = Completer<void>();
      final allTraces = Completer<void>();
      var traceEvents = 0;
      final traceListener = traceSubscription.messages.listen((_) {
        traceEvents += 1;
        if (traceEvents == 1) {
          firstTrace.complete();
        }
        if (traceEvents == 3) {
          allTraces.complete();
        }
      });
      final callerHeaders = NatsHeaders(
        entries: const [MapEntry("X-Caller", "preserved")],
      );

      await connection.publish(
        "integration.$suffix.target",
        const [1],
        headers: callerHeaders,
        trace: NatsTraceOptions(
          destination: traceSubject,
          traceOnly: true,
          encoding: NatsTraceEncoding.gzip,
        ),
      );
      await firstTrace.future;
      await connection.flush();
      expect(targetDeliveries, 0);
      expect(callerHeaders.first("Nats-Trace-Dest"), isNull);

      final requestHeaders = Completer<NatsHeaders>();
      final requestListener = requestResponder.messages.listen((request) {
        requestHeaders.complete(request.headers!);
        connection.publish(request.replySubject!, const [2]).ignore();
      });
      final response = await connection.request(
        "integration.$suffix.request.trace",
        const [1],
        trace: NatsTraceOptions(
          destination: traceSubject,
          encoding: NatsTraceEncoding.snappy,
        ),
      );
      expect(response.payload, [2]);
      expect(
        (await requestHeaders.future).first("Nats-Trace-Dest"),
        traceSubject,
      );

      final manyHeaders = Completer<NatsHeaders>();
      final manyListener = manyResponder.messages.listen((request) async {
        manyHeaders.complete(request.headers!);
        await connection.publish(request.replySubject!, const [3]);
        await connection.publish(request.replySubject!, const []);
      });
      final many = await connection.requestMany(
        "integration.$suffix.many.trace",
        const [1],
        trace: NatsTraceOptions(destination: traceSubject),
        options: NatsRequestManyOptions(completeOnEmptyPayload: true),
      );
      expect(
        (await many.messages.toList()).map((message) => message.payload.single),
        [3],
      );
      expect((await manyHeaders.future).first("Nats-Trace-Dest"), traceSubject);
      await allTraces.future;

      await requestListener.cancel();
      await manyListener.cancel();
      await traceListener.cancel();
      await targetListener.cancel();
      await requestResponder.unsubscribe();
      await manyResponder.unsubscribe();
      await traceSubscription.unsubscribe();
      await target.unsubscribe();
      await connection.close();
    },
    skip: messageTracingSkipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}
