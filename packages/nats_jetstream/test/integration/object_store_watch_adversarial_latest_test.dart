@TestOn("vm")
library;

import "dart:async";
import "dart:convert";
import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  final serverUrl = Platform.environment["NATS_URL"];
  final monitorUrl = Platform.environment["NATS_MONITOR_URL"];
  final skipReason = serverUrl == null ? "NATS_URL is not configured" : false;
  final monitorSkipReason = serverUrl == null
      ? "NATS_URL is not configured"
      : monitorUrl == null
      ? "NATS_MONITOR_URL is not configured"
      : false;

  test(
    "Object Store watch owns delivery interest before consumer creation",
    () async {
      final fixture = await _Fixture.create(serverUrl!);
      addTearDown(fixture.close);
      final stored = await fixture.bucket.putBytes("preexisting", const [1]);
      final createRequests = await fixture.connection.subscribe(
        "\$JS.API.CONSUMER.CREATE.OBJ_${fixture.bucketName}.>",
      );
      addTearDown(createRequests.unsubscribe);
      await fixture.connection.flush();
      final createRequest = createRequests.messages.first;

      final watch = fixture.bucket.watch();
      final events = StreamIterator(watch.events);
      final firstEvent = events.moveNext();
      final request = await createRequest.timeout(const Duration(seconds: 2));
      final decoded = jsonDecode(utf8.decode(request.payload));
      final deliverySubject = switch (decoded) {
        {"config": {"deliver_subject": final String subject}} => subject,
        _ => throw StateError(
          "Consumer create request omitted delivery subject",
        ),
      };

      await _waitForSubscription(
        Uri.parse(monitorUrl!),
        fixture.clientName,
        deliverySubject,
      );
      expect(await firstEvent.timeout(const Duration(seconds: 2)), isTrue);
      final changed = events.current as JetStreamObjectChanged;
      expect(changed.info.name, "preexisting");
      expect(changed.info.revision, stored.revision);
      expect(
        await events.moveNext().timeout(const Duration(seconds: 2)),
        isTrue,
      );
      expect(events.current, isA<JetStreamObjectInitialStateComplete>());

      await events.cancel();
      await watch.done;
      expect(await fixture.consumerCount(), BigInt.zero);
    },
    skip: monitorSkipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "Object Store watch enforces queue bounds and cleans consumers",
    () async {
      final fixture = await _Fixture.create(serverUrl!);
      addTearDown(fixture.close);

      final messageWatch = fixture.bucket.watch(
        options: JetStreamObjectWatchOptions(
          start: const JetStreamObjectWatchStart.updates(),
          maximumPendingMessages: 1,
        ),
      );
      final messageEvents = StreamIterator(messageWatch.events);
      expect(await messageEvents.moveNext(), isTrue);
      expect(messageEvents.current, isA<JetStreamObjectInitialStateComplete>());
      final messageDone = expectLater(
        messageWatch.done,
        throwsA(isA<NatsSlowConsumerException>()),
      );
      for (var value = 1; value <= 4; value += 1) {
        await fixture.bucket.putBytes("message-$value", [value]);
      }
      await messageDone;
      await expectLater(
        messageEvents.moveNext(),
        throwsA(isA<NatsSlowConsumerException>()),
      );
      await messageEvents.cancel();
      expect(await fixture.consumerCount(), BigInt.zero);

      final byteWatch = fixture.bucket.watch(
        options: JetStreamObjectWatchOptions(
          start: const JetStreamObjectWatchStart.updates(),
          maximumPendingBytes: 1,
        ),
      );
      final byteEvents = StreamIterator(byteWatch.events);
      expect(await byteEvents.moveNext(), isTrue);
      final byteDone = expectLater(
        byteWatch.done,
        throwsA(isA<NatsSlowConsumerException>()),
      );
      for (var value = 1; value <= 16; value += 1) {
        await fixture.bucket.putBytes("byte-$value", [value]);
      }
      await byteDone;
      await expectLater(
        byteEvents.moveNext(),
        throwsA(isA<NatsSlowConsumerException>()),
      );
      await byteEvents.cancel();
      expect(await fixture.consumerCount(), BigInt.zero);
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "Object Store watch preserves cursor through suppression and corruption",
    () async {
      final fixture = await _Fixture.create(serverUrl!);
      addTearDown(fixture.close);

      final first = await fixture.bucket.putBytes("first", const [1]);
      final second = await fixture.bucket.putBytes("second", const [2]);
      final revisionWatch = fixture.bucket.watch(
        options: JetStreamObjectWatchOptions(
          start: JetStreamObjectWatchStart.revision(first.revision),
        ),
      );
      final revisionEvents = StreamIterator(revisionWatch.events);
      expect(await revisionEvents.moveNext(), isTrue);
      expect(
        (revisionEvents.current as JetStreamObjectChanged).info.revision,
        first.revision,
      );
      expect(await revisionEvents.moveNext(), isTrue);
      expect(
        (revisionEvents.current as JetStreamObjectChanged).info.revision,
        second.revision,
      );
      expect(await revisionEvents.moveNext(), isTrue);
      expect(
        revisionEvents.current,
        isA<JetStreamObjectInitialStateComplete>(),
      );
      await revisionWatch.close();
      await revisionEvents.cancel();

      final suppressionWatch = fixture.bucket.watch(
        options: JetStreamObjectWatchOptions(
          start: const JetStreamObjectWatchStart.updates(),
          ignoreDeletes: true,
        ),
      );
      final suppressionEvents = StreamIterator(suppressionWatch.events);
      expect(await suppressionEvents.moveNext(), isTrue);
      await fixture.bucket.delete("first");
      final sentinel = await fixture.bucket.putBytes("sentinel", const [3]);
      expect(await suppressionEvents.moveNext(), isTrue);
      final sentinelEvent = suppressionEvents.current as JetStreamObjectChanged;
      expect(sentinelEvent.info.name, "sentinel");
      expect(sentinelEvent.info.revision, sentinel.revision);
      await suppressionWatch.close();
      await suppressionEvents.cancel();

      final malformedWatch = fixture.bucket.watch(
        options: JetStreamObjectWatchOptions(
          start: const JetStreamObjectWatchStart.updates(),
        ),
      );
      final malformedEvents = StreamIterator(malformedWatch.events);
      expect(await malformedEvents.moveNext(), isTrue);
      final malformedDone = expectLater(
        malformedWatch.done,
        throwsA(isA<JetStreamProtocolException>()),
      );
      final malformedNext = expectLater(
        malformedEvents.moveNext(),
        throwsA(isA<JetStreamProtocolException>()),
      );
      final badName = base64Url.encode(utf8.encode("live-bad"));
      await fixture.jetStream.publish(
        "\$O.${fixture.bucketName}.M.$badName",
        utf8.encode("{"),
      );
      await malformedDone;
      await malformedNext;
      await malformedEvents.cancel();
      expect(await fixture.consumerCount(), BigInt.zero);
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}

final class _Fixture {
  const _Fixture(
    this.connection,
    this.jetStream,
    this.bucket,
    this.bucketName,
    this.clientName,
  );

  final NatsConnection connection;
  final JetStreamContext jetStream;
  final JetStreamObjectStore bucket;
  final String bucketName;
  final String clientName;

  static Future<_Fixture> create(String serverUrl) async {
    final suffix = DateTime.now().microsecondsSinceEpoch;
    final clientName = "nats.dart.object.watch.$suffix";
    final connection = await NatsConnection.connect(
      NatsOptions(servers: [NatsServer.parse(serverUrl)], name: clientName),
    );
    final jetStream = JetStreamContext(connection);
    final bucketName = "A$suffix";
    final bucket = await jetStream.objectStore.createStore(
      JetStreamObjectStoreConfig(
        name: bucketName,
        storage: JetStreamStorage.memory,
      ),
    );
    return _Fixture(connection, jetStream, bucket, bucketName, clientName);
  }

  Future<BigInt> consumerCount() async =>
      (await jetStream.consumers.listPage("OBJ_$bucketName")).total;

  Future<void> close() async {
    try {
      await jetStream.objectStore.deleteStore(bucketName);
    } on JetStreamApiException {
      // The test already proved or attempted deletion.
    }
    await connection.close();
  }
}

Future<void> _waitForSubscription(
  Uri monitorUrl,
  String clientName,
  String expected,
) async {
  final deadline = DateTime.timestamp().add(const Duration(seconds: 2));
  var last = const <String>[];
  while (DateTime.timestamp().isBefore(deadline)) {
    last = await _subscriptionsFor(monitorUrl, clientName);
    if (last.contains(expected)) return;
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
  throw StateError(
    "NATS monitor did not expose $expected for $clientName. Last: $last",
  );
}

Future<List<String>> _subscriptionsFor(
  Uri monitorUrl,
  String clientName,
) async {
  final client = HttpClient();
  try {
    final request = await client.getUrl(
      monitorUrl.replace(
        path: "/connz",
        queryParameters: const {
          "state": "open",
          "subs": "1",
          "subscriptions_detail": "true",
        },
      ),
    );
    final response = await request.close();
    final decoded = jsonDecode(await utf8.decodeStream(response));
    if (decoded case {"connections": final List<Object?> connections}) {
      for (final connection in connections) {
        if (connection case {
          "name": final String name,
          "subscriptions_list": final List<Object?> subscriptions,
        } when name == clientName) {
          return subscriptions
              .map(
                (subscription) => switch (subscription) {
                  String() => subscription,
                  {"subject": final String subject} => subject,
                  _ => null,
                },
              )
              .whereType<String>()
              .toList(growable: false);
        }
      }
    }
    return const [];
  } finally {
    client.close(force: true);
  }
}
