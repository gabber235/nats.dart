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
  final fixtureBinary = Platform.environment["NATS_GO_INTEROP_BINARY"];
  final skipReason = serverUrl == null
      ? "NATS_URL is not configured"
      : fixtureBinary == null
      ? "NATS_GO_INTEROP_BINARY is not configured"
      : false;

  test(
    "official nats.go and Dart exchange Core, JetStream, KV, and Object Store",
    () async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      final prefix = "interop.$suffix";
      final streamName = "INTEROP$suffix";
      final keyValueName = "K$suffix";
      final objectStoreName = "O$suffix";
      final connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(serverUrl!)],
          name: "nats.dart interoperability test",
        ),
      );
      final jetStream = JetStreamContext(connection);
      var streamCreated = false;
      var keyValueCreated = false;
      var objectStoreCreated = false;
      addTearDown(() async {
        if (objectStoreCreated) {
          try {
            await jetStream.objectStore.deleteStore(objectStoreName);
          } on JetStreamApiException {
            // The test may fail after server side cleanup.
          }
        }
        if (keyValueCreated) {
          try {
            await jetStream.keyValue.deleteBucket(keyValueName);
          } on JetStreamApiException {
            // The test may fail after server side cleanup.
          }
        }
        if (streamCreated) {
          try {
            await jetStream.deleteStream(streamName);
          } on JetStreamApiException {
            // The test may fail after server side cleanup.
          }
        }
        await connection.close();
      });

      final fromGo = await connection.subscribe("$prefix.core.from_go");
      final observedByGo = await connection.subscribe(
        "$prefix.core.observed_by_go",
      );
      final dartRequests = await connection.subscribe("$prefix.dart.request");
      final responderTasks = _PendingTasks();
      final responder = dartRequests.messages.listen((request) {
        responderTasks.add(
          connection.publish(
            request.replySubject!,
            request.payload.reversed.toList(growable: false),
          ),
        );
      });
      addTearDown(() async {
        await responder.cancel();
        await responderTasks.close();
        await dartRequests.unsubscribe();
        await fromGo.unsubscribe();
        await observedByGo.unsubscribe();
      });
      await connection.flush();

      await jetStream.createStream(
        JetStreamStreamConfig(
          name: streamName,
          subjects: ["$prefix.js.>"],
          storage: JetStreamStorage.memory,
        ),
      );
      streamCreated = true;
      final keyValue = await jetStream.keyValue.createBucket(
        JetStreamKeyValueBucketConfig(
          name: keyValueName,
          storage: JetStreamStorage.memory,
        ),
      );
      keyValueCreated = true;
      final objectStore = await jetStream.objectStore.createStore(
        JetStreamObjectStoreConfig(
          name: objectStoreName,
          storage: JetStreamStorage.memory,
        ),
      );
      objectStoreCreated = true;

      final fixture = await Process.start(fixtureBinary!, [
        "--url",
        serverUrl,
        "--prefix",
        prefix,
        "--stream",
        streamName,
        "--kv",
        keyValueName,
        "--object",
        objectStoreName,
      ]);
      final fixtureErrors = StringBuffer();
      final errorListener = fixture.stderr
          .transform(utf8.decoder)
          .listen(fixtureErrors.write);
      final output = fixture.stdout
          .transform(utf8.decoder)
          .transform(const LineSplitter())
          .asBroadcastStream();

      addTearDown(() async {
        fixture.kill();
        try {
          await fixture.exitCode.timeout(const Duration(seconds: 2));
        } on TimeoutException {
          fixture.kill(ProcessSignal.sigkill);
          await fixture.exitCode;
        }
        await errorListener.cancel();
      });

      await output
          .firstWhere((line) => line == "READY")
          .timeout(
            const Duration(seconds: 5),
            onTimeout: () => throw StateError(
              "nats.go fixture was not ready: $fixtureErrors",
            ),
          );

      final binaryPayload = [0, 255, 13, 10, 128, 1];

      final dartToGo = observedByGo.messages.first;
      await connection.publish("$prefix.core.to_go", binaryPayload);
      await connection.flush();
      expect(
        (await dartToGo.timeout(const Duration(seconds: 2))).payload,
        binaryPayload,
      );

      final goToDart = fromGo.messages.first;
      final publishReceipt = await connection.request(
        "$prefix.command.publish",
        binaryPayload.reversed.toList(growable: false),
      );
      expect(utf8.decode(publishReceipt.payload), "OK");
      expect(
        (await goToDart.timeout(const Duration(seconds: 2))).payload,
        binaryPayload.reversed.toList(growable: false),
      );

      final goResponse = await connection.request(
        "$prefix.core.request",
        binaryPayload,
      );
      expect(goResponse.payload, [...utf8.encode("go:"), ...binaryPayload]);

      final dartResponseThroughGo = await connection.request(
        "$prefix.command.request_dart",
        binaryPayload,
      );
      expect(
        dartResponseThroughGo.payload,
        binaryPayload.reversed.toList(growable: false),
      );

      final dartPublish = await jetStream.publish(
        "$prefix.js.from_dart",
        binaryPayload,
      );
      final goRead = await connection.request(
        "$prefix.command.js_get",
        utf8.encode(dartPublish.sequence.toString()),
      );
      expect(goRead.payload, binaryPayload);

      final goJetStreamPayload = binaryPayload.reversed.toList(growable: false);
      final goPublish = await connection.request(
        "$prefix.command.js_publish",
        goJetStreamPayload,
      );
      final goSequence = BigInt.parse(utf8.decode(goPublish.payload));
      final dartRead = await jetStream.getMessage(streamName, goSequence);
      expect(dartRead.subject, "$prefix.js.from_go");
      expect(dartRead.payload, goJetStreamPayload);
      expect((await jetStream.streamInfo(streamName)).messages, BigInt.two);

      final dartKeyValueRevision = await keyValue.put("dart", binaryPayload);
      expect(dartKeyValueRevision, greaterThan(BigInt.zero));
      final goKeyValueRead = await connection.request(
        "$prefix.command.kv_get",
        const [],
      );
      expect(goKeyValueRead.payload, binaryPayload);

      final goKeyValuePayload = binaryPayload.reversed.toList(growable: false);
      final goKeyValueWrite = await connection.request(
        "$prefix.command.kv_put",
        goKeyValuePayload,
      );
      expect(
        BigInt.parse(utf8.decode(goKeyValueWrite.payload)),
        greaterThan(BigInt.zero),
      );
      expect((await keyValue.get("go"))?.value, goKeyValuePayload);

      final dartObject = await objectStore.putBytes("dart", binaryPayload);
      expect(dartObject.size, BigInt.from(binaryPayload.length));
      final goObjectRead = await connection.request(
        "$prefix.command.object_get",
        const [],
      );
      expect(goObjectRead.payload, binaryPayload);

      final goObjectPayload = binaryPayload.reversed.toList(growable: false);
      final goObjectWrite = await connection.request(
        "$prefix.command.object_put",
        goObjectPayload,
      );
      expect(utf8.decode(goObjectWrite.payload), startsWith("SHA-256="));
      expect(await objectStore.getBytes("go"), goObjectPayload);
      final goObjectInfo = await objectStore.getInfo("go");
      expect(goObjectInfo?.size, BigInt.from(goObjectPayload.length));
      expect(goObjectInfo?.digest, utf8.decode(goObjectWrite.payload));
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}

final class _PendingTasks {
  final Set<Future<void>> _tasks = {};
  Object? _error;
  StackTrace? _stackTrace;

  void add(Future<void> task) {
    final observed = _observe(task);
    _tasks.add(observed);
    unawaited(observed.whenComplete(() => _tasks.remove(observed)));
  }

  Future<void> close() async {
    while (_tasks.isNotEmpty) {
      await Future.wait(_tasks.toList(growable: false));
    }
    final error = _error;
    if (error != null) {
      Error.throwWithStackTrace(error, _stackTrace!);
    }
  }

  Future<void> _observe(Future<void> task) async {
    try {
      await task;
    } on Object catch (error, stackTrace) {
      _error ??= error;
      _stackTrace ??= stackTrace;
    }
  }
}
