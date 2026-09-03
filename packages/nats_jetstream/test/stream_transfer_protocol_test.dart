@TestOn("vm")
library;

import "dart:async";
import "dart:convert";
import "dart:typed_data";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:test/test.dart";

import "support/scripted_nats_peer.dart";

void main() {
  test(
    "snapshot uses exact framing and acknowledges after sink acceptance",
    () async {
      final peer = await ScriptedNatsPeer.start();
      final connection = await _connect(peer);
      addTearDown(() async {
        await connection.close();
        await peer.close();
      });
      final jetStream = JetStreamContext(connection);
      final sinkEntered = Completer<void>();
      final releaseSink = Completer<void>();
      final accepted = BytesBuilder(copy: false);
      var writes = 0;

      final resultFuture = jetStream.snapshotStreamTo(
        "EVENTS",
        (chunk) async {
          writes += 1;
          accepted.add(chunk);
          if (writes == 1) {
            sinkEntered.complete();
            await releaseSink.future;
          }
        },
        maximumBytes: BigInt.from(4096),
        options: JetStreamSnapshotOptions(
          includeConsumers: false,
          checkMessages: true,
          chunkSize: 1024,
          windowSize: 1024,
          maximumBufferedBytes: 2048,
          timeout: const Duration(seconds: 5),
        ),
      );

      final request = await peer.nextPublication(
        r"$JS.API.STREAM.SNAPSHOT.EVENTS",
      );
      expect(request.command, "HPUB");
      expect(
        ascii.decode(request.headerBytes),
        contains("Nats-Required-Api-Level: 4\r\n"),
      );
      final body = decodeExactJsonObject(request.body);
      expect(body["no_consumers"], isTrue);
      expect(body["chunk_size"], BigInt.from(1024));
      expect(body["window_size"], BigInt.from(1024));
      expect(body["jsck"], isTrue);
      final inbox = body["deliver_subject"]! as String;

      await peer.sendMessage(
        request.replySubject!,
        utf8.encode(
          r'{"config":{"name":"EVENTS","future":18446744073709551615},'
          r'"state":{"messages":2,"future_state":{"value":true}}}',
        ),
      );
      final first = Uint8List(1024)..fillRange(0, 1024, 7);
      await peer.sendMessage(inbox, first, replySubject: "snapshot.ack.1");
      await sinkEntered.future;
      await Future<void>.delayed(Duration.zero);
      expect(
        peer.publications.where((value) => value.subject == "snapshot.ack.1"),
        isEmpty,
      );

      releaseSink.complete();
      final acknowledgement = await peer.nextPublication("snapshot.ack.1");
      expect(acknowledgement.command, "PUB");
      expect(acknowledgement.body, isEmpty);
      await peer.sendMessage(inbox, const [1, 2, 3]);
      await peer.sendMessage(inbox, const <int>[]);

      final result = await resultFuture;
      expect(result.bytes, BigInt.from(1027));
      expect(result.chunks, 2);
      expect(accepted.takeBytes(), [...first, 1, 2, 3]);
      expect(
        result.metadata.config["future"],
        BigInt.parse("18446744073709551615"),
      );
      expect(result.metadata.state["future_state"], {"value": true});
    },
  );

  test(
    "restore sends sequential headerless chunks and one empty completion",
    () async {
      final peer = await ScriptedNatsPeer.start();
      final connection = await _connect(peer);
      addTearDown(() async {
        await connection.close();
        await peer.close();
      });
      final jetStream = JetStreamContext(connection);
      final metadata = JetStreamSnapshotMetadata(
        config: {
          "name": "EVENTS",
          "future": BigInt.parse("18446744073709551615"),
        },
        state: const {"future_state": true},
      );
      final mutableRemainder = [3, 4, 5];

      final resultFuture = jetStream.restoreStreamFrom(
        metadata,
        Stream<List<int>>.fromIterable([
          const [1, 2],
          const [],
          mutableRemainder,
        ]),
        maximumBytes: BigInt.from(5),
        options: JetStreamRestoreOptions(
          uploadChunkSize: 4,
          chunkTimeout: const Duration(seconds: 1),
          completionTimeout: const Duration(seconds: 2),
        ),
      );

      final start = await peer.nextPublication(
        r"$JS.API.STREAM.RESTORE.EVENTS",
      );
      expect(start.command, "HPUB");
      expect(
        ascii.decode(start.headerBytes),
        contains("Nats-Required-Api-Level: 4\r\n"),
      );
      final startBody = decodeExactJsonObject(start.body);
      expect(
        (startBody["config"]! as Map<String, Object?>)["future"],
        BigInt.parse("18446744073709551615"),
      );
      expect(startBody["state"], {"future_state": true});

      const upload = "restore.upload.EVENTS";
      await peer.sendMessage(
        start.replySubject!,
        utf8.encode(r'{"deliver_subject":"restore.upload.EVENTS"}'),
      );
      final first = await peer.nextPublication(upload);
      expect(first.command, "PUB");
      expect(first.headerBytes, isEmpty);
      expect(first.body, [1, 2, 3, 4]);
      mutableRemainder[2] = 99;
      await peer.sendMessage(first.replySubject!, const <int>[]);

      final second = await peer.nextPublication(upload);
      expect(second.command, "PUB");
      expect(second.headerBytes, isEmpty);
      expect(second.body, [5]);
      await peer.sendMessage(second.replySubject!, const <int>[]);

      final completion = await peer.nextPublication(upload);
      expect(completion.command, "PUB");
      expect(completion.headerBytes, isEmpty);
      expect(completion.body, isEmpty);
      await peer.sendMessage(
        completion.replySubject!,
        encodeExactJson({
          "config": {"name": "EVENTS"},
          "created": "2026-09-01T00:00:00Z",
          "state": {
            "messages": BigInt.zero,
            "bytes": BigInt.zero,
            "first_seq": BigInt.zero,
            "first_ts": "0001-01-01T00:00:00Z",
            "last_seq": BigInt.zero,
            "last_ts": "0001-01-01T00:00:00Z",
            "consumer_count": BigInt.zero,
          },
          "ts": "2026-09-01T00:00:01Z",
        }),
      );

      final result = await resultFuture;
      expect(result.config.name, "EVENTS");
      expect(
        peer.publications.where((value) => value.subject == upload).length,
        3,
      );
    },
  );

  test("restore rejects nonbyte source and sends its abort marker", () async {
    final peer = await ScriptedNatsPeer.start();
    final connection = await _connect(peer);
    addTearDown(() async {
      await connection.close();
      await peer.close();
    });
    final jetStream = JetStreamContext(connection);
    final metadata = JetStreamSnapshotMetadata(
      config: const {"name": "EVENTS"},
      state: const {},
    );

    final resultFuture = jetStream.restoreStreamFrom(
      metadata,
      Stream.value(const [256]),
      maximumBytes: BigInt.one,
    );
    final failure = expectLater(
      resultFuture,
      throwsA(
        isA<JetStreamRestoreException>()
            .having((error) => error.partialBytes, "partialBytes", BigInt.zero)
            .having((error) => error.cause, "cause", isA<FormatException>()),
      ),
    );
    final start = await peer.nextPublication(r"$JS.API.STREAM.RESTORE.EVENTS");
    const upload = "restore.upload.EVENTS";
    await peer.sendMessage(
      start.replySubject!,
      utf8.encode(r'{"deliver_subject":"restore.upload.EVENTS"}'),
    );

    final abort = await peer.nextPublication(upload);
    expect(abort.body, [0]);
    await failure;
    expect(
      peer.publications.where((value) => value.subject == upload),
      hasLength(1),
    );
  });
}

Future<NatsConnection> _connect(ScriptedNatsPeer peer) {
  return NatsConnection.connect(
    NatsOptions(
      servers: [NatsServer.parse(peer.uri.toString())],
      reconnect: NatsReconnectPolicy(maximumAttempts: 0),
      pingInterval: const Duration(minutes: 10),
    ),
  );
}
