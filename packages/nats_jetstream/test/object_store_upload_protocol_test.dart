@TestOn("vm")
library;

import "dart:async";
import "dart:convert";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";
import "package:nats_jetstream/src/exact_json.dart";
import "package:nats_jetstream/src/object_store_protocol.dart";
import "package:nats_jetstream/src/object_store_upload.dart";
import "package:test/test.dart";

import "support/scripted_nats_peer.dart";

void main() {
  test(
    "custom API prefix leaves Object Store publications unprefixed",
    () async {
      final peer = await ScriptedNatsPeer.start();
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
      final protocol = JetStreamObjectProtocol(
        connection,
        r"$JS.DOMAIN.API",
        const Duration(seconds: 2),
        "BUCKET",
      );

      final chunkResult = protocol.publish(
        protocol.chunkSubject("identifier"),
        const [1, 2, 3],
      );
      final chunk = await peer.nextPublication(r"$O.BUCKET.C.identifier");
      await peer.sendMessage(
        chunk.replySubject!,
        encodeExactJson({"stream": "OBJ_BUCKET", "seq": BigInt.one}),
      );
      expect(await chunkResult, BigInt.one);

      final storedMetadataSubject = protocol.metadataSubject("object");
      final metadataResult = protocol.publish(
        storedMetadataSubject,
        const [4],
        headers: NatsHeaders(
          entries: [
            MapEntry(
              "Nats-Expected-Last-Subject-Sequence-Subject",
              storedMetadataSubject,
            ),
          ],
        ),
      );
      final metadata = await peer.nextPublication(r"$O.BUCKET.M.b2JqZWN0");
      final headers = NatsHeaders.parse(metadata.headerBytes);
      expect(
        headers.first("Nats-Expected-Last-Subject-Sequence-Subject"),
        r"$O.BUCKET.M.b2JqZWN0",
      );
      await peer.sendMessage(
        metadata.replySubject!,
        encodeExactJson({"stream": "OBJ_BUCKET", "seq": BigInt.two}),
      );
      expect(await metadataResult, BigInt.two);
    },
  );

  test(
    "replacement retries cleanup for a tombstoned previous object",
    () async {
      final peer = await ScriptedNatsPeer.start();
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
      final previous = JetStreamObjectInfo(
        name: "object",
        bucket: "BUCKET",
        identifier: "AAAAAAAAAAAAAAAAAAAAAA",
        revision: BigInt.from(3),
        size: BigInt.one,
        chunks: BigInt.one,
        digest: "",
        modified: "2026-09-02T00:00:00Z",
        deleted: true,
        chunkSize: 128 * 1024,
      );
      var reads = 0;
      final upload = uploadJetStreamObject(
        connection: connection,
        apiPrefix: r"$JS.API",
        timeout: const Duration(seconds: 2),
        bucket: "BUCKET",
        metadata: JetStreamObjectMetadata(name: "object"),
        bytes: const Stream.empty(),
        getInfo: (name, {showDeleted = false}) async {
          reads += 1;
          expect(name, "object");
          expect(showDeleted, isTrue);
          return previous;
        },
      );

      final metadata = await peer
          .nextPublication(r"$O.BUCKET.M.b2JqZWN0")
          .timeout(const Duration(seconds: 2));
      await peer.sendMessage(
        metadata.replySubject!,
        encodeExactJson({"stream": "OBJ_BUCKET", "seq": BigInt.from(4)}),
      );
      final readback = await peer
          .nextPublication(r"$JS.API.STREAM.MSG.GET.OBJ_BUCKET")
          .timeout(const Duration(seconds: 2));
      expect(decodeExactJsonObject(readback.body), {"seq": BigInt.from(4)});
      await peer.sendMessage(
        readback.replySubject!,
        encodeExactJson({
          "error": {
            "code": 500,
            "err_code": 10039,
            "description": "readback failed",
          },
        }),
      );
      final cleanup = await peer
          .nextPublication(r"$JS.API.STREAM.PURGE.OBJ_BUCKET")
          .timeout(const Duration(seconds: 2));
      expect(decodeExactJsonObject(cleanup.body), {
        "filter": r"$O.BUCKET.C.AAAAAAAAAAAAAAAAAAAAAA",
      });
      await peer.sendMessage(
        cleanup.replySubject!,
        encodeExactJson({
          "error": {
            "code": 500,
            "err_code": 10039,
            "description": "cleanup failed",
          },
        }),
      );

      final failure = await _commitFailure(
        upload,
      ).timeout(const Duration(seconds: 2));
      expect(failure.bucket, "BUCKET");
      expect(failure.name, "object");
      expect(failure.revision, BigInt.from(4));
      expect(failure.cause, isA<JetStreamApiException>());
      expect(failure.cleanupCause, isA<JetStreamApiException>());
      expect(reads, 1);
    },
  );

  test("upload snapshots a stream event before its first await", () async {
    final peer = await ScriptedNatsPeer.start();
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
    final source = [1, 2, 3, 4];
    final upload = uploadJetStreamObject(
      connection: connection,
      apiPrefix: r"$JS.API",
      timeout: const Duration(seconds: 2),
      bucket: "BUCKET",
      metadata: JetStreamObjectMetadata(name: "object", chunkSize: 2),
      bytes: Stream.value(source),
      getInfo: (_, {showDeleted = false}) async => null,
    );

    final first = await peer
        .nextPublicationMatching(r"$O.BUCKET.C.*")
        .timeout(const Duration(seconds: 2));
    expect(first.body, [1, 2]);
    expect(first.replySubject, isNot(endsWith(r".$FI")));
    source
      ..[2] = 9
      ..[3] = 9;
    await peer.sendMessage(
      first.replySubject!,
      encodeExactJson({"stream": "OBJ_BUCKET", "seq": BigInt.one}),
    );
    final second = await peer
        .nextPublication(first.subject)
        .timeout(const Duration(seconds: 2));
    expect(second.body, [3, 4]);
    await peer.sendMessage(
      second.replySubject!,
      encodeExactJson({
        "error": {
          "code": 500,
          "err_code": 10039,
          "description": "stop after snapshot proof",
        },
      }),
    );
    final cleanup = await peer
        .nextPublication(r"$JS.API.STREAM.PURGE.OBJ_BUCKET")
        .timeout(const Duration(seconds: 2));
    await peer.sendMessage(
      cleanup.replySubject!,
      encodeExactJson({"success": true}),
    );

    await expectLater(upload, throwsA(isA<JetStreamObjectPutException>()));
  });

  test("object chunk acknowledgement rejects stream sequence zero", () async {
    final peer = await ScriptedNatsPeer.start();
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
    final upload = uploadJetStreamObject(
      connection: connection,
      apiPrefix: r"$JS.API",
      timeout: const Duration(seconds: 2),
      bucket: "BUCKET",
      metadata: JetStreamObjectMetadata(name: "object"),
      bytes: Stream.value(const [1]),
      getInfo: (_, {showDeleted = false}) async => null,
    );

    final chunk = await peer
        .nextPublicationMatching(r"$O.BUCKET.C.*")
        .timeout(const Duration(seconds: 2));
    await peer.sendMessage(
      chunk.replySubject!,
      encodeExactJson({"stream": "OBJ_BUCKET", "seq": BigInt.zero}),
    );
    final cleanup = await peer
        .nextPublication(r"$JS.API.STREAM.PURGE.OBJ_BUCKET")
        .timeout(const Duration(seconds: 2));
    await peer.sendMessage(
      cleanup.replySubject!,
      encodeExactJson({"success": true}),
    );

    final failure = await _putFailure(upload);
    expect(failure.metadataCommitAmbiguous, isFalse);
    expect(
      failure.cause,
      isA<JetStreamProtocolException>().having(
        (error) => error.fieldPath,
        "fieldPath",
        "seq",
      ),
    );
  });

  test(
    "domain fast ingest uses stored publication and cleanup subjects",
    () async {
      final peer = await ScriptedNatsPeer.start();
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
      final upload = uploadJetStreamObject(
        connection: connection,
        apiPrefix: r"$JS.DOMAIN.API",
        timeout: const Duration(seconds: 2),
        bucket: "BUCKET",
        metadata: JetStreamObjectMetadata(name: "object", chunkSize: 1),
        bytes: Stream.value(const [1, 2, 3]),
        getInfo: (_, {showDeleted = false}) async => null,
        mode: JetStreamObjectPutMode.experimentalFastIngest,
      );

      final first = await peer
          .nextPublicationMatching(r"$O.BUCKET.C.*")
          .timeout(const Duration(seconds: 2));
      expect(first.body, [1]);
      expect(first.replySubject, contains(".fail.1.0."));
      expect(
        NatsHeaders.parse(first.headerBytes).first("Nats-Expected-Stream"),
        "OBJ_BUCKET",
      );
      await peer.sendMessage(
        first.replySubject!,
        utf8.encode('{"type":"ack","seq":1,"msgs":10}'),
      );
      final second = await peer
          .nextPublication(first.subject)
          .timeout(const Duration(seconds: 2));
      final third = await peer
          .nextPublication(first.subject)
          .timeout(const Duration(seconds: 2));
      expect(second.body, [2]);
      expect(third.body, [3]);
      expect(second.replySubject, contains(".fail.2.1."));
      expect(third.replySubject, contains(".fail.3.2."));
      await peer.sendMessage(
        third.replySubject!,
        utf8.encode('{"type":"gap","last_seq":1,"seq":3}'),
      );
      final cleanup = await peer
          .nextPublication(r"$JS.DOMAIN.API.STREAM.PURGE.OBJ_BUCKET")
          .timeout(const Duration(seconds: 2));
      expect(decodeExactJsonObject(cleanup.body), {"filter": first.subject});
      await peer.sendMessage(
        cleanup.replySubject!,
        encodeExactJson({"success": true}),
      );

      final failure = await _putFailure(upload);
      expect(failure.metadataCommitAmbiguous, isFalse);
      expect(failure.chunkUploadAmbiguous, isTrue);
      expect(
        failure.cause,
        isA<JetStreamFastBatchException>()
            .having((error) => error.sentSequence, "sent", BigInt.from(3))
            .having(
              (error) => error.acknowledgedSequence,
              "acknowledged",
              BigInt.one,
            )
            .having((error) => error.ambiguous, "ambiguous", isTrue),
      );
      expect(
        peer.publications.where(
          (publication) => publication.subject == r"$O.BUCKET.M.b2JqZWN0",
        ),
        isEmpty,
      );
    },
  );

  test("metadata no responders is definitive and purges chunks", () async {
    final peer = await ScriptedNatsPeer.start();
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
    final upload = uploadJetStreamObject(
      connection: connection,
      apiPrefix: r"$JS.API",
      timeout: const Duration(seconds: 2),
      bucket: "BUCKET",
      metadata: JetStreamObjectMetadata(name: "object"),
      bytes: Stream.value(const [1]),
      getInfo: (_, {showDeleted = false}) async => null,
    );

    final chunk = await peer
        .nextPublicationMatching(r"$O.BUCKET.C.*")
        .timeout(const Duration(seconds: 2));
    await peer.sendMessage(
      chunk.replySubject!,
      encodeExactJson({"stream": "OBJ_BUCKET", "seq": BigInt.one}),
    );
    final metadata = await peer
        .nextPublication(r"$O.BUCKET.M.b2JqZWN0")
        .timeout(const Duration(seconds: 2));
    await peer.sendStatus(
      metadata.replySubject!,
      503,
      description: "No Responders",
    );
    final cleanup = await peer
        .nextPublication(r"$JS.API.STREAM.PURGE.OBJ_BUCKET")
        .timeout(const Duration(seconds: 2));
    expect(decodeExactJsonObject(cleanup.body), {"filter": chunk.subject});
    await peer.sendMessage(
      cleanup.replySubject!,
      encodeExactJson({"success": true}),
    );

    final failure = await _putFailure(upload);
    expect(failure.cause, isA<NatsNoRespondersException>());
    expect(failure.metadataCommitAmbiguous, isFalse);
    expect(failure.chunkUploadAmbiguous, isFalse);
  });

  test(
    "metadata preflight rejection is definitive and purges chunks",
    () async {
      final peer = await ScriptedNatsPeer.start(maximumPayloadBytes: 256);
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
      final upload = uploadJetStreamObject(
        connection: connection,
        apiPrefix: r"$JS.API",
        timeout: const Duration(seconds: 2),
        bucket: "BUCKET",
        metadata: JetStreamObjectMetadata(name: "object"),
        bytes: Stream.value(const [1]),
        getInfo: (_, {showDeleted = false}) async => null,
      );

      final chunk = await peer
          .nextPublicationMatching(r"$O.BUCKET.C.*")
          .timeout(
            const Duration(seconds: 2),
            onTimeout: () => throw StateError("Chunk was not published"),
          );
      await peer.sendMessage(
        chunk.replySubject!,
        encodeExactJson({"stream": "OBJ_BUCKET", "seq": BigInt.one}),
      );
      final cleanup = await peer
          .nextPublication(r"$JS.API.STREAM.PURGE.OBJ_BUCKET")
          .timeout(const Duration(seconds: 2));
      expect(decodeExactJsonObject(cleanup.body), {"filter": chunk.subject});
      await peer.sendMessage(
        cleanup.replySubject!,
        encodeExactJson({"success": true}),
      );

      final failure = await _putFailure(upload);
      expect(failure.cause, isA<NatsPublicationPreflightException>());
      expect(failure.metadataCommitAmbiguous, isFalse);
      expect(failure.chunkUploadAmbiguous, isFalse);
      expect(
        peer.publications.where(
          (publication) => publication.subject == r"$O.BUCKET.M.b2JqZWN0",
        ),
        isEmpty,
      );
    },
  );

  test(
    "generation change before metadata is definitive and purges chunks",
    () async {
      final peer = await ScriptedNatsPeer.start();
      final connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(peer.uri.toString())],
          reconnect: NatsReconnectPolicy(
            maximumAttempts: 3,
            initialDelay: const Duration(milliseconds: 10),
            baseDelay: const Duration(milliseconds: 10),
            maximumDelay: const Duration(milliseconds: 10),
            jitter: 0,
          ),
          pingInterval: const Duration(minutes: 10),
        ),
      );
      addTearDown(() async {
        await connection.close();
        await peer.close();
      });
      final generation = connection.generation;
      final chunkAcknowledged = Completer<void>();
      final completeSource = Completer<void>();
      Stream<List<int>> source() async* {
        yield const [1];
        chunkAcknowledged.complete();
        await completeSource.future;
      }

      final upload = uploadJetStreamObject(
        connection: connection,
        apiPrefix: r"$JS.API",
        timeout: const Duration(seconds: 2),
        bucket: "BUCKET",
        metadata: JetStreamObjectMetadata(name: "object", chunkSize: 1),
        bytes: source(),
        getInfo: (_, {showDeleted = false}) async => null,
      );

      final chunk = await peer
          .nextPublicationMatching(r"$O.BUCKET.C.*")
          .timeout(
            const Duration(seconds: 2),
            onTimeout: () => throw StateError("Chunk was not published"),
          );
      await peer.sendMessage(
        chunk.replySubject!,
        encodeExactJson({"stream": "OBJ_BUCKET", "seq": BigInt.one}),
      );
      await chunkAcknowledged.future.timeout(
        const Duration(seconds: 2),
        onTimeout: () => throw StateError("Chunk was not acknowledged"),
      );
      final reconnected = connection.events
          .where((event) => event is NatsConnected)
          .cast<NatsConnected>()
          .firstWhere((event) => event.generation > generation);
      await peer.disconnectClient();
      await reconnected.timeout(
        const Duration(seconds: 2),
        onTimeout: () => throw StateError("Connection did not reconnect"),
      );
      completeSource.complete();

      final cleanup = await peer
          .nextPublication(r"$JS.API.STREAM.PURGE.OBJ_BUCKET")
          .timeout(
            const Duration(seconds: 2),
            onTimeout: () => throw StateError("Chunk purge was not requested"),
          );
      expect(decodeExactJsonObject(cleanup.body), {"filter": chunk.subject});
      await peer.sendMessage(
        cleanup.replySubject!,
        encodeExactJson({"success": true}),
      );

      final failure = await _putFailure(upload);
      expect(
        failure.cause,
        isA<NatsPublicationGenerationException>()
            .having(
              (error) => error.expectedGeneration,
              "expected generation",
              generation,
            )
            .having(
              (error) => error.actualGeneration,
              "actual generation",
              greaterThan(generation),
            ),
      );
      expect(failure.metadataCommitAmbiguous, isFalse);
      expect(failure.chunkUploadAmbiguous, isFalse);
      expect(
        peer.publications.where(
          (publication) => publication.subject == r"$O.BUCKET.M.b2JqZWN0",
        ),
        isEmpty,
      );
    },
  );

  test("delete rejects a stale generation without changing metadata", () async {
    final peer = await ScriptedNatsPeer.start();
    final connection = await _reconnectingConnection(peer);
    addTearDown(() async {
      await connection.close();
      await peer.close();
    });
    final staleGeneration = connection.generation;
    await _reconnect(peer, connection, staleGeneration);
    final bucket = JetStreamObjectStore(
      connection,
      r"$JS.API",
      const Duration(seconds: 2),
      "BUCKET",
    );

    final deletion = bucket.delete(
      "object",
      expectedGeneration: staleGeneration,
    );
    await _replyWithStoredEmptyObject(peer);

    await expectLater(
      deletion,
      throwsA(
        isA<NatsPublicationGenerationException>().having(
          (error) => error.expectedGeneration,
          "expected generation",
          staleGeneration,
        ),
      ),
    );
    expect(
      peer.publications.where(
        (publication) => publication.subject == r"$O.BUCKET.M.b2JqZWN0",
      ),
      isEmpty,
    );
  });

  test(
    "metadata update rejects a stale generation without changing metadata",
    () async {
      final peer = await ScriptedNatsPeer.start();
      final connection = await _reconnectingConnection(peer);
      addTearDown(() async {
        await connection.close();
        await peer.close();
      });
      final staleGeneration = connection.generation;
      await _reconnect(peer, connection, staleGeneration);
      final bucket = JetStreamObjectStore(
        connection,
        r"$JS.API",
        const Duration(seconds: 2),
        "BUCKET",
      );

      final update = bucket.updateMetadata(
        "object",
        JetStreamObjectMetadataUpdate(name: "object", description: "changed"),
        expectedGeneration: staleGeneration,
      );
      await _replyWithStoredEmptyObject(peer);

      await expectLater(
        update,
        throwsA(
          isA<NatsPublicationGenerationException>().having(
            (error) => error.expectedGeneration,
            "expected generation",
            staleGeneration,
          ),
        ),
      );
      expect(
        peer.publications.where(
          (publication) => publication.subject == r"$O.BUCKET.M.b2JqZWN0",
        ),
        isEmpty,
      );
    },
  );
}

Future<NatsConnection> _reconnectingConnection(ScriptedNatsPeer peer) {
  return NatsConnection.connect(
    NatsOptions(
      servers: [NatsServer.parse(peer.uri.toString())],
      reconnect: NatsReconnectPolicy(
        maximumAttempts: 3,
        initialDelay: const Duration(milliseconds: 10),
        baseDelay: const Duration(milliseconds: 10),
        maximumDelay: const Duration(milliseconds: 10),
        jitter: 0,
      ),
      pingInterval: const Duration(minutes: 10),
    ),
  );
}

Future<void> _reconnect(
  ScriptedNatsPeer peer,
  NatsConnection connection,
  int generation,
) async {
  final reconnected = connection.events
      .where((event) => event is NatsConnected)
      .cast<NatsConnected>()
      .firstWhere((event) => event.generation > generation);
  await peer.disconnectClient();
  await reconnected.timeout(
    const Duration(seconds: 2),
    onTimeout: () => throw StateError("Connection did not reconnect"),
  );
}

Future<void> _replyWithStoredEmptyObject(ScriptedNatsPeer peer) async {
  final lookup = await peer
      .nextPublication(r"$JS.API.STREAM.MSG.GET.OBJ_BUCKET")
      .timeout(const Duration(seconds: 2));
  expect(decodeExactJsonObject(lookup.body), {
    "last_by_subj": r"$O.BUCKET.M.b2JqZWN0",
  });
  final metadata = encodeExactJson({
    "name": "object",
    "bucket": "BUCKET",
    "nuid": "AAAAAAAAAAAAAAAAAAAAAA",
    "size": BigInt.zero,
    "chunks": BigInt.zero,
    "digest": "SHA-256=47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU=",
  });
  await peer.sendMessage(
    lookup.replySubject!,
    encodeExactJson({
      "message": {
        "subject": r"$O.BUCKET.M.b2JqZWN0",
        "seq": BigInt.one,
        "data": base64Encode(metadata),
        "time": "2026-09-02T00:00:00Z",
      },
    }),
  );
}

Future<JetStreamObjectPutException> _putFailure(
  Future<JetStreamObjectInfo> upload,
) async {
  try {
    await upload;
  } on JetStreamObjectPutException catch (error) {
    return error;
  }
  throw StateError("Object upload unexpectedly succeeded");
}

Future<JetStreamObjectCommitVerificationException> _commitFailure(
  Future<JetStreamObjectInfo> upload,
) async {
  try {
    await upload;
  } on JetStreamObjectCommitVerificationException catch (error) {
    return error;
  }
  throw StateError("Object upload unexpectedly succeeded");
}
