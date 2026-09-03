import "dart:async";
import "dart:io";
import "dart:typed_data";

import "package:crypto/crypto.dart";
import "package:test/test.dart";

import "../tool/native_nats_server.dart";

void main() {
  group("cacheVerifiedNativeNatsArchive", () {
    test(
      "fails when the server does not return headers before deadline",
      () async {
        final fixture = await _Fixture.create();
        addTearDown(fixture.close);
        final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
        addTearDown(() => server.close(force: true));
        var requests = 0;
        server.listen((request) {
          requests += 1;
        });

        await expectLater(
          cacheVerifiedNativeNatsArchive(
            source: _serverUri(server),
            destination: fixture.archive,
            expectedSha256: _digest(const [1, 2, 3]),
            timeout: const Duration(seconds: 1),
          ),
          throwsA(isA<TimeoutException>()),
        );

        expect(requests, 1);
        expect(fixture.archive.existsSync(), isFalse);
        expect(fixture.files, isEmpty);
      },
    );

    test(
      "fails when the response body does not finish before deadline",
      () async {
        final fixture = await _Fixture.create();
        addTearDown(fixture.close);
        final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
        HttpResponse? pendingResponse;
        addTearDown(() async {
          try {
            await pendingResponse?.close();
          } on Object {
            // The client closes this response when its deadline expires.
          }
          await server.close(force: true);
        });
        server.listen((request) {
          pendingResponse = request.response;
          request.response.contentLength = 2;
          request.response.add(const [1]);
          unawaited(request.response.flush().onError((_, _) {}));
        });

        await expectLater(
          cacheVerifiedNativeNatsArchive(
            source: _serverUri(server),
            destination: fixture.archive,
            expectedSha256: _digest(const [1, 2]),
            timeout: const Duration(milliseconds: 50),
          ),
          throwsA(isA<TimeoutException>()),
        );

        expect(fixture.archive.existsSync(), isFalse);
        expect(fixture.files, isEmpty);
      },
    );

    test("replaces a truncated cached archive", () async {
      final fixture = await _Fixture.create();
      addTearDown(fixture.close);
      final expected = Uint8List.fromList(const [1, 2, 3, 4, 5]);
      await fixture.archive.writeAsBytes(const [1, 2]);
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(() => server.close(force: true));
      var requests = 0;
      server.listen((request) async {
        requests += 1;
        request.response.add(expected);
        await request.response.close();
      });

      final actualDigest = await cacheVerifiedNativeNatsArchive(
        source: _serverUri(server),
        destination: fixture.archive,
        expectedSha256: _digest(expected),
        timeout: const Duration(seconds: 2),
      );

      expect(actualDigest, _digest(expected));
      expect(await fixture.archive.readAsBytes(), expected);
      expect(requests, 1);
      expect(fixture.files, [fixture.archive.path]);
    });

    test("an interrupted download cannot become a cache hit", () async {
      final fixture = await _Fixture.create();
      addTearDown(fixture.close);
      final expected = Uint8List.fromList(const [1, 2, 3, 4, 5, 6]);
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(() => server.close(force: true));
      var requests = 0;
      server.listen((request) async {
        requests += 1;
        if (requests == 1) {
          request.response.contentLength = expected.length;
          final socket = await request.response.detachSocket();
          socket.add(expected.sublist(0, 2));
          await socket.flush();
          socket.destroy();
          return;
        }
        request.response.add(expected);
        await request.response.close();
      });

      await expectLater(
        cacheVerifiedNativeNatsArchive(
          source: _serverUri(server),
          destination: fixture.archive,
          expectedSha256: _digest(expected),
          timeout: const Duration(seconds: 2),
        ),
        throwsA(isA<HttpException>()),
      );

      expect(fixture.archive.existsSync(), isFalse);
      expect(fixture.files, isEmpty);

      final actualDigest = await cacheVerifiedNativeNatsArchive(
        source: _serverUri(server),
        destination: fixture.archive,
        expectedSha256: _digest(expected),
        timeout: const Duration(seconds: 2),
      );

      expect(actualDigest, _digest(expected));
      expect(await fixture.archive.readAsBytes(), expected);
      expect(requests, 2);
      expect(fixture.files, [fixture.archive.path]);
    });

    test("a digest mismatch cannot become a cache hit", () async {
      final fixture = await _Fixture.create();
      addTearDown(fixture.close);
      final expected = Uint8List.fromList(const [1, 2, 3, 4]);
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(() => server.close(force: true));
      var requests = 0;
      server.listen((request) async {
        requests += 1;
        request.response.add(requests == 1 ? const [9, 9] : expected);
        await request.response.close();
      });

      await expectLater(
        cacheVerifiedNativeNatsArchive(
          source: _serverUri(server),
          destination: fixture.archive,
          expectedSha256: _digest(expected),
          timeout: const Duration(seconds: 2),
        ),
        throwsA(
          isA<StateError>().having(
            (error) => error.message,
            "message",
            contains("archive digest mismatch"),
          ),
        ),
      );

      expect(fixture.archive.existsSync(), isFalse);
      expect(fixture.files, isEmpty);

      final actualDigest = await cacheVerifiedNativeNatsArchive(
        source: _serverUri(server),
        destination: fixture.archive,
        expectedSha256: _digest(expected),
        timeout: const Duration(seconds: 2),
      );

      expect(actualDigest, _digest(expected));
      expect(await fixture.archive.readAsBytes(), expected);
      expect(requests, 2);
      expect(fixture.files, [fixture.archive.path]);
    });

    test("uses a valid cached archive without contacting the server", () async {
      final fixture = await _Fixture.create();
      addTearDown(fixture.close);
      final expected = Uint8List.fromList(const [1, 2, 3, 4]);
      await fixture.archive.writeAsBytes(expected);
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(() => server.close(force: true));
      var requests = 0;
      server.listen((request) async {
        requests += 1;
        request.response.statusCode = HttpStatus.internalServerError;
        await request.response.close();
      });

      final actualDigest = await cacheVerifiedNativeNatsArchive(
        source: _serverUri(server),
        destination: fixture.archive,
        expectedSha256: _digest(expected),
        timeout: const Duration(seconds: 2),
      );

      expect(actualDigest, _digest(expected));
      expect(requests, 0);
      expect(fixture.files, [fixture.archive.path]);
    });
  });
}

Uri _serverUri(HttpServer server) =>
    Uri.parse("http://${server.address.address}:${server.port}/archive");

String _digest(List<int> bytes) => sha256.convert(bytes).toString();

final class _Fixture {
  const _Fixture._(this.root, this.archive);

  final File archive;
  final Directory root;

  List<String> get files =>
      root.listSync().whereType<File>().map((file) => file.path).toList()
        ..sort();

  static Future<_Fixture> create() async {
    final root = await Directory.systemTemp.createTemp("nats-native-cache-");
    return _Fixture._(root, File("${root.path}/nats-server.tar.gz"));
  }

  Future<void> close() => root.delete(recursive: true);
}
