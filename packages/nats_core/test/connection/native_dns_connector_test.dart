@TestOn("vm")
library;

import "dart:async";
import "dart:io";
import "dart:typed_data";

import "package:nats_core/nats_core.dart";
import "package:nats_core/src/transport/connector_io.dart";
import "package:nats_core/src/transport/transport.dart";
import "package:test/test.dart";

void main() {
  test("connection preserves aggregate native candidate failure", () async {
    final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final port = listener.port;
    await listener.close();

    await expectLater(
      NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.nats("127.0.0.1", port: port)],
          connectTimeout: const Duration(seconds: 1),
          reconnect: NatsReconnectPolicy(maximumAttempts: 0),
        ),
      ),
      throwsA(
        isA<NatsConnectCandidatesException>()
            .having((error) => error.host, "host", "127.0.0.1")
            .having((error) => error.port, "port", port)
            .having((error) => error.failures, "failures", hasLength(1)),
      ),
    );
  });

  test("connection preserves DNS failure", () async {
    await expectLater(
      NatsConnection.connect(
        NatsOptions(
          servers: [const NatsServer.nats("nats-does-not-exist.invalid")],
          connectTimeout: const Duration(seconds: 2),
          reconnect: NatsReconnectPolicy(maximumAttempts: 0),
        ),
      ),
      throwsA(
        isA<NatsDnsException>().having(
          (error) => error.host,
          "host",
          "nats-does-not-exist.invalid",
        ),
      ),
    );
  });

  test("native DNS tries every rotated candidate within one budget", () async {
    final first = InternetAddress("192.0.2.1");
    final second = InternetAddress("192.0.2.2");
    final attempted = <String>[];
    final budgets = <Duration>[];
    final transport = _FakeTransport();

    final connected = await connectNativeTransport(
      const NatsServer.nats("nats.example"),
      NatsTlsOptions(),
      const Duration(seconds: 2),
      resolver: (_) async => [first, second],
      orderer: (addresses) => addresses.reversed.toList(),
      connector: (server, address, tls, timeout) async {
        expect(server.host, "nats.example");
        attempted.add(address.address);
        budgets.add(timeout);
        if (address.address == second.address) {
          throw const SocketException("candidate unavailable");
        }
        return transport;
      },
    );

    expect(connected, same(transport));
    expect(attempted, [second.address, first.address]);
    expect(budgets, hasLength(2));
    expect(budgets.first, lessThan(const Duration(seconds: 2)));
    expect(budgets.every((value) => value > Duration.zero), isTrue);
  });

  test(
    "native DNS aggregates candidate causes without rendering them",
    () async {
      final secretCause = StateError("credential material must stay hidden");
      final addresses = [
        InternetAddress("192.0.2.10"),
        InternetAddress("2001:db8::10"),
      ];

      final failure = await _candidateFailure(
        connectNativeTransport(
          const NatsServer.nats("nats.example", port: 4333),
          NatsTlsOptions(),
          const Duration(seconds: 2),
          resolver: (_) async => addresses,
          orderer: (values) => values,
          connector: (_, _, _, _) async => throw secretCause,
        ),
      );

      expect(failure.host, "nats.example");
      expect(failure.port, 4333);
      expect(
        failure.failures.map((value) => value.address),
        addresses.map((value) => value.address),
      );
      expect(
        failure.failures.every((value) => identical(value.cause, secretCause)),
        isTrue,
      );
      expect(failure.toString(), isNot(contains("credential material")));
      expect(
        () => failure.failures.add(
          NatsConnectCandidateFailure(
            "127.0.0.1",
            secretCause,
            StackTrace.empty,
          ),
        ),
        throwsUnsupportedError,
      );
    },
  );

  test("native DNS lookup obeys the attempt deadline", () async {
    final unresolved = Completer<List<InternetAddress>>();

    await expectLater(
      connectNativeTransport(
        const NatsServer.nats("unresolved.example"),
        NatsTlsOptions(),
        const Duration(milliseconds: 25),
        resolver: (_) => unresolved.future,
      ),
      throwsA(
        isA<NatsDnsException>()
            .having((error) => error.host, "host", "unresolved.example")
            .having((error) => error.cause, "cause", isA<TimeoutException>()),
      ),
    );
  });

  test("resolved TLS candidate retains the configured hostname", () async {
    final address = InternetAddress("192.0.2.20");
    final transport = _FakeTransport();

    await connectNativeTransport(
      const NatsServer.tlsFirst("tls.example"),
      NatsTlsOptions(),
      const Duration(seconds: 1),
      resolver: (_) async => [address],
      connector: (server, candidate, tls, timeout) async {
        expect(server.host, "tls.example");
        expect(server.tlsMode, NatsTlsMode.first);
        expect(candidate, address);
        return transport;
      },
    );
  });

  test("native connector reaches an IPv4 candidate", () async {
    await _expectRealConnection(InternetAddress.loopbackIPv4);
  });

  test("native connector reaches an IPv6 candidate when supported", () async {
    ServerSocket listener;
    try {
      listener = await ServerSocket.bind(InternetAddress.loopbackIPv6, 0);
    } on SocketException {
      markTestSkipped("IPv6 loopback is unavailable");
      return;
    }
    await _expectRealConnection(
      InternetAddress.loopbackIPv6,
      listener: listener,
    );
  });
}

Future<NatsConnectCandidatesException> _candidateFailure(
  Future<NatsTransport> connection,
) async {
  try {
    await connection;
  } on NatsConnectCandidatesException catch (error) {
    return error;
  }
  throw StateError("Native candidate connection unexpectedly succeeded");
}

Future<void> _expectRealConnection(
  InternetAddress address, {
  ServerSocket? listener,
}) async {
  final server = listener ?? await ServerSocket.bind(address, 0);
  final accepted = server.first.timeout(const Duration(seconds: 2));
  NatsTransport? transport;
  Future<void>? incomingDone;
  Socket? peer;
  try {
    transport = await connectNativeTransport(
      NatsServer.nats("localhost", port: server.port),
      NatsTlsOptions(),
      const Duration(seconds: 2),
      resolver: (_) async => [address],
      orderer: (addresses) => addresses,
    );
    incomingDone = transport.incoming.drain<void>();
    peer = await accepted;
    expect(peer.remoteAddress.type, address.type);
  } finally {
    peer?.destroy();
    await incomingDone?.timeout(const Duration(seconds: 2));
    await transport?.close().timeout(const Duration(seconds: 2));
    await server.close().timeout(const Duration(seconds: 2));
  }
}

final class _FakeTransport implements NatsTransport {
  final StreamController<Uint8List> _incoming = StreamController();

  @override
  Stream<Uint8List> get incoming => _incoming.stream;

  @override
  void add(Uint8List bytes) {}

  @override
  Future<void> flush() async {}

  @override
  Future<void> startTls({
    required String host,
    required NatsTlsOptions options,
    required Duration timeout,
  }) async {}

  @override
  Future<void> close() => _incoming.close();
}
