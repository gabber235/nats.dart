import "package:nats_core/nats_core.dart";
import "package:test/test.dart";

void main() {
  test("subscription options validate local queue bounds", () {
    expect(
      () => NatsSubscriptionOptions(maximumMessages: 0),
      throwsArgumentError,
    );
    expect(
      () => NatsSubscriptionOptions(maximumBytes: -1),
      throwsArgumentError,
    );
    expect(
      () => NatsSubscriptionOptions(maximumDeliveries: 0),
      throwsArgumentError,
    );
    expect(NatsSubscriptionOptions(maximumDeliveries: 7).maximumDeliveries, 7);
    expect(
      NatsSubscriptionOptions(
        maximumMessages: 12,
        maximumBytes: 34,
        reconnect: NatsSubscriptionReconnectPolicy.close,
      ).reconnect,
      NatsSubscriptionReconnectPolicy.close,
    );
  });

  group("NatsServer", () {
    test("parses a credential free plain server", () {
      final server = NatsServer.parse("nats://localhost:4333");

      expect(server.scheme, "nats");
      expect(server.host, "localhost");
      expect(server.port, 4333);
      expect(server.toString(), "nats://localhost:4333");
    });

    test("rejects credentials and unsupported schemes", () {
      const secret = "highly-secret-value";
      for (final value in [
        "nats://user:$secret@localhost:4222",
        "ws://localhost/nats?token=$secret",
      ]) {
        Object? failure;
        try {
          NatsServer.parse(value);
        } on Object catch (error) {
          failure = error;
        }

        expect(failure, isA<ArgumentError>());
        expect(failure.toString(), isNot(contains(secret)));
      }
      expect(
        () => NatsServer.parse("http://localhost:443"),
        throwsArgumentError,
      );
    });

    test("WebSocket server URLs preserve secure defaults and paths", () {
      final plain = NatsServer.parse("ws://localhost/nats");
      final secure = NatsServer.parse("wss://localhost/nats");

      expect(plain.scheme, "ws");
      expect(plain.port, 80);
      expect(plain.path, "/nats");
      expect(plain.uri, Uri.parse("ws://localhost:80/nats"));
      expect(secure.scheme, "wss");
      expect(secure.port, 443);
      expect(secure.uri, Uri.parse("wss://localhost:443/nats"));
    });

    test("rejects invalid transport specific URI components", () {
      expect(
        () => NatsServer.parse("nats://localhost:4222/nats"),
        throwsArgumentError,
      );
      expect(
        () => NatsServer.parse("ws://localhost/nats?token=secret"),
        throwsArgumentError,
      );
    });

    test("distinguishes explicit TLS and TLS first", () {
      final explicit = NatsServer.parse("tls://nats.example:4222");
      const first = NatsServer.tlsFirst("nats.example");

      expect(explicit.tlsMode, NatsTlsMode.upgrade);
      expect(first.tlsMode, NatsTlsMode.first);
    });
  });

  group("NatsOptions", () {
    test("snapshots the server list", () {
      final servers = [const NatsServer.nats("localhost")];
      final options = NatsOptions(servers: servers);
      servers.clear();

      expect(options.servers, hasLength(1));
    });

    test("validates directly constructed server endpoints", () {
      for (final server in [
        const NatsServer.nats(""),
        const NatsServer.nats("bad host"),
        const NatsServer.nats("host/name"),
        const NatsServer.nats("localhost", port: 0),
        const NatsServer.nats("localhost", port: 65536),
        const NatsServer.websocket("localhost", path: "nats"),
        const NatsServer.websocket("localhost", path: "/nats?secret=yes"),
      ]) {
        expect(() => NatsOptions(servers: [server]), throwsArgumentError);
      }

      expect(
        NatsOptions(
          servers: const [
            NatsServer.nats("127.0.0.1"),
            NatsServer.nats("::1"),
            NatsServer.nats("nats.example"),
            NatsServer.websocket("nats.example", path: "/nats"),
          ],
        ).servers,
        hasLength(4),
      );
    });

    test("rejects credentials in every direct server constructor", () {
      const secret = "highly-secret-value";
      final servers = <NatsServer>[
        const NatsServer.nats("user:$secret@nats.example"),
        const NatsServer.tls("user:$secret@nats.example"),
        const NatsServer.tlsFirst("user:$secret@nats.example"),
        const NatsServer.websocket("user:$secret@nats.example"),
        const NatsServer.secureWebSocket("user:$secret@nats.example"),
      ];

      for (final server in servers) {
        Object? failure;
        try {
          NatsOptions(servers: [server]);
        } on Object catch (error) {
          failure = error;
        }

        expect(failure, isA<ArgumentError>());
        expect(failure.toString(), isNot(contains(secret)));
      }

      Object? pathFailure;
      try {
        NatsOptions(
          servers: const [
            NatsServer.websocket("nats.example", path: "/nats?token=$secret"),
          ],
        );
      } on Object catch (error) {
        pathFailure = error;
      }
      expect(pathFailure, isA<ArgumentError>());
      expect(pathFailure.toString(), isNot(contains(secret)));

      expect(
        NatsOptions(servers: const [NatsServer.nats("::1")]).servers,
        hasLength(1),
      );
    });

    test("snapshots TLS certificate bytes", () {
      final certificate = [1, 2, 3];
      final tls = NatsTlsOptions(trustedCertificates: certificate);
      certificate[0] = 99;

      expect(tls.trustedCertificates, [1, 2, 3]);
      expect(() => tls.trustedCertificates![0] = 99, throwsUnsupportedError);
      expect(
        () => NatsTlsOptions(trustedCertificates: const [-1]),
        throwsFormatException,
      );
      expect(
        () => NatsTlsOptions(trustedCertificates: const [256]),
        throwsFormatException,
      );
    });

    test("snapshots and validates native WebSocket headers", () {
      final headers = {"X-Nats-Test": "present"};
      final webSocket = NatsWebSocketOptions(headers: headers);
      headers["X-Nats-Test"] = "changed";

      expect(webSocket.headers, {"X-Nats-Test": "present"});
      expect(
        () => webSocket.headers["X-Nats-Test"] = "changed",
        throwsUnsupportedError,
      );
      expect(
        () => NatsWebSocketOptions(headers: {"X-Test": "value\r\ninjected"}),
        throwsArgumentError,
      );
      expect(
        () => NatsWebSocketOptions(headers: const {"Bad Header": "value"}),
        throwsArgumentError,
      );
      Object? headerFailure;
      try {
        NatsWebSocketOptions(
          headers: const {
            "Authorization": "Bearer highly-secret-value",
            "Bad Header": "value",
          },
        );
      } on Object catch (error) {
        headerFailure = error;
      }
      expect(headerFailure, isA<ArgumentError>());
      expect(headerFailure.toString(), isNot(contains("highly-secret-value")));
      expect(
        () => NatsWebSocketOptions(headers: const {"Upgrade": "other"}),
        throwsArgumentError,
      );
      expect(
        () => NatsWebSocketOptions(headers: const {"X-Test": "bad\u0000"}),
        throwsArgumentError,
      );
      for (final value in ["\u0080", "é", "\uD800"]) {
        expect(
          () => NatsWebSocketOptions(headers: {"X-Test": value}),
          throwsArgumentError,
          reason: value.codeUnits.toString(),
        );
      }
      expect(
        NatsWebSocketOptions(
          headers: const {"X-Test": "printable ASCII\tvalue"},
        ).headers,
        const {"X-Test": "printable ASCII\tvalue"},
      );
    });

    test("rejects absent servers and invalid bounds", () {
      expect(() => NatsOptions(servers: const []), throwsArgumentError);
      expect(
        () => NatsOptions(
          servers: const [NatsServer.nats("localhost")],
          subscriptionQueueMessages: 0,
        ),
        throwsArgumentError,
      );
      expect(
        () => NatsOptions(
          servers: const [NatsServer.nats("localhost")],
          pingInterval: Duration.zero,
        ),
        throwsArgumentError,
      );
      expect(
        () => NatsOptions(
          servers: const [NatsServer.nats("localhost")],
          maximumOutstandingPings: 0,
        ),
        throwsArgumentError,
      );
    });

    test("preserves explicit keepalive bounds", () {
      final options = NatsOptions(
        servers: const [NatsServer.nats("localhost")],
        pingInterval: const Duration(seconds: 3),
        maximumOutstandingPings: 4,
      );

      expect(options.pingInterval, const Duration(seconds: 3));
      expect(options.maximumOutstandingPings, 4);
    });

    test("validates reconnect server selection delay", () {
      expect(
        () => NatsReconnectServerSelection(
          server: const NatsServer.nats("localhost"),
          delay: const Duration(microseconds: -1),
        ),
        throwsArgumentError,
      );
      final selection = NatsReconnectServerSelection(
        server: const NatsServer.nats("localhost"),
        delay: const Duration(milliseconds: 25),
      );

      expect(selection.server.host, "localhost");
      expect(selection.delay, const Duration(milliseconds: 25));
    });

    test("authentication retries require explicit reconnect opt in", () {
      expect(NatsReconnectPolicy().retryAuthenticationFailures, isFalse);
      expect(
        NatsReconnectPolicy(
          retryAuthenticationFailures: true,
        ).retryAuthenticationFailures,
        isTrue,
      );
    });

    test("preserves explicit echo behavior", () {
      final options = NatsOptions(
        servers: const [NatsServer.nats("localhost")],
        echo: false,
      );

      expect(options.echo, isFalse);
    });

    test("preserves and validates the inbound payload memory ceiling", () {
      final options = NatsOptions(
        servers: const [NatsServer.nats("localhost")],
        maximumInboundPayloadBytes: 12 * 1024 * 1024,
      );

      expect(options.maximumInboundPayloadBytes, 12 * 1024 * 1024);
      expect(
        () => NatsOptions(
          servers: const [NatsServer.nats("localhost")],
          maximumInboundPayloadBytes: 0,
        ),
        throwsArgumentError,
      );
    });

    test("preserves and validates the outbound control line ceiling", () {
      final options = NatsOptions(
        servers: const [NatsServer.nats("localhost")],
        maximumOutboundControlLineBytes: 8192,
      );

      expect(options.maximumOutboundControlLineBytes, 8192);
      for (final value in [63, 1024 * 1024 + 1]) {
        expect(
          () => NatsOptions(
            servers: const [NatsServer.nats("localhost")],
            maximumOutboundControlLineBytes: value,
          ),
          throwsArgumentError,
        );
      }
      expect(
        () => NatsOptions(
          servers: const [NatsServer.nats("localhost")],
          name: "n" * 65,
          maximumOutboundControlLineBytes: 64,
        ),
        throwsArgumentError,
      );
      expect(
        () => NatsOptions(
          servers: const [NatsServer.nats("localhost")],
          name: String.fromCharCode(0xd800),
        ),
        throwsArgumentError,
      );
    });
  });
}
