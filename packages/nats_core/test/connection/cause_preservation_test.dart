@TestOn("vm")
library;

import "dart:convert";
import "dart:io";
import "dart:typed_data";

import "package:nats_core/nats_core.dart";
import "package:test/test.dart";

void main() {
  test(
    "authentication boundary preserves the original cause and stack",
    () async {
      final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
      final peerHandled = listener.first.then((peer) async {
        peer.add(ascii.encode("INFO {\"max_payload\":1048576}\r\n"));
        await peer.flush();
        peer.destroy();
      });
      final sentinel = _SentinelFailure();
      late StackTrace sentinelStack;
      final authentication = NatsAuthentication.provider((_) async {
        try {
          throw sentinel;
        } on _SentinelFailure catch (_, stackTrace) {
          sentinelStack = stackTrace;
          rethrow;
        }
      });

      try {
        NatsAuthenticationException? failure;
        try {
          await NatsConnection.connect(
            NatsOptions(
              servers: [
                NatsServer.nats(
                  InternetAddress.loopbackIPv4.address,
                  port: listener.port,
                ),
              ],
              authentication: authentication,
              reconnect: NatsReconnectPolicy(maximumAttempts: 0),
            ),
          );
        } on NatsAuthenticationException catch (error) {
          failure = error;
        }

        expect(failure, isNotNull);
        expect(failure!.message, "Authentication provider failed");
        expect(failure.message, isNot(contains(sentinel.secret)));
        expect(failure.cause, same(sentinel));
        expect(failure.causeStackTrace, same(sentinelStack));
        await peerHandled;
      } finally {
        await listener.close();
      }
    },
  );

  test("invalid resolved credentials fail before CONNECT is written", () async {
    final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final received = BytesBuilder(copy: false);
    final peerHandled = listener.first.then((peer) async {
      peer.add(ascii.encode("INFO {\"max_payload\":1048576}\r\n"));
      await peer.flush();
      await for (final bytes in peer) {
        received.add(bytes);
      }
    });

    try {
      await expectLater(
        NatsConnection.connect(
          NatsOptions(
            servers: [
              NatsServer.nats(
                InternetAddress.loopbackIPv4.address,
                port: listener.port,
              ),
            ],
            authentication: NatsAuthentication.provider(
              (_) async => NatsConnectCredentials(token: "\uD800"),
            ),
            reconnect: NatsReconnectPolicy(maximumAttempts: 0),
          ),
        ),
        throwsA(isA<NatsAuthenticationException>()),
      );
      await peerHandled.timeout(const Duration(seconds: 1));
      expect(received.takeBytes(), isEmpty);
    } finally {
      await listener.close();
    }
  });

  test("server authentication errors win over adjacent reset", () async {
    await _expectServerAuthenticationFailure(
      "Authorization Violation",
      NatsAuthenticationFailure.authorizationViolation,
    );
    await _expectServerAuthenticationFailure(
      "Authentication Timeout",
      NatsAuthenticationFailure.authenticationTimeout,
    );
  });

  test(
    "authenticated CONNECT close is a typed authorization failure",
    () async {
      final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
      final peerHandled = listener.first.then((peer) async {
        peer.add(ascii.encode("INFO {\"max_payload\":1048576}\r\n"));
        await peer.flush();
        await peer
            .cast<List<int>>()
            .transform(utf8.decoder)
            .transform(const LineSplitter())
            .firstWhere((line) => line.startsWith("CONNECT "));
        peer.destroy();
      });

      try {
        await expectLater(
          NatsConnection.connect(
            NatsOptions(
              servers: [
                NatsServer.nats(
                  InternetAddress.loopbackIPv4.address,
                  port: listener.port,
                ),
              ],
              authentication: NatsAuthentication.userPassword("bad", "bad"),
              reconnect: NatsReconnectPolicy(maximumAttempts: 0),
            ),
          ),
          throwsA(
            isA<NatsAuthenticationException>()
                .having(
                  (error) => error.reason,
                  "reason",
                  NatsAuthenticationFailure.authorizationViolation,
                )
                .having(
                  (error) => error.cause,
                  "cause",
                  isA<NatsConnectionException>(),
                ),
          ),
        );
        await peerHandled;
      } finally {
        await listener.close();
      }
    },
  );
}

Future<void> _expectServerAuthenticationFailure(
  String message,
  NatsAuthenticationFailure reason,
) async {
  final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  final peerHandled = listener.first.then((peer) async {
    peer.add(ascii.encode("INFO {\"max_payload\":1048576}\r\n"));
    await peer.flush();
    await peer
        .cast<List<int>>()
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .firstWhere((line) => line.startsWith("CONNECT "));
    peer.add(ascii.encode("-ERR '$message'\r\n"));
    await peer.flush();
    peer.destroy();
  });

  try {
    await expectLater(
      NatsConnection.connect(
        NatsOptions(
          servers: [
            NatsServer.nats(
              InternetAddress.loopbackIPv4.address,
              port: listener.port,
            ),
          ],
          authentication: NatsAuthentication.userPassword("bad", "bad"),
          reconnect: NatsReconnectPolicy(maximumAttempts: 0),
        ),
      ),
      throwsA(
        isA<NatsAuthenticationException>()
            .having((error) => error.reason, "reason", reason)
            .having((error) => error.message, "message", message),
      ),
    );
    await peerHandled;
  } finally {
    await listener.close();
  }
}

final class _SentinelFailure implements Exception {
  final String secret = "provider-secret-material";

  @override
  String toString() => secret;
}
