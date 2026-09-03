@TestOn("vm")
library;

import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:test/test.dart";

void main() {
  final explicitUrl = Platform.environment["NATS_TLS_URL"];
  final firstUrl = Platform.environment["NATS_TLS_FIRST_URL"];
  final certificatePath = Platform.environment["NATS_CA"];
  final mutualTlsUrl = Platform.environment["NATS_MTLS_URL"];
  final mutualTlsCaPath = Platform.environment["NATS_MTLS_CA"];
  final clientCertificatePath = Platform.environment["NATS_MTLS_CLIENT_CERT"];
  final clientKeyPath = Platform.environment["NATS_MTLS_CLIENT_KEY"];
  final outsiderCertificatePath =
      Platform.environment["NATS_MTLS_OUTSIDER_CERT"];
  final outsiderKeyPath = Platform.environment["NATS_MTLS_OUTSIDER_KEY"];
  final skipReason =
      explicitUrl == null ||
          firstUrl == null ||
          certificatePath == null ||
          mutualTlsUrl == null ||
          mutualTlsCaPath == null ||
          clientCertificatePath == null ||
          clientKeyPath == null ||
          outsiderCertificatePath == null ||
          outsiderKeyPath == null
      ? "TLS integration environment is not configured"
      : false;

  test(
    "explicit TLS verifies trust and exchanges messages",
    () async {
      final certificate = await File(certificatePath!).readAsBytes();
      await _exercise(
        NatsServer.parse(explicitUrl!),
        NatsTlsOptions(trustedCertificates: certificate),
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "TLS first verifies trust and exchanges messages",
    () async {
      final uri = Uri.parse(firstUrl!);
      final certificate = await File(certificatePath!).readAsBytes();
      await _exercise(
        NatsServer.tlsFirst(uri.host, port: uri.port),
        NatsTlsOptions(trustedCertificates: certificate),
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "untrusted certificate fails with a typed connection error",
    () async {
      await expectLater(
        NatsConnection.connect(
          NatsOptions(servers: [NatsServer.parse(explicitUrl!)]),
        ),
        throwsA(isA<NatsConnectionException>()),
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "mutual TLS accepts a client certificate signed by the configured CA",
    () async {
      await _exercise(
        NatsServer.parse(mutualTlsUrl!),
        await _mutualTlsOptions(
          mutualTlsCaPath!,
          clientCertificatePath!,
          clientKeyPath!,
        ),
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "mutual TLS rejects a missing client certificate",
    () async {
      final ca = await File(mutualTlsCaPath!).readAsBytes();
      await expectLater(
        NatsConnection.connect(
          NatsOptions(
            servers: [NatsServer.parse(mutualTlsUrl!)],
            tls: NatsTlsOptions(trustedCertificates: ca),
            reconnect: NatsReconnectPolicy(maximumAttempts: 0),
          ),
        ),
        throwsA(isA<NatsConnectionException>()),
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "mutual TLS rejects a client certificate signed by another CA",
    () async {
      await expectLater(
        NatsConnection.connect(
          NatsOptions(
            servers: [NatsServer.parse(mutualTlsUrl!)],
            tls: await _mutualTlsOptions(
              mutualTlsCaPath!,
              outsiderCertificatePath!,
              outsiderKeyPath!,
            ),
            reconnect: NatsReconnectPolicy(maximumAttempts: 0),
          ),
        ),
        throwsA(isA<NatsConnectionException>()),
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}

Future<NatsTlsOptions> _mutualTlsOptions(
  String caPath,
  String certificatePath,
  String keyPath,
) async => NatsTlsOptions(
  trustedCertificates: await File(caPath).readAsBytes(),
  clientCertificateChain: await File(certificatePath).readAsBytes(),
  clientPrivateKey: await File(keyPath).readAsBytes(),
);

Future<void> _exercise(NatsServer server, NatsTlsOptions tls) async {
  final connection = await NatsConnection.connect(
    NatsOptions(servers: [server], tls: tls),
  );
  final subject = "tls.${DateTime.now().microsecondsSinceEpoch}";
  final subscription = await connection.subscribe(subject);
  final received = subscription.messages.first;

  await connection.publish(subject, const [0, 255, 13, 10]);
  await connection.flush();

  expect((await received.timeout(const Duration(seconds: 2))).payload, [
    0,
    255,
    13,
    10,
  ]);
  await subscription.unsubscribe();
  await connection.close();
}
