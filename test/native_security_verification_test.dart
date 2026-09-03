import "dart:io";

import "package:test/test.dart";

import "../tool/run_native_security_verification.dart";

void main() {
  test("repository contains every native security fixture", () {
    expect(
      () => validateNativeSecurityFixtures(
        Directory("tool/native_security_fixtures"),
      ),
      returnsNormally,
    );
  });

  test("missing fixtures fail with every absent file", () async {
    final directory = await Directory.systemTemp.createTemp(
      "nats-native-security-missing-",
    );
    addTearDown(() => directory.delete(recursive: true));

    expect(
      () => validateNativeSecurityFixtures(directory),
      throwsA(
        isA<StateError>().having(
          (error) => error.message,
          "message",
          allOf(
            contains("ca.pem"),
            contains("server.key"),
            contains("user.creds"),
          ),
        ),
      ),
    );
  });

  test("NATS config paths quote and normalize Windows separators", () {
    expect(
      natsConfigPath(r"C:\fixtures\native test\server.pem"),
      r'"C:/fixtures/native test/server.pem"',
    );
  });

  test("WebSocket security material must be complete", () {
    expect(
      () => webSocketServerConfig(port: 8443, certificate: "server.pem"),
      throwsArgumentError,
    );
    expect(
      () => webSocketServerConfig(port: 8443, key: "server.key"),
      throwsArgumentError,
    );
  });
}
