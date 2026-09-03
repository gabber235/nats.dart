@TestOn("vm")
library;

import "dart:async";
import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:test/test.dart";

const _seed = "SUAKYRHVIOREXV7EUZTBHUHL7NUMHPMAS7QMDU3GTIUWEI5LDNOXD43IZY";

void main() {
  test("credentials file loads without exposing path or content", () async {
    final directory = await Directory.systemTemp.createTemp("nats-creds-");
    addTearDown(() => directory.delete(recursive: true));
    final file = File("${directory.path}/user.creds");
    await file.writeAsString("""
-----BEGIN NATS USER JWT-----
header.payload.signature
------END NATS USER JWT------
-----BEGIN USER NKEY SEED-----
$_seed
------END USER NKEY SEED------
""");

    final authentication = await loadNatsCredentialsFile(file.path);
    final resolved = await authentication.authenticate(
      NatsAuthContext(
        serverInfo: const {},
        nonce: "challenge",
        deadline: DateTime.timestamp().add(const Duration(seconds: 1)),
        cancelled: Completer<void>().future,
      ),
    );

    expect(resolved.jwt, "header.payload.signature");
    expect(resolved.signature, isNotEmpty);
  });

  test("credentials file preserves read failure cause", () async {
    final missing =
        "${Directory.systemTemp.path}/missing-nats-creds-${DateTime.now().microsecondsSinceEpoch}";

    await expectLater(
      loadNatsCredentialsFile(missing),
      throwsA(
        isA<NatsAuthenticationException>().having(
          (error) => error.cause,
          "cause",
          isA<FileSystemException>(),
        ),
      ),
    );
  });
}
