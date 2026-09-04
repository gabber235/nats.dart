import "dart:async";

import "package:nats_core/nats_core.dart";
import "package:test/test.dart";

const _seed = "SUAKYRHVIOREXV7EUZTBHUHL7NUMHPMAS7QMDU3GTIUWEI5LDNOXD43IZY";
const _publicKey = "UD466L6EBCM3YY5HEGHJANNTN4LSKTSUXTH7RILHCKEQMQHTBNLHJJXT";

void main() {
  test("NKey matches the official seed and public key vector", () async {
    final authentication = NatsAuthentication.nkey(_seed);

    final credentials = await authentication.authenticate(
      _context(nonce: "challenge"),
    );

    expect(credentials.nkey, _publicKey);
    expect(credentials.signature, isNotEmpty);
    expect(credentials.jwt, isNull);
  });

  test("JWT signing can combine Typewriter connection fields", () async {
    final authentication = NatsAuthentication.nkey(
      _seed,
      jwt: "header.payload.signature",
      username: "entity",
      password: "secret",
      connectNkey: "encoded-qualifier",
      extensions: const {"custom": true},
    );

    final credentials = await authentication.authenticate(
      _context(nonce: "challenge"),
    );
    final fields = credentials.toConnectFields();

    expect(fields["jwt"], "header.payload.signature");
    expect(fields["nkey"], "encoded-qualifier");
    expect(fields["user"], "entity");
    expect(fields["pass"], "secret");
    expect(fields["sig"], isNotEmpty);
    expect(fields["custom"], isTrue);
  });

  test("NKey authentication requires a server nonce", () async {
    final authentication = NatsAuthentication.nkey(_seed);

    await expectLater(
      authentication.authenticate(_context()),
      throwsA(
        isA<NatsAuthenticationException>().having(
          (error) => error.message,
          "message",
          "Server did not provide an authentication nonce",
        ),
      ),
    );
  });

  test("deployment qualifier permits a missing server nonce", () async {
    final authentication = NatsAuthentication.nkey(
      _seed,
      jwt: "header.payload.signature",
      username: "entity",
      password: "secret",
      connectNkey: "encoded-qualifier",
    );

    final credentials = await authentication.authenticate(_context());
    final fields = credentials.toConnectFields();

    expect(fields["jwt"], "header.payload.signature");
    expect(fields["nkey"], "encoded-qualifier");
    expect(fields["user"], "entity");
    expect(fields["pass"], "secret");
    expect(fields, isNot(contains("sig")));
  });

  test("credentials reject conflicting and reserved fields", () {
    expect(
      () =>
          NatsConnectCredentials(token: "token", username: "u", password: "p"),
      throwsA(isA<NatsAuthenticationException>()),
    );
    expect(
      () => NatsConnectCredentials(extensions: const {"nkey": "override"}),
      throwsA(isA<NatsAuthenticationException>()),
    );
    expect(
      () => NatsConnectCredentials(extensions: {"invalid": Object()}),
      throwsA(isA<NatsAuthenticationException>()),
    );
  });

  test("CONNECT extensions cannot shadow client owned fields by case", () {
    const reserved = <String>[
      "Verbose",
      "Pedantic",
      "TLS_REQUIRED",
      "Name",
      "Lang",
      "Version",
      "Protocol",
      "Echo",
      "Headers",
      "No_Responders",
      "User",
      "Pass",
      "Auth_Token",
      "Jwt",
      "NKey",
      "Sig",
    ];
    for (final key in reserved) {
      expect(
        () => NatsConnectCredentials(extensions: {key: true}),
        throwsA(isA<NatsAuthenticationException>()),
        reason: key,
      );
    }
    expect(
      () => NatsConnectCredentials(
        extensions: const {"CustomField": true, "customfield": false},
      ),
      throwsA(isA<NatsAuthenticationException>()),
    );

    final credentials = NatsConnectCredentials(
      extensions: const {"CustomField": true},
    );
    expect(credentials.extensions, const {"CustomField": true});
  });

  test("explicit CONNECT credential text requires exact UTF 8", () {
    final invalidValues = <NatsConnectCredentials Function(String)>[
      (value) => NatsConnectCredentials(username: value, password: "valid"),
      (value) => NatsConnectCredentials(username: "valid", password: value),
      (value) => NatsConnectCredentials(token: value),
      (value) => NatsConnectCredentials(jwt: value),
      (value) => NatsConnectCredentials(nkey: value),
      (value) => NatsConnectCredentials(signature: value),
    ];
    for (final create in invalidValues) {
      for (final invalid in const ["\uD800", "\uDC00"]) {
        expect(
          () => create(invalid),
          throwsA(isA<NatsAuthenticationException>()),
        );
      }
    }

    final credentials = NatsConnectCredentials(
      username: "user\u{1F680}",
      password: "pass\u{1F680}",
    );
    expect(credentials.username, "user\u{1F680}");
    expect(credentials.password, "pass\u{1F680}");
    expect(
      NatsConnectCredentials(token: "token\u{1F680}").token,
      "token\u{1F680}",
    );
  });

  test("credentials deeply snapshot extension JSON", () {
    final nested = <String, Object?>{"enabled": true};
    final values = <Object?>[nested];
    final extensions = <String, Object?>{"values": values};

    final credentials = NatsConnectCredentials(extensions: extensions);
    nested["enabled"] = false;
    values.add("changed");
    extensions["new"] = true;

    final fields = credentials.toConnectFields();
    final snapshotValues = fields["values"]! as List<Object?>;
    final snapshotNested = snapshotValues.single as Map<String, Object?>;
    expect(snapshotNested["enabled"], isTrue);
    expect(snapshotValues, hasLength(1));
    expect(fields, isNot(contains("new")));
    expect(() => snapshotValues.add("new"), throwsUnsupportedError);
    expect(() => snapshotNested["enabled"] = false, throwsUnsupportedError);
  });

  test("NKey authentication snapshots extensions before signing", () async {
    final nested = <String, Object?>{"enabled": true};
    final values = <Object?>[nested];
    final extensions = <String, Object?>{"values": values};
    final authentication = NatsAuthentication.nkey(
      _seed,
      extensions: extensions,
    );
    nested["enabled"] = false;
    values.add("changed");
    extensions["new"] = true;

    final credentials = await authentication.authenticate(
      _context(nonce: "challenge"),
    );
    final fields = credentials.toConnectFields();
    final snapshotValues = fields["values"]! as List<Object?>;
    final snapshotNested = snapshotValues.single as Map<String, Object?>;
    expect(snapshotNested["enabled"], isTrue);
    expect(snapshotValues, hasLength(1));
    expect(fields, isNot(contains("new")));
  });

  test("credential diagnostics are redacted", () {
    final credentials = NatsConnectCredentials(
      username: "user",
      password: "highly-secret-value",
    );

    expect(credentials.toString(), isNot(contains("highly-secret-value")));
  });

  test("authentication context deeply snapshots server information", () {
    final leaf = <String, Object?>{"enabled": true};
    final servers = <Object?>["nats.example:4222", leaf];
    final source = <String, Object?>{
      "cluster": <String, Object?>{"servers": servers},
    };

    final context = _context(serverInfo: source);
    leaf["enabled"] = false;
    servers.add("nats.backup:4222");
    source["cluster"] = "changed";

    final cluster = context.serverInfo["cluster"]! as Map<String, Object?>;
    final snapshotServers = cluster["servers"]! as List<Object?>;
    final snapshotLeaf = snapshotServers[1]! as Map<String, Object?>;
    expect(snapshotServers, hasLength(2));
    expect(snapshotLeaf["enabled"], isTrue);
    expect(() => context.serverInfo["new"] = true, throwsUnsupportedError);
    expect(() => snapshotServers.add("new"), throwsUnsupportedError);
    expect(() => snapshotLeaf["enabled"] = false, throwsUnsupportedError);
  });

  test("authentication extensions reject excessive JSON nesting", () {
    expect(
      () => NatsConnectCredentials(extensions: {"nested": _nestedLists(66)}),
      throwsA(isA<NatsAuthenticationException>()),
    );
  });

  test("invalid authentication extensions do not render credentials", () {
    const secret = "highly-secret-value";
    final invalid = <String, Object?>{
      "custom_token": secret,
      "invalid": Object(),
    };
    final cyclic = <String, Object?>{"custom_token": secret};
    cyclic["cycle"] = cyclic;

    for (final create in <Object? Function()>[
      () => NatsConnectCredentials(extensions: invalid),
      () => NatsConnectCredentials(extensions: cyclic),
      () => NatsAuthentication.nkey(_seed, extensions: invalid),
      () => NatsAuthentication.nkey(_seed, extensions: cyclic),
    ]) {
      Object? failure;
      try {
        create();
      } on Object catch (error) {
        failure = error;
      }

      expect(failure, isA<NatsAuthenticationException>());
      expect(failure.toString(), isNot(contains(secret)));
    }
  });

  test("credentials content parses JWT and user seed blocks", () async {
    final authentication = NatsAuthentication.credentials(
      """
-----BEGIN NATS USER JWT-----
header.payload.signature
------END NATS USER JWT------

************************* IMPORTANT *************************

-----BEGIN USER NKEY SEED-----
$_seed
------END USER NKEY SEED------
"""
          .replaceAll("\n", "\r\n"),
    );

    final credentials = await authentication.authenticate(
      _context(nonce: "challenge"),
    );

    expect(credentials.jwt, "header.payload.signature");
    expect(credentials.nkey, isNull);
    expect(credentials.signature, isNotEmpty);
    expect(authentication.toString(), isNot(contains(_seed)));
  });

  test("credentials content requires exactly one complete block pair", () {
    const jwt = """
-----BEGIN NATS USER JWT-----
header.payload.signature
------END NATS USER JWT------
""";
    const seed =
        """
-----BEGIN USER NKEY SEED-----
$_seed
------END USER NKEY SEED------
""";

    expect(
      () => NatsAuthentication.credentials(jwt),
      throwsA(isA<NatsAuthenticationException>()),
    );
    expect(
      () => NatsAuthentication.credentials("$jwt$jwt$seed"),
      throwsA(isA<NatsAuthenticationException>()),
    );
    expect(
      () => NatsAuthentication.credentials("$jwt$seed$seed"),
      throwsA(isA<NatsAuthenticationException>()),
    );
  });
}

NatsAuthContext _context({
  Map<String, Object?> serverInfo = const {},
  String? nonce,
}) => NatsAuthContext(
  serverInfo: serverInfo,
  nonce: nonce,
  deadline: DateTime.timestamp().add(const Duration(seconds: 1)),
  cancelled: Completer<void>().future,
);

Object? _nestedLists(int count) {
  Object? value = true;
  for (var index = 0; index < count; index += 1) {
    value = <Object?>[value];
  }
  return value;
}
