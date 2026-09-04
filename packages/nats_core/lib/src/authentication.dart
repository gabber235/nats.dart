import "package:nats_core/src/auth/nkey.dart";
import "package:nats_core/src/error.dart";
import "package:nats_core/src/immutable_json.dart";

/// Information supplied to authentication for every handshake.
final class NatsAuthContext {
  /// Creates an immutable handshake context.
  NatsAuthContext({
    required Map<String, Object?> serverInfo,
    required this.nonce,
    required this.deadline,
    required this.cancelled,
  }) : serverInfo = freezeJsonObject(serverInfo);

  /// Forward compatible INFO fields for this server generation.
  final Map<String, Object?> serverInfo;

  /// Server challenge, when supplied.
  final String? nonce;

  /// Absolute deadline for this complete connection attempt.
  final DateTime deadline;

  /// Completes when this attempt no longer accepts provider results.
  ///
  /// Providers should stop external work and release owned resources when this
  /// signal completes. A later credential result is ignored.
  final Future<void> cancelled;
}

/// Resolved CONNECT authentication fields.
final class NatsConnectCredentials {
  /// Creates credentials with explicit optional fields.
  factory NatsConnectCredentials({
    String? username,
    String? password,
    String? token,
    String? jwt,
    String? nkey,
    String? signature,
    Map<String, Object?> extensions = const {},
  }) {
    final validatedUsername = _credentialText(username, "username");
    final validatedPassword = _credentialText(password, "password");
    final validatedToken = _credentialText(token, "token");
    final validatedJwt = _credentialText(jwt, "jwt");
    final validatedNkey = _credentialText(nkey, "nkey");
    final validatedSignature = _credentialText(signature, "signature");
    if (token != null && (username != null || password != null)) {
      throw const NatsAuthenticationException(
        "Token cannot be combined with username or password",
      );
    }
    if ((username == null) != (password == null)) {
      throw const NatsAuthenticationException(
        "Username and password must be supplied together",
      );
    }
    final extensionSnapshot = _freezeAuthenticationExtensions(extensions);
    final normalizedExtensionKeys = <String>{};
    for (final key in extensionSnapshot.keys) {
      final normalizedKey = _asciiLowercase(key);
      if (_reservedConnectKeys.contains(normalizedKey)) {
        throw NatsAuthenticationException(
          "CONNECT extension uses reserved key $key",
        );
      }
      if (!normalizedExtensionKeys.add(normalizedKey)) {
        throw NatsAuthenticationException(
          "CONNECT extensions contain a case insensitive duplicate key $key",
        );
      }
    }
    return NatsConnectCredentials._(
      validatedUsername,
      validatedPassword,
      validatedToken,
      validatedJwt,
      validatedNkey,
      validatedSignature,
      extensionSnapshot,
    );
  }

  const NatsConnectCredentials._(
    this.username,
    this.password,
    this.token,
    this.jwt,
    this.nkey,
    this.signature,
    this.extensions,
  );

  /// Username.
  final String? username;

  /// Password.
  final String? password;

  /// Authentication token.
  final String? token;

  /// User JWT.
  final String? jwt;

  /// Public NKey or deployment specific CONNECT qualifier.
  final String? nkey;

  /// URL safe challenge signature without padding.
  final String? signature;

  /// Additional nonreserved CONNECT fields.
  final Map<String, Object?> extensions;

  /// Returns a new CONNECT field map.
  Map<String, Object?> toConnectFields() => {
    ...extensions,
    "user": ?username,
    "pass": ?password,
    "auth_token": ?token,
    "jwt": ?jwt,
    "nkey": ?nkey,
    "sig": ?signature,
  };

  @override
  String toString() => "NatsConnectCredentials(<redacted>)";
}

/// Resolves fresh authentication for each connection generation.
abstract interface class NatsAuthentication {
  /// Creates username and password authentication.
  factory NatsAuthentication.userPassword(String username, String password) =
      _UserPasswordAuthentication;

  /// Creates token authentication.
  factory NatsAuthentication.token(String token) = _TokenAuthentication;

  /// Creates JWT and NKey authentication from NATS credentials content.
  factory NatsAuthentication.credentials(String contents) {
    final parsed = _parseCredentials(contents);
    return _NKeyAuthentication(parsed.seed, jwt: parsed.jwt);
  }

  /// Creates NKey or JWT challenge response authentication.
  ///
  /// [connectNkey] replaces the public NKey with a deployment specific
  /// qualifier. This mode signs a server nonce when present, but can continue
  /// without one because the qualifier cannot represent the signing key.
  /// Standard NKey and JWT authentication still require a server nonce.
  factory NatsAuthentication.nkey(
    String seed, {
    String? jwt,
    String? username,
    String? password,
    String? connectNkey,
    Map<String, Object?> extensions = const {},
  }) => _NKeyAuthentication(
    seed,
    jwt: jwt,
    username: username,
    password: password,
    connectNkey: connectNkey,
    extensions: extensions,
  );

  /// Creates authentication backed by an asynchronous [provider].
  factory NatsAuthentication.provider(NatsAuthProvider provider) =
      _ProviderAuthentication;

  /// Resolves credentials for one handshake.
  Future<NatsConnectCredentials> authenticate(NatsAuthContext context);
}

/// Callback resolving credentials for one handshake.
typedef NatsAuthProvider =
    Future<NatsConnectCredentials> Function(NatsAuthContext context);

final class _UserPasswordAuthentication implements NatsAuthentication {
  const _UserPasswordAuthentication(this.username, this.password);

  final String username;
  final String password;

  @override
  Future<NatsConnectCredentials> authenticate(NatsAuthContext context) async =>
      NatsConnectCredentials(username: username, password: password);
}

final class _TokenAuthentication implements NatsAuthentication {
  const _TokenAuthentication(this.token);

  final String token;

  @override
  Future<NatsConnectCredentials> authenticate(NatsAuthContext context) async =>
      NatsConnectCredentials(token: token);
}

final class _NKeyAuthentication implements NatsAuthentication {
  _NKeyAuthentication(
    String seed, {
    this.jwt,
    this.username,
    this.password,
    this.connectNkey,
    Map<String, Object?> extensions = const {},
  }) : extensions = _freezeAuthenticationExtensions(extensions),
       _signer = NKeySigner.fromSeed(seed);

  final NKeySigner _signer;
  final String? jwt;
  final String? username;
  final String? password;
  final String? connectNkey;
  final Map<String, Object?> extensions;

  @override
  Future<NatsConnectCredentials> authenticate(NatsAuthContext context) async {
    final nonce = context.nonce;
    if (nonce == null || nonce.isEmpty) {
      if (connectNkey != null) {
        return NatsConnectCredentials(
          username: username,
          password: password,
          jwt: jwt,
          nkey: connectNkey,
          extensions: extensions,
        );
      }
      throw const NatsAuthenticationException(
        "Server did not provide an authentication nonce",
      );
    }
    final publicKey = await _signer.publicKey();
    final signature = await _signer.sign(nonce);
    return NatsConnectCredentials(
      username: username,
      password: password,
      jwt: jwt,
      nkey: connectNkey ?? (jwt == null ? publicKey : null),
      signature: signature,
      extensions: extensions,
    );
  }

  @override
  String toString() => "NatsAuthentication(<redacted>)";
}

Map<String, Object?> _freezeAuthenticationExtensions(
  Map<String, Object?> extensions,
) {
  try {
    return freezeJsonObject(extensions);
  } on Object catch (error, stackTrace) {
    throw NatsAuthenticationException(
      "CONNECT extensions are invalid",
      cause: error,
      causeStackTrace: stackTrace,
    );
  }
}

final class _ProviderAuthentication implements NatsAuthentication {
  const _ProviderAuthentication(this.provider);

  final NatsAuthProvider provider;

  @override
  Future<NatsConnectCredentials> authenticate(NatsAuthContext context) =>
      provider(context);
}

const _reservedConnectKeys = {
  "verbose",
  "pedantic",
  "tls_required",
  "name",
  "lang",
  "version",
  "protocol",
  "echo",
  "headers",
  "no_responders",
  "user",
  "pass",
  "auth_token",
  "jwt",
  "nkey",
  "sig",
};

String? _credentialText(String? value, String field) {
  if (value == null) {
    return null;
  }
  try {
    return exactUtf8String(value, argumentName: field);
  } on ArgumentError {
    throw NatsAuthenticationException(
      "CONNECT credential $field must contain exact UTF 8 text",
    );
  }
}

String _asciiLowercase(String value) {
  final codeUnits = value.codeUnits;
  var changed = false;
  final normalized = List<int>.generate(codeUnits.length, (index) {
    final codeUnit = codeUnits[index];
    if (codeUnit >= 0x41 && codeUnit <= 0x5a) {
      changed = true;
      return codeUnit + 0x20;
    }
    return codeUnit;
  }, growable: false);
  return changed ? String.fromCharCodes(normalized) : value;
}

_ParsedCredentials _parseCredentials(String contents) {
  final jwt = _credentialBlock(contents, "NATS USER JWT");
  final seed = _credentialBlock(contents, "USER NKEY SEED");
  return _ParsedCredentials(jwt, seed);
}

String _credentialBlock(String contents, String label) {
  final pattern = RegExp(
    "-+BEGIN $label-+\\s*([A-Za-z0-9._-]+)\\s*-+END $label-+",
    multiLine: true,
  );
  final matches = pattern.allMatches(contents).toList(growable: false);
  if (matches.length != 1) {
    throw NatsAuthenticationException(
      "Credentials must contain exactly one $label block",
    );
  }
  return matches.single.group(1)!;
}

final class _ParsedCredentials {
  const _ParsedCredentials(this.jwt, this.seed);

  final String jwt;
  final String seed;
}
