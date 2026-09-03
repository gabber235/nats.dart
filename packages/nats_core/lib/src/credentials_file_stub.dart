import "package:nats_core/src/authentication.dart";
import "package:nats_core/src/error.dart";

/// File credentials are unavailable on this platform.
Future<NatsAuthentication> loadNatsCredentialsFile(String path) async =>
    throw const NatsUnsupportedRuntimeException(
      operation: "credentials file loading",
      option: "credentialsFile",
    );
