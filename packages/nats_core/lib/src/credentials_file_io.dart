import "dart:io";

import "package:nats_core/src/authentication.dart";
import "package:nats_core/src/error.dart";

/// Loads NATS credentials from [path].
Future<NatsAuthentication> loadNatsCredentialsFile(String path) async {
  try {
    return NatsAuthentication.credentials(await File(path).readAsString());
  } on NatsAuthenticationException {
    rethrow;
  } on Object catch (error, stackTrace) {
    Error.throwWithStackTrace(
      NatsAuthenticationException(
        "Unable to load NATS credentials file",
        cause: error,
        causeStackTrace: stackTrace,
      ),
      stackTrace,
    );
  }
}
