/// A reliable pure Dart client for NATS Core.
library;

export "src/authentication.dart";
export "src/connection_event.dart";
export "src/credentials_file_stub.dart"
    if (dart.library.io) "src/credentials_file_io.dart";
export "src/error.dart";
export "src/headers.dart";
export "src/message.dart";
export "src/nats_connection.dart";
export "src/options.dart" hide sameServerEndpoint, validateAndSnapshotServers;
export "src/request_many.dart";
export "src/server_info.dart" show NatsServerInfo;
export "src/statistics.dart";
export "src/subject.dart";
export "src/subscription.dart";
export "src/trace.dart";
