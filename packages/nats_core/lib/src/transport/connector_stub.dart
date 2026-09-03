import "package:nats_core/src/error.dart";
import "package:nats_core/src/options.dart";
import "package:nats_core/src/transport/transport.dart";

/// Rejects transport work before connection state is created.
void validateTransportOptions(NatsOptions options) {
  throw const NatsUnsupportedRuntimeException(operation: "NATS transport");
}

/// Reports that no transport exists for this runtime yet.
Future<NatsTransport> connectTransport(
  NatsServer server,
  NatsTlsOptions tls,
  NatsWebSocketOptions webSocket,
  Duration timeout,
) => throw const NatsUnsupportedRuntimeException(operation: "NATS transport");
