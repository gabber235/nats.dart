import "dart:typed_data";

import "package:nats_core/src/options.dart";

/// Internal byte transport used by the protocol lifecycle owner.
abstract interface class NatsTransport {
  /// Bytes received from the peer.
  Stream<Uint8List> get incoming;

  /// Accepts bytes for ordered writing.
  void add(Uint8List bytes);

  /// Completes when the transport has accepted prior bytes for ordered sending.
  Future<void> flush();

  /// Upgrades an established plain transport to TLS.
  Future<void> startTls({
    required String host,
    required NatsTlsOptions options,
    required Duration timeout,
  });

  /// Closes the transport and its byte stream.
  Future<void> close();
}
