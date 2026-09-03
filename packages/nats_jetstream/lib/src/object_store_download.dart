import "dart:async";
import "dart:typed_data";

import "package:crypto/crypto.dart";
import "package:nats_jetstream/src/object_store.dart";
import "package:nats_jetstream/src/object_store_digest.dart";
import "package:nats_jetstream/src/object_store_error.dart";
import "package:nats_jetstream/src/object_store_protocol.dart";

/// One single listener, chunk bounded Object Store download.
final class JetStreamObjectDownload {
  /// Creates a lazy verified download over immutable committed metadata.
  JetStreamObjectDownload._(this.info, JetStreamObjectProtocol protocol)
    : _protocol = protocol {
    chunks = _read();
    _verified.future.ignore();
  }

  /// Metadata snapshot that selects the immutable chunk subject.
  final JetStreamObjectInfo info;

  final JetStreamObjectProtocol _protocol;
  final Completer<void> _verified = Completer<void>();
  var _listened = false;

  /// Immutable byte chunks. Exactly one listener is allowed.
  late final Stream<Uint8List> chunks;

  /// Completes only after chunk count, size, and SHA 256 all match metadata.
  Future<void> get verified => _verified.future;

  Stream<Uint8List> _read() async* {
    if (_listened) {
      throw StateError("Object download supports one listener");
    }
    _listened = true;
    final digestResult = _DownloadDigestResult();
    final digestSink = sha256.startChunkedConversion(digestResult);
    var sequence = BigInt.one;
    var received = BigInt.zero;
    var complete = false;
    try {
      final subject = _protocol.chunkSubject(info.identifier);
      for (var chunk = BigInt.zero; chunk < info.chunks; chunk += BigInt.one) {
        final stored = await _protocol.getStored({
          "seq": sequence,
          "next_by_subj": subject,
        });
        if (stored == null || stored.subject != subject) {
          throw JetStreamObjectIntegrityException(
            "Object chunk sequence is incomplete",
            bucket: info.bucket,
            name: info.name,
          );
        }
        final value = Uint8List.fromList(stored.payload).asUnmodifiableView();
        received += BigInt.from(value.length);
        if (received > info.size) {
          throw JetStreamObjectIntegrityException(
            "Object chunks exceed committed size",
            bucket: info.bucket,
            name: info.name,
          );
        }
        digestSink.add(value);
        sequence = stored.sequence + BigInt.one;
        yield value;
      }
      digestSink.close();
      final actual = digestResult.value.bytes;
      if (received != info.size ||
          !objectHashMatches(objectDigestHash(info.digest), actual)) {
        throw JetStreamObjectIntegrityException(
          "Object bytes do not match committed metadata",
          bucket: info.bucket,
          name: info.name,
          expectedDigest: info.digest,
          actualDigest: objectDigestFromHash(actual),
        );
      }
      complete = true;
      _verified.complete();
    } on Object catch (error, stackTrace) {
      if (!_verified.isCompleted) {
        _verified.completeError(error, stackTrace);
      }
      rethrow;
    } finally {
      if (!complete && !_verified.isCompleted) {
        _verified.completeError(
          JetStreamObjectDownloadCancelledException(info.bucket, info.name),
        );
      }
    }
  }
}

/// Creates a lazy download after metadata and local bounds were checked.
JetStreamObjectDownload openJetStreamObjectDownload(
  JetStreamObjectInfo info,
  JetStreamObjectProtocol protocol,
) => JetStreamObjectDownload._(info, protocol);

final class _DownloadDigestResult implements Sink<Digest> {
  Digest? _value;

  Digest get value => _value ?? (throw StateError("Digest is not complete"));

  @override
  void add(Digest data) {
    if (_value != null) {
      throw StateError("Digest completed more than once");
    }
    _value = data;
  }

  @override
  void close() {
    if (_value == null) {
      throw StateError("Digest did not complete");
    }
  }
}
