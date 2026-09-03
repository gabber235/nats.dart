part of "package:nats_core/src/protocol/server_parser.dart";

final class _ChunkBuffer {
  static const _chunkBytes = 8192;
  static const _compactionThreshold = 64;

  final List<_BufferChunk> _chunks = [];
  final List<int> controlPrefix = [];
  int _headChunk = 0;
  int _headOffset = 0;
  int _scanChunk = 0;
  int _scanOffset = 0;
  int _length = 0;
  int scannedBytes = 0;
  int? previousScanByte;

  int get length => _length;

  void addRange(List<int> bytes, int start, int end) {
    RangeError.checkValidRange(start, end, bytes.length);
    final wasEmpty = _length == 0;
    var sourceOffset = start;
    while (sourceOffset < end) {
      final chunk = _writableChunk();
      final copied = chunk.addRange(bytes, sourceOffset, end);
      sourceOffset += copied;
      _length += copied;
    }
    if (wasEmpty) {
      _resetScan();
    }
  }

  _BufferChunk _writableChunk() {
    if (_chunks.isNotEmpty && _chunks.last.remaining > 0) {
      return _chunks.last;
    }
    final chunk = _BufferChunk(_chunkBytes);
    _chunks.add(chunk);
    return chunk;
  }

  int? nextScanByte() {
    while (_scanChunk < _chunks.length) {
      final chunk = _chunks[_scanChunk];
      if (_scanOffset == chunk.length) {
        if (_scanChunk + 1 == _chunks.length) {
          return null;
        }
        _scanChunk += 1;
        _scanOffset = 0;
        continue;
      }
      final byte = chunk.bytes[_scanOffset];
      _scanOffset += 1;
      if (controlPrefix.length < 5) {
        controlPrefix.add(byte);
      }
      final prior = _lastScanByte;
      _lastScanByte = byte;
      previousScanByte = prior;
      scannedBytes += 1;
      return byte;
    }
    return null;
  }

  int? _lastScanByte;

  int byteAt(int index) {
    if (index < 0 || index >= _length) {
      throw RangeError.index(index, this, "index", null, _length);
    }
    var remaining = index;
    for (
      var chunkIndex = _headChunk;
      chunkIndex < _chunks.length;
      chunkIndex += 1
    ) {
      final chunk = _chunks[chunkIndex];
      final start = chunkIndex == _headChunk ? _headOffset : 0;
      final available = chunk.length - start;
      if (remaining < available) {
        return chunk.bytes[start + remaining];
      }
      remaining -= available;
    }
    throw StateError("Chunk buffer index is inconsistent");
  }

  Uint8List take(int length) {
    final result = Uint8List(length);
    _copyAndDiscard(result, length);
    return result;
  }

  void discard(int length) {
    _copyAndDiscard(null, length);
  }

  void clear() {
    _chunks.clear();
    _headChunk = 0;
    _headOffset = 0;
    _length = 0;
    _resetScan();
  }

  void _copyAndDiscard(Uint8List? destination, int count) {
    if (count < 0 || count > _length) {
      throw RangeError.range(count, 0, _length, "count");
    }
    var remaining = count;
    var destinationOffset = 0;
    while (remaining > 0) {
      final chunk = _chunks[_headChunk];
      final available = chunk.length - _headOffset;
      final copied = remaining < available ? remaining : available;
      destination?.setRange(
        destinationOffset,
        destinationOffset + copied,
        chunk.bytes,
        _headOffset,
      );
      _headOffset += copied;
      destinationOffset += copied;
      remaining -= copied;
      if (_headOffset == chunk.length) {
        _headChunk += 1;
        _headOffset = 0;
      }
    }
    _length -= count;
    if (_length == 0) {
      _chunks.clear();
      _headChunk = 0;
      _headOffset = 0;
    }
    _compact();
    _resetScan();
  }

  void _compact() {
    if (_headChunk < _compactionThreshold || _headChunk * 2 < _chunks.length) {
      return;
    }
    _chunks.removeRange(0, _headChunk);
    _headChunk = 0;
  }

  void _resetScan() {
    _scanChunk = _headChunk;
    _scanOffset = _headOffset;
    scannedBytes = 0;
    previousScanByte = null;
    _lastScanByte = null;
    controlPrefix.clear();
  }
}

final class _BufferChunk {
  _BufferChunk(int capacity) : bytes = Uint8List(capacity);

  final Uint8List bytes;
  int length = 0;

  int get remaining => bytes.length - length;

  int addRange(List<int> source, int sourceOffset, int sourceEnd) {
    final sourceRemaining = sourceEnd - sourceOffset;
    final count = sourceRemaining < remaining ? sourceRemaining : remaining;
    bytes.setRange(length, length + count, source, sourceOffset);
    length += count;
    return count;
  }
}

final class _PendingMessage {
  const _PendingMessage({
    required this.subject,
    required this.subscriptionId,
    required this.replySubject,
    required this.headerLength,
    required this.totalLength,
  });

  final String subject;
  final int subscriptionId;
  final String? replySubject;
  final int? headerLength;
  final int totalLength;
}
