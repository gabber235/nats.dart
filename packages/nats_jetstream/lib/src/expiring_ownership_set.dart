// ignore_for_file: public_member_api_docs

import "dart:collection";

/// Bounded local ownership for server resources with automatic server expiry.
final class JetStreamExpiringOwnershipSet {
  JetStreamExpiringOwnershipSet(
    this.retention, {
    this.maximumEntries = 256,
    DateTime Function()? clock,
  }) : _clock = clock ?? DateTime.timestamp;

  final Duration retention;
  final int maximumEntries;
  final DateTime Function() _clock;
  final LinkedHashMap<String, DateTime> _deadlines = LinkedHashMap();

  void add(String value) {
    _prune();
    _deadlines.remove(value);
    while (_deadlines.length >= maximumEntries) {
      _deadlines.remove(_deadlines.keys.first);
    }
    _deadlines[value] = _clock().add(retention);
  }

  bool contains(String value) {
    _prune();
    return _deadlines.containsKey(value);
  }

  void remove(String value) => _deadlines.remove(value);

  List<String> snapshot() {
    _prune();
    return List.unmodifiable(_deadlines.keys);
  }

  bool get isNotEmpty {
    _prune();
    return _deadlines.isNotEmpty;
  }

  void _prune() {
    final now = _clock();
    _deadlines.removeWhere((_, deadline) => !deadline.isAfter(now));
  }
}
