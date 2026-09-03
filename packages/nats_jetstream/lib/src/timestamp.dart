// ignore_for_file: public_member_api_docs

final _jetStreamRfc3339Syntax = RegExp(
  r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,9})?(?:Z|[+-](\d{2}):(\d{2}))$",
);

String validateJetStreamRfc3339(String value, String name) {
  final match = _jetStreamRfc3339Syntax.firstMatch(value);
  if (match == null) {
    throw ArgumentError.value(value, name, "Must be an RFC 3339 timestamp");
  }
  final year = int.parse(match.group(1)!);
  final month = int.parse(match.group(2)!);
  final day = int.parse(match.group(3)!);
  final hour = int.parse(match.group(4)!);
  final minute = int.parse(match.group(5)!);
  final second = int.parse(match.group(6)!);
  final offsetHour = match.group(7);
  final offsetMinute = match.group(8);
  if (year == 0 ||
      month < 1 ||
      month > 12 ||
      day < 1 ||
      day > _daysInMonth(year, month) ||
      hour > 23 ||
      minute > 59 ||
      second > 59 ||
      (offsetHour != null && int.parse(offsetHour) > 23) ||
      (offsetMinute != null && int.parse(offsetMinute) > 59)) {
    throw ArgumentError.value(value, name, "Must be an RFC 3339 timestamp");
  }
  return value;
}

String serializeJetStreamRfc3339(DateTime value, String name) =>
    validateJetStreamRfc3339(value.toIso8601String(), name);

int _daysInMonth(int year, int month) => switch (month) {
  2 => _isLeapYear(year) ? 29 : 28,
  4 || 6 || 9 || 11 => 30,
  _ => 31,
};

bool _isLeapYear(int year) =>
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);

/// Formats an exact Unix nanosecond timestamp as RFC 3339 UTC.
String jetStreamTimestamp(BigInt nanoseconds) {
  final seconds = nanoseconds ~/ BigInt.from(1000000000);
  final remainder = nanoseconds.remainder(BigInt.from(1000000000)).toInt();
  final date = DateTime.fromMillisecondsSinceEpoch(
    seconds.toInt() * 1000,
    isUtc: true,
  );
  final base = date.toIso8601String().split(".").first;
  return "$base.${remainder.toString().padLeft(9, "0")}Z";
}
