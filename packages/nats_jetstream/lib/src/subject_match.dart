import "package:nats_core/nats_core.dart";

/// Whether one concrete subject belongs to a validated subscription pattern.
bool matchesNatsSubjectPattern(String pattern, String subject) {
  try {
    validatePublishSubject(subject);
  } on NatsSubjectException {
    return false;
  }
  final patternTokens = pattern.split(".");
  final subjectTokens = subject.split(".");
  for (var index = 0; index < patternTokens.length; index += 1) {
    final token = patternTokens[index];
    if (token == ">") return index < subjectTokens.length;
    if (index >= subjectTokens.length ||
        (token != "*" && token != subjectTokens[index])) {
      return false;
    }
  }
  return patternTokens.length == subjectTokens.length;
}

/// Whether two validated subscription patterns can match one same subject.
bool natsSubjectPatternsIntersect(String left, String right) {
  final leftTokens = left.split(".");
  final rightTokens = right.split(".");
  final commonLength = leftTokens.length < rightTokens.length
      ? leftTokens.length
      : rightTokens.length;
  for (var index = 0; index < commonLength; index += 1) {
    final leftToken = leftTokens[index];
    final rightToken = rightTokens[index];
    if (leftToken == ">" || rightToken == ">") return true;
    if (leftToken != "*" && rightToken != "*" && leftToken != rightToken) {
      return false;
    }
  }
  return leftTokens.length == rightTokens.length;
}
