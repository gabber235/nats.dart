/// Maximum patterns validated as one disjoint subject set.
const maximumJetStreamSubjectPatterns = 4096;

const _maximumSubjectPatternWork = 1000000;

/// Rejects duplicate or intersecting NATS subscription patterns.
void validateDisjointNatsSubjectPatterns(
  List<String> patterns, {
  String name = "subjects",
}) {
  _validatePatternCount(patterns, name);
  final root = _SubjectPatternNode();
  var work = 0;
  for (final pattern in patterns) {
    final tokens = pattern.split(".");
    if (_intersects(root, tokens, () {
      work += 1;
      if (work > _maximumSubjectPatternWork) {
        throw ArgumentError.value(
          patterns.length,
          name,
          "Subject intersection work exceeds $_maximumSubjectPatternWork",
        );
      }
    })) {
      throw ArgumentError.value(patterns, name, "Must not overlap");
    }
    _insert(root, tokens);
  }
}

/// Requires every pattern to intersect at least one candidate.
///
/// Validation work is explicitly bounded.
void validateNatsSubjectPatternsIntersectAny(
  List<String> patterns,
  List<String> candidates, {
  String name = "patterns",
}) {
  _validatePatternCount(patterns, name);
  _validatePatternCount(candidates, "candidates");
  final root = _SubjectPatternNode();
  for (final candidate in candidates) {
    _insert(root, candidate.split("."));
  }
  var work = 0;
  for (final pattern in patterns) {
    final intersects = _intersects(root, pattern.split("."), () {
      work += 1;
      if (work > _maximumSubjectPatternWork) {
        throw ArgumentError.value(
          patterns.length,
          name,
          "Subject intersection work exceeds $_maximumSubjectPatternWork",
        );
      }
    });
    if (!intersects) {
      throw ArgumentError.value(
        pattern,
        name,
        "Must intersect a candidate pattern",
      );
    }
  }
}

void _validatePatternCount(List<String> patterns, String name) {
  if (patterns.length > maximumJetStreamSubjectPatterns) {
    throw ArgumentError.value(
      patterns.length,
      name,
      "Exceeds $maximumJetStreamSubjectPatterns patterns",
    );
  }
}

bool _intersects(
  _SubjectPatternNode root,
  List<String> tokens,
  void Function() recordWork,
) {
  final pending = <(_SubjectPatternNode, int)>[(root, 0)];
  while (pending.isNotEmpty) {
    final (node, index) = pending.removeLast();
    recordWork();
    if (node.greaterThan && index < tokens.length) return true;
    if (index == tokens.length) {
      if (node.terminal) return true;
      continue;
    }
    final token = tokens[index];
    if (token == ">") {
      if (node.greaterThan ||
          node.wildcard != null ||
          node.literals.isNotEmpty) {
        return true;
      }
      continue;
    }
    final nextIndex = index + 1;
    final wildcard = node.wildcard;
    if (wildcard != null) pending.add((wildcard, nextIndex));
    if (token == "*") {
      for (final child in node.literals.values) {
        pending.add((child, nextIndex));
      }
    } else {
      final literal = node.literals[token];
      if (literal != null) pending.add((literal, nextIndex));
    }
  }
  return false;
}

void _insert(_SubjectPatternNode root, List<String> tokens) {
  var node = root;
  for (final token in tokens) {
    if (token == ">") {
      node.greaterThan = true;
      return;
    }
    if (token == "*") {
      node = node.wildcard ??= _SubjectPatternNode();
    } else {
      node = node.literals.putIfAbsent(token, _SubjectPatternNode.new);
    }
  }
  node.terminal = true;
}

final class _SubjectPatternNode {
  final Map<String, _SubjectPatternNode> literals = {};
  _SubjectPatternNode? wildcard;
  bool greaterThan = false;
  bool terminal = false;
}
