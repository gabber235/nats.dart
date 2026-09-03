bool requiredIntegrationCapability(
  Map<String, String> environment,
  String name,
) => switch (environment[name]) {
  "true" => true,
  "false" => false,
  final value => throw StateError(
    "$name must be explicitly true or false, received ${value ?? "missing"}",
  ),
};
