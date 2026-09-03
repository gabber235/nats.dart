import "dart:convert";
import "dart:io";

Future<void> main(List<String> arguments) async {
  if (arguments.length != 1) {
    stderr.writeln("Usage: dart run tool/run_integration_lane.dart LANE");
    exitCode = 64;
    return;
  }

  final manifest = _object(
    jsonDecode(File("tool/compatibility.json").readAsStringSync()),
    "manifest",
  );
  final lanes = _object(manifest["integrationLanes"], "integrationLanes");
  final laneName = arguments.single;
  final lane = _object(lanes[laneName], "integration lane $laneName");
  final laneEnvironment = _stringMap(
    lane["environment"],
    "integration lane $laneName environment",
  );
  final resolvedEnvironment = {...Platform.environment, ...laneEnvironment};

  for (final prerequisite in _strings(lane["prerequisites"], "prerequisites")) {
    final value = resolvedEnvironment[prerequisite];
    if (value == null || value.isEmpty) {
      throw StateError("Integration lane $laneName requires $prerequisite");
    }
  }

  final files = _strings(lane["files"], "files");
  if (files.isEmpty) {
    throw StateError("Integration lane $laneName owns no tests");
  }
  for (final path in files) {
    if (!File(path).existsSync()) {
      throw StateError("Integration lane $laneName references missing $path");
    }
  }

  final testsByPackage = <String, List<String>>{};
  for (final path in files) {
    final segments = path.split("/");
    if (segments.length < 4 || segments.first != "packages") {
      throw StateError("Integration test is outside a package: $path");
    }
    final package = segments.take(2).join("/");
    testsByPackage
        .putIfAbsent(package, () => [])
        .add(segments.skip(2).join("/"));
  }

  for (final entry in testsByPackage.entries) {
    final testArguments = <String>[
      "test",
      "--chain-stack-traces",
      "--concurrency=1",
      if (lane["platform"] case final String platform) ...[
        "--platform",
        platform,
      ],
      ...entry.value,
    ];
    final process = await Process.start(
      Platform.resolvedExecutable,
      testArguments,
      workingDirectory: entry.key,
      environment: laneEnvironment,
      mode: ProcessStartMode.inheritStdio,
    );
    final result = await process.exitCode;
    if (result != 0) {
      exitCode = result;
      return;
    }
  }
}

Map<String, Object?> _object(Object? value, String name) {
  if (value is! Map<String, Object?>) {
    throw FormatException("$name must be an object");
  }
  return value;
}

List<String> _strings(Object? value, String name) {
  if (value is! List<Object?> || value.any((item) => item is! String)) {
    throw FormatException("$name must be a string list");
  }
  return value.cast<String>();
}

Map<String, String> _stringMap(Object? value, String name) {
  if (value == null) return const {};
  if (value is! Map<String, Object?> ||
      value.values.any((item) => item is! String)) {
    throw FormatException("$name must be a string map");
  }
  return value.cast<String, String>();
}
