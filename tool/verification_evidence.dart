import "dart:convert";
import "dart:io";

Map<String, Object?> readCompatibilityManifest() {
  final decoded = jsonDecode(
    File("tool/compatibility.json").readAsStringSync(),
  );
  if (decoded is! Map<String, Object?>) {
    throw const FormatException("Compatibility manifest must be an object");
  }
  return decoded;
}

Future<Map<String, Object?>> repositoryIdentity() async {
  final revision = await Process.run("git", ["rev-parse", "HEAD"]);
  final status = await Process.run("git", ["status", "--porcelain"]);
  return {
    "revision": revision.exitCode == 0
        ? (revision.stdout as String).trim()
        : "unavailable",
    "dirty":
        status.exitCode != 0 || (status.stdout as String).trim().isNotEmpty,
  };
}

Map<String, Object?> ownershipSummary(Map<String, Object?> manifest) {
  final lanes = manifest["integrationLanes"];
  if (lanes is! Map<String, Object?>) {
    throw const FormatException("Integration lanes must be an object");
  }

  final owned = <String>{};
  final laneSummary = <String, Object?>{};
  for (final entry in lanes.entries) {
    final lane = entry.value;
    if (lane is! Map<String, Object?>) {
      throw FormatException("Integration lane ${entry.key} must be an object");
    }
    final files = (lane["files"] as List<Object?>).cast<String>();
    owned.addAll(files);
    laneSummary[entry.key] = {
      "check": lane["check"],
      "prerequisites": lane["prerequisites"],
      "files": files,
    };
  }

  final discovered = <String>[];
  final selfSkipping = <String>[];
  for (final directory in [
    "packages/nats_core/test/integration",
    "packages/nats_jetstream/test/integration",
    "packages/nats_core/test/browser",
  ]) {
    for (final entity in Directory(directory).listSync(recursive: true)) {
      if (entity is! File || !entity.path.endsWith("_test.dart")) continue;
      final path = entity.path.replaceAll(Platform.pathSeparator, "/");
      discovered.add(path);
      if (RegExp(r"\bskip\s*[:=]").hasMatch(entity.readAsStringSync())) {
        selfSkipping.add(path);
      }
    }
  }
  discovered.sort();
  selfSkipping.sort();

  return {
    "discoveredTests": discovered.length,
    "ownedTests": owned.length,
    "unownedTests": discovered.where((path) => !owned.contains(path)).toList(),
    "selfSkippingTests": selfSkipping,
    "ownershipRule":
        "Every test is owned by a mandatory lane that validates "
        "prerequisites before the test process starts",
    "lanes": laneSummary,
  };
}

Map<String, Object?> manifestVersions(Map<String, Object?> manifest) {
  return {
    for (final key in [
      "dart",
      "flutter",
      "dagger",
      "natsServer",
      "natsGo",
      "nativeServerArchives",
      "images",
    ])
      key: manifest[key],
  };
}

Future<void> writeJson(String path, Object? value) async {
  final file = File(path);
  await file.parent.create(recursive: true);
  await file.writeAsString(
    "${const JsonEncoder.withIndent("  ").convert(value)}\n",
  );
}
