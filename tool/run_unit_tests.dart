import "dart:io";

Future<void> main() async {
  final tests = <String>[];
  for (final testRoot in [
    "test",
    "packages/nats_core/test",
    "packages/nats_jetstream/test",
  ]) {
    await for (final entity in Directory(
      testRoot,
    ).list(recursive: true, followLinks: false)) {
      if (entity is! File || !entity.path.endsWith("_test.dart")) continue;
      final path = entity.path.replaceAll(Platform.pathSeparator, "/");
      if (path.contains("/integration/") || path.contains("/browser/")) {
        continue;
      }
      tests.add(path);
    }
  }
  tests.sort();
  if (tests.isEmpty) throw StateError("No unit tests were discovered");

  final process = await Process.start(Platform.resolvedExecutable, [
    "test",
    "--chain-stack-traces",
    ...tests,
  ], mode: ProcessStartMode.inheritStdio);
  final exitCode = await process.exitCode;
  if (exitCode != 0) exit(exitCode);
}
