import "dart:async";
import "dart:convert";
import "dart:io";

Future<void> main(List<String> arguments) async {
  if (arguments.any((argument) => argument != "--lock-only") ||
      arguments.length > 1) {
    throw StateError("Expected no arguments or --lock-only");
  }
  final manifest = jsonDecode(File("compatibility.json").readAsStringSync());
  if (manifest is! Map<String, Object?>) {
    throw const FormatException("Compatibility manifest must be an object");
  }
  final flutter = manifest["flutter"];
  if (flutter is! Map<String, Object?>) {
    throw const FormatException("Flutter compatibility must be an object");
  }

  final user = await _runCaptured("id", ["-un"]);
  _require(
    user.exitCode == 0,
    "Could not resolve the effective user: ${user.stderr}",
  );
  _require(
    user.stdout.trim() == "flutter",
    "Flutter verification must run as the flutter user",
  );
  _require(
    Platform.environment["HOME"] == "/home/flutter",
    "Flutter verification has an unexpected HOME",
  );

  final version = await _runCaptured("flutter", ["--version", "--machine"]);
  _require(
    version.exitCode == 0,
    "flutter version inspection failed: ${version.stderr}",
  );
  final decodedVersion = jsonDecode(version.stdout);
  if (decodedVersion is! Map<String, Object?>) {
    throw const FormatException("Flutter version output must be an object");
  }
  _require(
    decodedVersion["frameworkVersion"] == flutter["required"],
    "Flutter SDK version does not match compatibility.json",
  );
  _require(
    decodedVersion["frameworkRevision"] == flutter["revision"],
    "Flutter SDK revision does not match compatibility.json",
  );

  await _runFlutter(["pub", "get", "--enforce-lockfile"]);
  if (arguments.contains("--lock-only")) return;
  await _runFlutter(["test", "--no-pub"]);
}

Future<_CapturedResult> _runCaptured(
  String executable,
  List<String> arguments,
) async {
  final process = await Process.start(executable, arguments);
  final stdoutResult = process.stdout.transform(utf8.decoder).join();
  final stderrResult = process.stderr.transform(utf8.decoder).join();
  try {
    final result = await process.exitCode.timeout(const Duration(minutes: 5));
    return _CapturedResult(result, await stdoutResult, await stderrResult);
  } on TimeoutException {
    process.kill(ProcessSignal.sigkill);
    await process.exitCode;
    await stdoutResult;
    await stderrResult;
    throw StateError("$executable inspection exceeded 5 minutes");
  }
}

Future<void> _runFlutter(List<String> arguments) async {
  final process = await Process.start(
    "flutter",
    arguments,
    mode: ProcessStartMode.inheritStdio,
  );
  try {
    final result = await process.exitCode.timeout(const Duration(minutes: 15));
    _require(result == 0, "flutter ${arguments.join(" ")} failed");
  } on TimeoutException {
    process.kill(ProcessSignal.sigkill);
    await process.exitCode;
    throw StateError("flutter ${arguments.join(" ")} exceeded 15 minutes");
  }
}

void _require(bool condition, String message) {
  if (!condition) throw StateError(message);
}

final class _CapturedResult {
  const _CapturedResult(this.exitCode, this.stdout, this.stderr);

  final int exitCode;
  final String stdout;
  final String stderr;
}
