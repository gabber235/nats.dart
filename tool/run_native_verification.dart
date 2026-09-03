import "dart:io";

import "native_nats_server.dart";
import "verification_evidence.dart";

Future<void> main() async {
  final runner = Platform.environment["RUNNER_OS"] ?? Platform.operatingSystem;
  final evidenceDirectory = Directory("build/verification-evidence");
  await evidenceDirectory.create(recursive: true);
  final results = <Map<String, Object?>>[];
  NativeNatsServer? nativeServer;
  Object? installationFailure;

  for (final command in const [
    ["dart", "pub", "get", "--enforce-lockfile"],
    ["dart", "analyze", "--fatal-infos", "--fatal-warnings"],
    ["dart", "run", "tool/run_unit_tests.dart"],
  ]) {
    final result = await _run(command);
    results.add(result);
    if (result["exitCode"] != 0) break;
  }

  final manifest = readCompatibilityManifest();
  if (results.length == 3 &&
      results.every((result) => result["exitCode"] == 0)) {
    try {
      nativeServer = await installNativeNatsServer(manifest);
      results.add(
        await _run(
          const [
            "dart",
            "run",
            "tool/run_integration_lane.dart",
            "reconnectLatest",
          ],
          environment: {"NATS_SERVER_BINARY": nativeServer.binary},
        ),
      );
      if (results.last["exitCode"] == 0) {
        results.add(
          await _run(
            const ["dart", "run", "tool/run_native_security_verification.dart"],
            environment: {"NATS_SERVER_BINARY": nativeServer.binary},
          ),
        );
      }
      if (!Platform.isWindows && results.last["exitCode"] == 0) {
        results.add(
          await _run(
            const [
              "dart",
              "run",
              "tool/run_integration_lane.dart",
              "reconnectSuspendLatest",
            ],
            environment: {"NATS_SERVER_BINARY": nativeServer.binary},
          ),
        );
      }
    } on Object catch (error) {
      installationFailure = error;
      stderr.writeln("Native NATS Server installation failed: $error");
    }
  }
  final passed =
      installationFailure == null &&
      nativeServer != null &&
      results.length == (Platform.isWindows ? 5 : 6) &&
      results.every((result) => result["exitCode"] == 0);
  await writeJson("${evidenceDirectory.path}/native-$runner.json", {
    "schemaVersion": 1,
    "suite": "native",
    "runner": runner,
    "result": passed ? "passed" : "failed",
    "repository": await repositoryIdentity(),
    "versions": manifestVersions(manifest),
    "ownership": ownershipSummary(manifest),
    "commands": results,
    "nativeNatsServer": nativeServer?.toJson(),
    "installationFailure": installationFailure?.runtimeType.toString(),
    "serviceLogs": {
      "included": false,
      "reason": "Real server output streams to the job and is not retained",
    },
  });

  if (!passed) exitCode = 1;
}

Future<Map<String, Object?>> _run(
  List<String> command, {
  Map<String, String>? environment,
}) async {
  final line = command.join(" ");
  stdout.writeln("Running $line");
  final process = await Process.start(
    command.first,
    command.skip(1).toList(),
    environment: environment,
    mode: ProcessStartMode.inheritStdio,
  );
  final result = await process.exitCode;
  return {"command": command, "exitCode": result};
}
