import "dart:convert";
import "dart:io";

void main() {
  final manifest = _object(
    jsonDecode(File("tool/compatibility.json").readAsStringSync()),
    "manifest",
  );
  _require(manifest["schemaVersion"] == 1, "Unsupported manifest schema");

  final dart = _object(manifest["dart"], "dart");
  final flutter = _object(manifest["flutter"], "flutter");
  final dagger = _object(manifest["dagger"], "dagger");
  final githubActions = _object(manifest["githubActions"], "githubActions");
  final server = _object(manifest["natsServer"], "natsServer");
  final natsGo = _object(manifest["natsGo"], "natsGo");
  final nativeServerArchives = _object(
    manifest["nativeServerArchives"],
    "nativeServerArchives",
  );
  final images = _object(manifest["images"], "images");
  final referenceClientFeatures = _object(
    manifest["referenceClientFeatures"],
    "referenceClientFeatures",
  );
  final mainGo = File(".dagger/main.go").readAsStringSync();
  final readme = File("README.md").readAsStringSync();

  _require(
    !manifest.containsKey("verifiedAt"),
    "verifiedAt is evidence shaped metadata and must not be stored",
  );
  for (final function in [
    "unit",
    "api-docs",
    "external-git-consumer",
    "external-flutter-git-consumer",
    "minimum-sdk",
    "flutter-consumer",
    "core-latest",
    "jet-stream-latest",
    "jet-stream-protocol-latest",
    "jet-stream-cluster-latest",
    "ordered-consumer-latest",
    "object-store-latest",
    "reconnect-latest",
    "reconnect-suspend-latest",
    "stream-transfer-latest",
    "go-interop-latest",
    "auth-latest",
    "web-socket-latest",
    "soak-smoke",
  ]) {
    _require(
      readme.contains("dagger call $function stdout"),
      "README must execute the $function container with stdout",
    );
  }
  _require(
    readme.contains(
      "dagger call soak --duration-seconds=1800 --seed=20260902 stdout",
    ),
    "README must execute the soak container with stdout",
  );
  _require(
    _strings(
          manifest["excludedUnreleasedFeatures"],
          "excludedUnreleasedFeatures",
        ).toSet().difference({"server_2_15"}).isEmpty &&
        _strings(
              manifest["excludedUnreleasedFeatures"],
              "excludedUnreleasedFeatures",
            ).length ==
            1,
    "Only actual NATS Server 2.15 work may be labeled unreleased",
  );
  const featureStatuses = {
    "kv_codecs_adr_54": "public_in_nats_js_3_4_0",
    "kv_subject_transforms_adr_57": "proposed_not_in_nats_js_3_4_0",
  };
  for (final feature in featureStatuses.entries) {
    final reference = _object(
      referenceClientFeatures[feature.key],
      "reference client feature ${feature.key}",
    );
    _require(
      _string(reference, "status") == feature.value,
      "${feature.key} must record its exact reference client status",
    );
    _require(
      _string(reference, "specificationStatus") == "proposed",
      "${feature.key} must retain the current ADR status",
    );
    _require(
      _string(reference, "implementation") == "complete",
      "${feature.key} must record complete implementation",
    );
  }
  final subjectTransforms = _object(
    referenceClientFeatures["kv_subject_transforms_adr_57"],
    "reference client feature kv_subject_transforms_adr_57",
  );
  _require(
    _string(subjectTransforms, "serverProof") == "nats_2_14_6",
    "KV subject transforms must record their stable server proof",
  );

  for (final entry in images.entries) {
    final image = entry.value;
    _require(image is String, "Image ${entry.key} must be a string");
    _require(
      RegExp(r"^[^@]+@sha256:[0-9a-f]{64}$").hasMatch(image as String),
      "Image ${entry.key} is not pinned by digest",
    );
    _require(mainGo.contains(image), "Dagger does not use image ${entry.key}");
  }

  final minimumDart = _string(dart, "minimum");
  _require(
    _string(images, "dartMinimum").startsWith("dart:$minimumDart@"),
    "Minimum Dart image version does not match",
  );
  for (final path in [
    "pubspec.yaml",
    "packages/nats_core/pubspec.yaml",
    "packages/nats_jetstream/pubspec.yaml",
  ]) {
    _require(
      File(path).readAsStringSync().contains(">=$minimumDart <4.0.0"),
      "$path does not declare Dart $minimumDart",
    );
  }
  final flutterVerifier = File(
    "tool/flutter_consumer/verify_consumer.dart",
  ).readAsStringSync();
  for (final assertion in [
    'decodedVersion["frameworkVersion"]',
    'decodedVersion["frameworkRevision"]',
    '_runCaptured("id", ["-un"])',
    '_runCaptured("flutter", ["--version", "--machine"])',
    'Platform.environment["HOME"]',
  ]) {
    _require(
      flutterVerifier.contains(assertion),
      "Flutter consumer does not assert $assertion",
    );
  }
  final flutterCheck = _daggerCheckBody(mainGo, "FlutterConsumer");
  _require(
    mainGo.contains('"compatibility.json"') &&
        flutterCheck.contains('"verify_consumer.dart"'),
    "Flutter consumer does not run its exact environment verifier",
  );

  final flutterVersion = _string(flutter, "required");
  _require(
    _string(
      images,
      "flutter",
    ).startsWith("ghcr.io/gmeligio/flutter-linux:$flutterVersion@"),
    "Flutter image version does not match",
  );
  _require(
    RegExp(r"^[0-9a-f]{40}$").hasMatch(_string(flutter, "revision")),
    "Flutter revision must be an exact commit",
  );
  for (final path in [
    "tool/flutter_consumer/pubspec.yaml",
    "tool/flutter_consumer/pubspec.lock",
    "tool/flutter_consumer/test/public_packages_test.dart",
    "tool/flutter_consumer/verify_consumer.dart",
  ]) {
    _require(
      File(path).existsSync(),
      "Flutter consumer fixture missing: $path",
    );
  }

  final daggerVersion = _string(dagger, "required");
  final daggerConfiguration = File("dagger.json").readAsStringSync();
  _require(
    daggerConfiguration.contains('"engineVersion": "v$daggerVersion"'),
    "dagger.json does not match the compatibility manifest",
  );
  final actionVersion = _string(dagger, "githubAction");
  final workflowFiles = {
    "verification": File(
      ".github/workflows/verification.yml",
    ).readAsStringSync(),
    "resilience": File(".github/workflows/resilience.yml").readAsStringSync(),
  };
  final workflow = workflowFiles["verification"]!;
  _require(
    workflow.contains("# v$actionVersion"),
    "The Dagger GitHub Action pin is not documented as v$actionVersion",
  );
  for (final action in githubActions.entries) {
    final revision = action.value;
    _require(
      revision is String && RegExp(r"^[0-9a-f]{40}$").hasMatch(revision),
      "GitHub Action ${action.key} must use an exact commit",
    );
    _require(
      workflowFiles.values.any((source) => source.contains(revision as String)),
      "Workflows do not use GitHub Action ${action.key}",
    );
  }
  final checkoutRevision = _string(githubActions, "checkout");
  final daggerRevision = _string(githubActions, "dagger");
  for (final entry in workflowFiles.entries) {
    final uses = RegExp(
      r"uses:\s+[^@\s]+@([^\s#]+)",
    ).allMatches(entry.value).map((match) => match.group(1)!).toList();
    _require(uses.isNotEmpty, "${entry.key} workflow has no actions");
    _require(
      uses.every((revision) => RegExp(r"^[0-9a-f]{40}$").hasMatch(revision)),
      "${entry.key} workflow contains a mutable action reference",
    );
    _require(
      entry.value.contains(checkoutRevision) &&
          entry.value.contains(daggerRevision),
      "${entry.key} workflow does not use central action pins",
    );
  }
  _require(
    workflow.contains('sdk: "${_string(dart, "required")}"'),
    "Native CI does not use the required Dart SDK",
  );

  final latestServer = _string(server, "required");
  final compatibility212 = _string(server, "mergeCompatibility");
  final compatibility210 = _string(server, "scheduledCompatibility");
  _require(
    _string(images, "natsLatest").startsWith("nats:$latestServer-alpine@"),
    "Latest NATS image version does not match",
  );
  const expectedNativeArchives = {
    "linux-x64",
    "linux-arm64",
    "macos-x64",
    "macos-arm64",
    "windows-x64",
    "windows-arm64",
  };
  _require(
    nativeServerArchives.keys
            .toSet()
            .difference(expectedNativeArchives)
            .isEmpty &&
        expectedNativeArchives
            .difference(nativeServerArchives.keys.toSet())
            .isEmpty,
    "Native NATS Server archive mappings must be exact",
  );
  for (final entry in nativeServerArchives.entries) {
    final archive = _object(entry.value, "native archive ${entry.key}");
    final url = _string(archive, "url");
    final digest = _string(archive, "sha256");
    _require(
      url.startsWith(
        "https://github.com/nats-io/nats-server/releases/download/"
        "v$latestServer/nats-server-v$latestServer-",
      ),
      "Native archive ${entry.key} is not an official $latestServer asset",
    );
    _require(
      RegExp(r"^[0-9a-f]{64}$").hasMatch(digest),
      "Native archive ${entry.key} does not have an exact SHA256",
    );
  }
  _require(
    _string(
      images,
      "natsCompatibility212",
    ).startsWith("nats:$compatibility212-alpine@"),
    "NATS 2.12 image version does not match",
  );
  _require(
    _string(
      images,
      "natsCompatibility210",
    ).startsWith("nats:$compatibility210-alpine@"),
    "NATS 2.10 image version does not match",
  );

  final goVersion = _string(natsGo, "required");
  _require(
    File(
      "tool/go_interop/go.mod",
    ).readAsStringSync().contains("github.com/nats-io/nats.go v$goVersion"),
    "nats.go module version does not match",
  );

  final features = manifest["requiredLatestFeatures"];
  if (features is! List<Object?> || features.isEmpty) {
    throw StateError("Features missing");
  }
  _require(
    features.every((feature) => feature is String),
    "Latest feature names must be strings",
  );
  _require(
    features.toSet().length == features.length,
    "Latest feature names must be unique",
  );

  final lanes = _object(manifest["integrationLanes"], "integrationLanes");
  const expectedMessageTracingCapabilities = {
    "compatibility212": "true",
    "compatibility210": "false",
    "coreLatest": "true",
  };
  final ownedIntegrationTests = <String>{};
  for (final laneEntry in lanes.entries) {
    final laneName = laneEntry.key;
    final lane = _object(laneEntry.value, "integration lane $laneName");
    final check = _string(lane, "check");
    final environment = _stringMap(
      lane["environment"],
      "$laneName environment",
    );
    for (final variable in environment.keys) {
      _require(
        RegExp(r"^[A-Z][A-Z0-9_]+$").hasMatch(variable),
        "$laneName has invalid environment variable $variable",
      );
    }
    final prerequisites = _strings(
      lane["prerequisites"],
      "$laneName prerequisites",
    );
    _require(
      prerequisites.toSet().length == prerequisites.length,
      "$laneName prerequisites must be unique",
    );
    for (final prerequisite in prerequisites) {
      _require(
        RegExp(r"^[A-Z][A-Z0-9_]+$").hasMatch(prerequisite),
        "$laneName has invalid prerequisite $prerequisite",
      );
    }

    final files = _strings(lane["files"], "$laneName files");
    _require(files.isNotEmpty, "$laneName must own tests");
    _require(
      files.toSet().length == files.length,
      "$laneName test files must be unique",
    );
    for (final path in files) {
      _require(File(path).existsSync(), "$laneName references missing $path");
      _require(
        _isIntegrationTest(path),
        "$laneName references nonintegration test $path",
      );
      ownedIntegrationTests.add(path);
    }

    final platform = lane["platform"];
    _require(
      platform == null || platform == "chrome",
      "$laneName has unsupported platform $platform",
    );

    final checkBody = _daggerCheckBody(mainGo, check);
    _require(
      checkBody.contains('"$laneName"'),
      "Dagger check $check does not execute lane $laneName",
    );
    if (expectedMessageTracingCapabilities[laneName] case final expected?) {
      _require(
        environment["NATS_EXPECTS_MESSAGE_TRACING"] == expected,
        "$laneName must declare message tracing capability as $expected",
      );
    }
  }

  final discoveredIntegrationTests = _discoverIntegrationTests();
  final unowned = discoveredIntegrationTests.difference(ownedIntegrationTests);
  final stale = ownedIntegrationTests.difference(discoveredIntegrationTests);
  _require(unowned.isEmpty, "Unowned integration tests: ${unowned.join(", ")}");
  _require(stale.isEmpty, "Stale integration tests: ${stale.join(", ")}");

  final featureTests = _object(manifest["featureTests"], "featureTests");
  final featureNames = features.cast<String>().toSet();
  _require(
    featureTests.keys.toSet().difference(featureNames).isEmpty &&
        featureNames.difference(featureTests.keys.toSet()).isEmpty,
    "Feature test mappings must exactly match required latest features",
  );
  for (final feature in featureNames) {
    final proofs = featureTests[feature];
    _require(
      proofs is List<Object?> && proofs.isNotEmpty,
      "$feature must map to test proofs",
    );
    for (final proofValue in proofs! as List<Object?>) {
      final proof = _object(proofValue, "$feature test proof");
      final path = _string(proof, "file");
      final markers = _strings(proof["markers"], "$feature source markers");
      _require(File(path).existsSync(), "$feature references missing $path");
      _require(
        ownedIntegrationTests.contains(path),
        "$feature must reference an owned integration test: $path",
      );
      _require(markers.isNotEmpty, "$feature must have source markers");
      _require(
        markers.toSet().length == markers.length,
        "$feature source markers must be unique",
      );
      final source = File(path).readAsStringSync();
      for (final marker in markers) {
        _require(
          marker.length >= 8 && source.contains(marker),
          "$feature source marker is missing from $path: $marker",
        );
      }
    }
  }

  for (final path in [
    "tool/test_certs/ca.pem",
    "tool/test_certs/server.pem",
    "tool/test_certs/server.key",
  ]) {
    _require(
      File(path).existsSync(),
      "Checked in test certificate missing: $path",
    );
  }
  _require(!mainGo.contains("apt-get"), "Dagger must not resolve apt packages");
  _require(
    !mainGo.contains('"apk", "add"'),
    "Dagger must not resolve apk packages",
  );
  _require(!mainGo.contains("openssl"), "Dagger must not resolve OpenSSL");
}

Set<String> _discoverIntegrationTests() {
  final discovered = <String>{};
  for (final directory in [
    "packages/nats_core/test/integration",
    "packages/nats_jetstream/test/integration",
    "packages/nats_core/test/browser",
  ]) {
    for (final entity in Directory(directory).listSync(recursive: true)) {
      if (entity is! File || !entity.path.endsWith("_test.dart")) continue;
      discovered.add(entity.path.replaceAll(Platform.pathSeparator, "/"));
    }
  }
  return discovered;
}

bool _isIntegrationTest(String path) {
  return path.endsWith("_test.dart") &&
      (path.contains("/test/integration/") || path.contains("/test/browser/"));
}

String _daggerCheckBody(String mainGo, String check) {
  final signature = "// +check\nfunc (m *NatsDart) $check(";
  final start = mainGo.indexOf(signature);
  _require(start >= 0, "Dagger check $check is missing");
  final nextFunction = mainGo.indexOf(
    "\nfunc (m *NatsDart)",
    start + signature.length,
  );
  return mainGo.substring(
    start,
    nextFunction < 0 ? mainGo.length : nextFunction,
  );
}

Map<String, Object?> _object(Object? value, String name) {
  if (value is! Map<String, Object?>) {
    throw FormatException("$name must be an object");
  }
  return value;
}

String _string(Map<String, Object?> object, String name) {
  final value = object[name];
  if (value is! String || value.isEmpty) {
    throw FormatException("$name must be a nonempty string");
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

void _require(bool condition, String message) {
  if (!condition) throw StateError(message);
}
