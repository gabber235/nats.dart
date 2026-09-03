import "dart:async";
import "dart:io";
import "dart:math";

import "package:crypto/crypto.dart";

final class NativeNatsServer {
  const NativeNatsServer({
    required this.archiveKey,
    required this.archiveUrl,
    required this.expectedSha256,
    required this.actualSha256,
    required this.binary,
    required this.version,
  });

  final String archiveKey;
  final String archiveUrl;
  final String expectedSha256;
  final String actualSha256;
  final String binary;
  final String version;

  Map<String, Object?> toJson() => {
    "archiveKey": archiveKey,
    "archiveUrl": archiveUrl,
    "expectedSha256": expectedSha256,
    "actualSha256": actualSha256,
    "binaryName": Uri.file(binary).pathSegments.last,
    "version": version,
  };
}

Future<NativeNatsServer> installNativeNatsServer(
  Map<String, Object?> manifest,
) async {
  final server = _object(manifest["natsServer"], "natsServer");
  final requiredVersion = _string(server, "required");
  final archives = _object(
    manifest["nativeServerArchives"],
    "nativeServerArchives",
  );
  final archiveKey = "${_platformKey()}-${await _architecture()}";
  final archive = _object(archives[archiveKey], "archive $archiveKey");
  final url = _string(archive, "url");
  final expectedSha256 = _string(archive, "sha256");

  final suffix = Uri.parse(url).pathSegments.last.endsWith(".zip")
      ? ".zip"
      : ".tar.gz";
  final root = Directory(
    "build/native-nats/$requiredVersion/$archiveKey-$expectedSha256",
  );
  await root.create(recursive: true);
  final archiveFile = File("${root.path}/nats-server$suffix");
  final actualSha256 = await cacheVerifiedNativeNatsArchive(
    source: Uri.parse(url),
    destination: archiveFile,
    expectedSha256: expectedSha256,
  );

  final extracted = Directory("${root.path}/extracted");
  await extracted.create(recursive: true);
  final binaryName = Platform.isWindows ? "nats-server.exe" : "nats-server";
  var binary = await _locateBinary(extracted, binaryName);
  if (binary == null) {
    final extraction = await Process.run("tar", [
      "-xf",
      archiveFile.absolute.path,
      "-C",
      extracted.absolute.path,
    ]);
    if (extraction.exitCode != 0) {
      throw StateError("NATS Server extraction failed: ${extraction.stderr}");
    }
    binary = await _locateBinary(extracted, binaryName);
  }
  if (binary == null) {
    throw StateError("NATS Server archive did not contain $binaryName");
  }
  if (!Platform.isWindows) {
    final chmod = await Process.run("chmod", ["+x", binary.absolute.path]);
    if (chmod.exitCode != 0) {
      throw StateError("Could not make NATS Server executable");
    }
  }

  final versionResult = await Process.run(binary.absolute.path, ["--version"]);
  if (versionResult.exitCode != 0) {
    throw StateError("NATS Server version inspection failed");
  }
  final version = (versionResult.stdout as String).trim();
  if (!version.contains("v$requiredVersion")) {
    throw StateError(
      "NATS Server version mismatch. Expected $requiredVersion, got $version",
    );
  }

  return NativeNatsServer(
    archiveKey: archiveKey,
    archiveUrl: url,
    expectedSha256: expectedSha256,
    actualSha256: actualSha256,
    binary: binary.absolute.path,
    version: version,
  );
}

/// Ensures [destination] contains the verified archive from [source].
///
/// Existing bytes are verified before use. A failed transfer never replaces the
/// cache. [timeout] is one absolute budget for cache validation and transfer.
Future<String> cacheVerifiedNativeNatsArchive({
  required Uri source,
  required File destination,
  required String expectedSha256,
  Duration timeout = const Duration(minutes: 2),
}) async {
  if (timeout <= Duration.zero) {
    throw ArgumentError.value(timeout, "timeout", "must be positive");
  }
  final deadline = _ArchiveDeadline(timeout, source);

  if (destination.existsSync()) {
    final cachedSha256 = await _digestFile(destination, deadline);
    if (cachedSha256 == expectedSha256) return cachedSha256;
    await deadline.wait(destination.delete);
  }

  await deadline.wait(() => destination.parent.create(recursive: true));
  final temporary = await _createTemporarySibling(destination, deadline);
  final client = HttpClient();
  RandomAccessFile? output;
  try {
    client
      ..autoUncompress = false
      ..connectionTimeout = deadline.remaining;
    final request = await deadline.wait(() => client.getUrl(source));
    request.headers.set(HttpHeaders.userAgentHeader, "nats.dart verification");
    final response = await deadline.wait(request.close);
    if (response.statusCode != HttpStatus.ok) {
      throw HttpException(
        "NATS Server download returned ${response.statusCode}",
        uri: source,
      );
    }

    output = temporary.openSync(mode: FileMode.write);
    await deadline.wait(
      () => response.forEach((chunk) {
        deadline.check();
        output!.writeFromSync(chunk);
      }),
    );
    deadline.check();
    output
      ..flushSync()
      ..closeSync();
    output = null;

    final actualSha256 = await _digestFile(temporary, deadline);
    if (actualSha256 != expectedSha256) {
      throw StateError(
        "NATS Server archive digest mismatch. "
        "Expected $expectedSha256, received $actualSha256",
      );
    }

    await deadline.wait(() => temporary.rename(destination.path));
    return actualSha256;
  } finally {
    client.close(force: true);
    _closeQuietly(output);
    _deleteTemporaryQuietly(temporary);
  }
}

void _closeQuietly(RandomAccessFile? output) {
  try {
    output?.closeSync();
  } on FileSystemException {
    // Preserve the transfer failure that caused cleanup.
  }
}

void _deleteTemporaryQuietly(File temporary) {
  try {
    if (temporary.existsSync()) temporary.deleteSync();
  } on FileSystemException {
    // Temporary names cannot become cache hits.
  }
}

Future<String> _digestFile(File file, _ArchiveDeadline deadline) async {
  final bytes = await deadline.wait(file.readAsBytes);
  deadline.check();
  final digest = sha256.convert(bytes).toString();
  deadline.check();
  return digest;
}

Future<File> _createTemporarySibling(
  File destination,
  _ArchiveDeadline deadline,
) async {
  final random = Random.secure();
  for (var attempt = 0; attempt < 3; attempt += 1) {
    deadline.check();
    final candidate = File(
      "${destination.path}.${pid.toRadixString(16)}."
      "${random.nextInt(1 << 32).toRadixString(16)}.tmp",
    );
    try {
      return await deadline.wait(() => candidate.create(exclusive: true));
    } on FileSystemException {
      if (!candidate.existsSync() || attempt == 2) rethrow;
    }
  }
  throw StateError("Could not create a temporary NATS Server archive");
}

final class _ArchiveDeadline {
  _ArchiveDeadline(this.timeout, this.source) : _elapsed = Stopwatch()..start();

  final Stopwatch _elapsed;
  final Uri source;
  final Duration timeout;

  Duration get remaining {
    final value = timeout - _elapsed.elapsed;
    if (value <= Duration.zero) {
      throw TimeoutException(
        "NATS Server archive download exceeded its total deadline for $source",
        timeout,
      );
    }
    return value;
  }

  void check() {
    remaining;
  }

  Future<T> wait<T>(Future<T> Function() operation) {
    final budget = remaining;
    return operation().timeout(
      budget,
      onTimeout: () => throw TimeoutException(
        "NATS Server archive download exceeded its total deadline for $source",
        timeout,
      ),
    );
  }
}

Future<File?> _locateBinary(Directory root, String name) async {
  await for (final entity in root.list(recursive: true, followLinks: false)) {
    if (entity is File && entity.uri.pathSegments.last == name) return entity;
  }
  return null;
}

String _platformKey() {
  if (Platform.isLinux) return "linux";
  if (Platform.isMacOS) return "macos";
  if (Platform.isWindows) return "windows";
  throw UnsupportedError(
    "Native NATS Server verification does not support "
    "${Platform.operatingSystem}",
  );
}

Future<String> _architecture() async {
  final runner = Platform.environment["RUNNER_ARCH"]?.toLowerCase();
  if (runner == "x64") return "x64";
  if (runner == "arm64") return "arm64";
  if (runner != null) {
    throw UnsupportedError("Unsupported runner architecture $runner");
  }

  if (Platform.isWindows) {
    final processor = Platform.environment["PROCESSOR_ARCHITECTURE"]
        ?.toLowerCase();
    if (processor == "amd64") return "x64";
    if (processor == "arm64") return "arm64";
    throw UnsupportedError("Unsupported Windows architecture $processor");
  }

  final uname = await Process.run("uname", ["-m"]);
  if (uname.exitCode != 0) {
    throw StateError("Could not determine native architecture");
  }
  return switch ((uname.stdout as String).trim().toLowerCase()) {
    "x86_64" => "x64",
    "amd64" => "x64",
    "aarch64" => "arm64",
    "arm64" => "arm64",
    final value => throw UnsupportedError("Unsupported architecture $value"),
  };
}

Map<String, Object?> _object(Object? value, String name) {
  if (value is! Map<String, Object?>) {
    throw FormatException("$name must be an object");
  }
  return value;
}

String _string(Map<String, Object?> object, String key) {
  final value = object[key];
  if (value is! String || value.isEmpty) {
    throw FormatException("$key must be a nonempty string");
  }
  return value;
}
