// ignore_for_file: depend_on_referenced_packages

import "dart:async";
import "dart:convert";
import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";

Future<void> main(List<String> arguments) async {
  final configuration = _SoakConfiguration.parse(arguments);
  final serverValue = Platform.environment["NATS_URL"];
  if (serverValue == null || serverValue.isEmpty) {
    throw StateError("NATS_URL is required");
  }

  final runner = _SoakRunner(configuration, NatsServer.parse(serverValue));
  await runner.run();
}

final class _SoakConfiguration {
  const _SoakConfiguration({required this.duration, required this.seed});

  factory _SoakConfiguration.parse(List<String> arguments) {
    var duration = const Duration(seconds: 15);
    var seed = 20260902;
    for (var index = 0; index < arguments.length; index += 1) {
      final argument = arguments[index];
      if (argument == "--duration-seconds" && index + 1 < arguments.length) {
        duration = Duration(
          seconds: _positiveInt(arguments[index += 1], argument),
        );
        continue;
      }
      if (argument == "--seed" && index + 1 < arguments.length) {
        seed = _nonnegativeInt(arguments[index += 1], argument);
        continue;
      }
      throw FormatException("Unknown or incomplete argument: $argument");
    }
    return _SoakConfiguration(duration: duration, seed: seed);
  }

  final Duration duration;
  final int seed;
}

final class _SoakRunner {
  _SoakRunner(this.configuration, this.server);

  final _SoakConfiguration configuration;
  final NatsServer server;
  final Map<String, int> _operationCounts = {};

  Future<void> run() async {
    final startedAt = DateTime.now().toUtc();
    final runId =
        "${configuration.seed.toRadixString(36)}"
        "${startedAt.microsecondsSinceEpoch.toRadixString(36)}";
    final prefix = "soak.$runId";
    final streamName = "SOAK${runId.toUpperCase()}";
    final bucketName = "S${runId.toUpperCase()}";
    final random = _SeededSequence(configuration.seed);
    final stopwatch = Stopwatch()..start();
    final deadline = startedAt.add(configuration.duration);
    var iteration = 0;
    NatsConnection? connection;
    NatsConnection? responderConnection;
    JetStreamContext? jetStream;
    NatsSubscription? delivery;
    StreamIterator<NatsMessage>? deliveries;
    NatsSubscription? responder;
    NatsSubscription? blackhole;
    StreamSubscription<NatsMessage>? responderListener;
    StreamSubscription<NatsMessage>? blackholeListener;
    final responderFailure = _FailureCapture();
    final responderTasks = _AsyncTaskOwner();
    var streamCreated = false;
    var bucketCreated = false;
    Object? primaryError;
    StackTrace? primaryStackTrace;

    try {
      final activeConnection = connection = await _connect(
        "nats.dart.soak.$runId",
      );
      final activeResponderConnection = responderConnection = await _connect(
        "nats.dart.soak.responder.$runId",
      );
      final activeJetStream = jetStream = JetStreamContext(activeConnection);
      final activeDelivery = delivery = await activeConnection.subscribe(
        "$prefix.core",
      );
      final activeDeliveries = deliveries = StreamIterator(
        activeDelivery.messages,
      );
      final activeResponder = responder = await activeResponderConnection
          .subscribe("$prefix.request");
      final activeBlackhole = blackhole = await activeResponderConnection
          .subscribe("$prefix.blackhole");
      responderListener = activeResponder.messages.listen((message) {
        responderTasks.add(
          _respond(activeResponderConnection, message, responderFailure),
        );
      });
      blackholeListener = activeBlackhole.messages.listen((_) {});

      await activeJetStream.createStream(
        JetStreamStreamConfig(
          name: streamName,
          subjects: ["$prefix.jetstream"],
          storage: JetStreamStorage.memory,
        ),
      );
      streamCreated = true;
      final bucket = await activeJetStream.keyValue.createBucket(
        JetStreamKeyValueBucketConfig(
          name: bucketName,
          storage: JetStreamStorage.memory,
        ),
      );
      bucketCreated = true;

      while (DateTime.now().toUtc().isBefore(deadline)) {
        final operation = random.nextInt(6);
        final operationName = switch (operation) {
          0 => "publish",
          1 => "request",
          2 => "jetstream",
          3 => "kv",
          4 => "timeout",
          _ => "connect",
        };
        try {
          await _runOperation(
            operation,
            iteration,
            prefix,
            activeConnection,
            activeJetStream,
            bucket,
            activeDeliveries,
          ).timeout(const Duration(seconds: 5));
          _operationCounts.update(
            operationName,
            (count) => count + 1,
            ifAbsent: () => 1,
          );
          iteration += 1;
          responderFailure.throwIfPresent();
          if (iteration % 100 == 0) {
            await activeConnection.flush();
            _assertQuiescent(activeConnection.statistics, iteration);
          }
        } catch (error, stackTrace) {
          Error.throwWithStackTrace(
            StateError(
              "Soak failed with seed ${configuration.seed}, iteration "
              "$iteration, operation $operationName: $error",
            ),
            stackTrace,
          );
        }
      }

      await activeConnection.flush();
      _assertQuiescent(activeConnection.statistics, iteration);
      if (iteration < 6) {
        throw StateError("Soak completed too few operations: $iteration");
      }
      _assertOperationCoverage();
    } on Object catch (error, stackTrace) {
      primaryError = error;
      primaryStackTrace = stackTrace;
    }

    final cleanupFailures = await _cleanup([
      if (responderListener != null)
        _CleanupAction("cancel responder listener", responderListener.cancel),
      if (blackholeListener != null)
        _CleanupAction("cancel blackhole listener", blackholeListener.cancel),
      _CleanupAction("finish responder tasks", responderTasks.close),
      if (responder != null)
        _CleanupAction("unsubscribe responder", responder.unsubscribe),
      if (blackhole != null)
        _CleanupAction("unsubscribe blackhole", blackhole.unsubscribe),
      if (delivery != null)
        _CleanupAction("unsubscribe delivery", delivery.unsubscribe),
      if (deliveries != null)
        _CleanupAction("cancel delivery iterator", deliveries.cancel),
      if (bucketCreated && jetStream != null)
        _CleanupAction(
          "delete KV bucket",
          () => jetStream!.keyValue.deleteBucket(bucketName),
        ),
      if (streamCreated && jetStream != null)
        _CleanupAction(
          "delete stream",
          () => jetStream!.deleteStream(streamName),
        ),
      if (responderConnection != null)
        _CleanupAction("close responder connection", responderConnection.close),
      if (connection != null)
        _CleanupAction("close primary connection", connection.close),
    ]);
    stopwatch.stop();

    if (primaryError != null) {
      if (cleanupFailures.isEmpty) {
        Error.throwWithStackTrace(primaryError, primaryStackTrace!);
      }
      Error.throwWithStackTrace(
        _SoakFailure(primaryError, cleanupFailures),
        primaryStackTrace!,
      );
    }
    if (cleanupFailures.isNotEmpty) {
      throw _SoakCleanupFailure(cleanupFailures);
    }

    stdout.writeln(
      const JsonEncoder.withIndent("  ").convert({
        "seed": configuration.seed,
        "requestedDurationSeconds": configuration.duration.inSeconds,
        "elapsedMilliseconds": stopwatch.elapsedMilliseconds,
        "iterations": iteration,
        "operations": _operationCounts,
        "completedAt": DateTime.now().toUtc().toIso8601String(),
      }),
    );
  }

  Future<void> _runOperation(
    int operation,
    int iteration,
    String prefix,
    NatsConnection connection,
    JetStreamContext jetStream,
    JetStreamKeyValue bucket,
    StreamIterator<NatsMessage> deliveries,
  ) async {
    final payload = <int>[
      iteration & 0xff,
      (iteration >> 8) & 0xff,
      (iteration >> 16) & 0xff,
    ];
    switch (operation) {
      case 0:
        final next = deliveries.moveNext();
        await connection.publish("$prefix.core", payload);
        await connection.flush();
        if (!await next || !_sameBytes(deliveries.current.payload, payload)) {
          throw StateError("Core publication payload mismatch");
        }
      case 1:
        final response = await connection.request("$prefix.request", payload);
        if (!_sameBytes(response.payload, payload.reversed.toList())) {
          throw StateError("Request payload mismatch");
        }
      case 2:
        final acknowledgement = await jetStream.publish(
          "$prefix.jetstream",
          payload,
          options: JetStreamPublishOptions(
            messageId: "${configuration.seed}.$iteration",
          ),
        );
        if (acknowledgement.stream.isEmpty || acknowledgement.duplicate) {
          throw StateError("Unexpected JetStream acknowledgement");
        }
      case 3:
        final key = "key.${iteration % 32}";
        final revision = await bucket.put(key, payload);
        final entry = await bucket.get(key);
        if (entry?.revision != revision ||
            !_sameBytes(entry?.value ?? const [], payload)) {
          throw StateError("KV value mismatch");
        }
      case 4:
        try {
          await connection.request(
            "$prefix.blackhole",
            payload,
            timeout: const Duration(milliseconds: 20),
          );
          throw StateError("Blackhole request unexpectedly received a reply");
        } on NatsTimeoutException {
          if (connection.statistics.activeRequests != 0) {
            throw StateError("Timed out request remained active");
          }
        }
      case 5:
        final transient = await _connect(
          "nats.dart.soak.transient.${configuration.seed}.$iteration",
        );
        await transient.flush();
        await transient.close();
        await transient.closed;
        if (transient.isConnected) {
          throw StateError("Transient connection remained connected");
        }
    }
  }

  Future<NatsConnection> _connect(String name) =>
      NatsConnection.connect(NatsOptions(servers: [server], name: name));

  void _assertQuiescent(NatsConnectionStatistics statistics, int iteration) {
    if (statistics.activeRequests != 0 ||
        statistics.subscriptionQueuedMessages != 0 ||
        statistics.reconnectRetainedMessages != 0 ||
        statistics.outstandingPings != 0) {
      throw StateError(
        "Retained work after iteration $iteration: "
        "requests ${statistics.activeRequests}, queued messages "
        "${statistics.subscriptionQueuedMessages}, reconnect messages "
        "${statistics.reconnectRetainedMessages}, pings "
        "${statistics.outstandingPings}",
      );
    }
  }

  void _assertOperationCoverage() {
    const required = {
      "publish",
      "request",
      "jetstream",
      "kv",
      "timeout",
      "connect",
    };
    final missing = required.difference(_operationCounts.keys.toSet());
    if (missing.isNotEmpty) {
      throw StateError("Soak did not exercise: ${missing.join(", ")}");
    }
  }
}

Future<void> _respond(
  NatsConnection connection,
  NatsMessage message,
  _FailureCapture failure,
) async {
  try {
    await connection.publish(
      message.replySubject!,
      message.payload.reversed.toList(),
    );
  } on Object catch (error, stackTrace) {
    failure.capture(error, stackTrace);
  }
}

final class _FailureCapture {
  Object? _error;
  StackTrace? _stackTrace;

  void capture(Object error, StackTrace stackTrace) {
    _error ??= error;
    _stackTrace ??= stackTrace;
  }

  void throwIfPresent() {
    final error = _error;
    if (error != null) {
      Error.throwWithStackTrace(error, _stackTrace!);
    }
  }
}

final class _AsyncTaskOwner {
  final Set<Future<void>> _tasks = {};

  void add(Future<void> task) {
    _tasks.add(task);
    unawaited(task.whenComplete(() => _tasks.remove(task)));
  }

  Future<void> close() async {
    while (_tasks.isNotEmpty) {
      await Future.wait(_tasks.toList(growable: false));
    }
  }
}

final class _CleanupAction {
  const _CleanupAction(this.name, this.run);

  final String name;
  final Future<void> Function() run;
}

final class _CleanupFailure {
  const _CleanupFailure(this.action, this.error, this.stackTrace);

  final String action;
  final Object error;
  final StackTrace stackTrace;

  @override
  String toString() => "$action: $error";
}

final class _SoakFailure implements Exception {
  const _SoakFailure(this.primary, this.cleanupFailures);

  final Object primary;
  final List<_CleanupFailure> cleanupFailures;

  @override
  String toString() =>
      "$primary; cleanup failures: ${cleanupFailures.join("; ")}";
}

final class _SoakCleanupFailure implements Exception {
  const _SoakCleanupFailure(this.failures);

  final List<_CleanupFailure> failures;

  @override
  String toString() => "Soak cleanup failed: ${failures.join("; ")}";
}

Future<List<_CleanupFailure>> _cleanup(List<_CleanupAction> actions) async {
  final failures = <_CleanupFailure>[];
  for (final action in actions) {
    try {
      await action.run().timeout(const Duration(seconds: 5));
    } on Object catch (error, stackTrace) {
      failures.add(_CleanupFailure(action.name, error, stackTrace));
    }
  }
  return failures;
}

final class _SeededSequence {
  _SeededSequence(int seed) : _state = seed & 0x7fffffff;

  int _state;

  int nextInt(int upperBound) {
    _state = (1103515245 * _state + 12345) & 0x7fffffff;
    return _state % upperBound;
  }
}

int _positiveInt(String value, String name) {
  final parsed = int.tryParse(value);
  if (parsed == null || parsed < 1) {
    throw FormatException("$name must be a positive integer");
  }
  return parsed;
}

int _nonnegativeInt(String value, String name) {
  final parsed = int.tryParse(value);
  if (parsed == null || parsed.isNegative) {
    throw FormatException("$name must be a nonnegative integer");
  }
  return parsed;
}

bool _sameBytes(List<int> left, List<int> right) {
  if (left.length != right.length) return false;
  for (var index = 0; index < left.length; index += 1) {
    if (left[index] != right[index]) return false;
  }
  return true;
}
