import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/server_duration.dart";
import "package:nats_jetstream/src/timestamp.dart";

/// Predefined schedules supported by NATS Server.
enum JetStreamPredefinedSchedule {
  /// Once each year.
  yearly("@yearly"),

  /// Once each month.
  monthly("@monthly"),

  /// Once each week.
  weekly("@weekly"),

  /// Once each day.
  daily("@daily"),

  /// Once each hour.
  hourly("@hourly");

  const JetStreamPredefinedSchedule(this.wireValue);

  /// Protocol header value.
  final String wireValue;
}

/// Validated schedule pattern and optional time zone.
final class JetStreamSchedule {
  /// Creates a one shot UTC schedule.
  factory JetStreamSchedule.at(DateTime time) {
    if (!time.isUtc) {
      throw ArgumentError.value(time, "time", "Must be a UTC DateTime");
    }
    return JetStreamSchedule._(
      "@at ${serializeJetStreamRfc3339(time, "time")}",
      null,
      2,
    );
  }

  /// Creates a fixed interval schedule with a minimum of one second.
  factory JetStreamSchedule.every(Duration interval) {
    if (interval < const Duration(seconds: 1)) {
      throw ArgumentError.value(interval, "interval", "Must be at least 1s");
    }
    jetStreamServerDurationNanoseconds(interval, "interval");
    return JetStreamSchedule._("@every ${interval.inMicroseconds}us", null, 2);
  }

  /// Creates a six field cron schedule.
  factory JetStreamSchedule.cron(String expression, {String? timeZone}) {
    if (expression.trim().split(RegExp(r"\s+")).length != 6) {
      throw ArgumentError.value(
        expression,
        "expression",
        "Must contain six cron fields",
      );
    }
    _validateHeaderValue(expression, "expression");
    if (timeZone != null) {
      _validateHeaderValue(timeZone, "timeZone");
      if (RegExp(r"^[+-]\d{2}:?\d{2}$").hasMatch(timeZone)) {
        throw ArgumentError.value(
          timeZone,
          "timeZone",
          "Fixed offsets are not supported",
        );
      }
    }
    return JetStreamSchedule._(expression, timeZone, timeZone == null ? 2 : 4);
  }

  /// Creates a server defined calendar schedule.
  factory JetStreamSchedule.predefined(JetStreamPredefinedSchedule schedule) {
    return JetStreamSchedule._(schedule.wireValue, null, 2);
  }

  const JetStreamSchedule._(this.pattern, this.timeZone, this.minimumApiLevel);

  /// Value for `Nats-Schedule`.
  final String pattern;

  /// Value for `Nats-Schedule-Time-Zone`, when present.
  final String? timeZone;

  /// Minimum server API level for this schedule form.
  final int minimumApiLevel;
}

/// TTL copied to messages produced by a schedule.
final class JetStreamScheduleTtl {
  /// Creates a positive generated message TTL.
  factory JetStreamScheduleTtl.duration(Duration duration) {
    if (duration < const Duration(seconds: 1)) {
      throw ArgumentError.value(duration, "duration", "Must be at least 1s");
    }
    jetStreamServerDurationNanoseconds(duration, "duration");
    return JetStreamScheduleTtl._("${duration.inMicroseconds}us");
  }

  /// Disables generated message expiry.
  const JetStreamScheduleTtl.never() : wireValue = "never";

  const JetStreamScheduleTtl._(this.wireValue);

  /// Protocol header value.
  final String wireValue;
}

/// Validated controls for one scheduled message definition.
final class JetStreamScheduleOptions {
  /// Creates schedule controls.
  factory JetStreamScheduleOptions({
    required JetStreamSchedule schedule,
    required String target,
    String? source,
    JetStreamScheduleTtl? generatedMessageTtl,
    bool rollupGeneratedSubject = false,
  }) {
    validatePublishSubject(target);
    if (source != null) {
      validatePublishSubject(source);
      if (source == target) {
        throw ArgumentError.value(
          source,
          "source",
          "Must differ from the target",
        );
      }
    }
    return JetStreamScheduleOptions._(
      schedule,
      target,
      source,
      generatedMessageTtl,
      rollupGeneratedSubject,
    );
  }

  const JetStreamScheduleOptions._(
    this.schedule,
    this.target,
    this.source,
    this.generatedMessageTtl,
    this.rollupGeneratedSubject,
  );

  /// Pattern controlling delivery times.
  final JetStreamSchedule schedule;

  /// Subject receiving generated messages.
  final String target;

  /// Optional subject whose latest value is sampled.
  final String? source;

  /// Optional TTL for each generated message.
  final JetStreamScheduleTtl? generatedMessageTtl;

  /// Whether generated messages replace their subject history.
  final bool rollupGeneratedSubject;

  /// Minimum server API level for these options.
  int get minimumApiLevel {
    if (rollupGeneratedSubject || schedule.timeZone != null) {
      return 4;
    }
    return schedule.minimumApiLevel;
  }
}

void _validateHeaderValue(String value, String name) {
  if (value.isEmpty) {
    throw ArgumentError.value(value, name, "Must not be empty");
  }
  NatsHeaders(entries: [MapEntry("Validation", value)]);
}
