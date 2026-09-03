import "package:nats_jetstream/nats_jetstream.dart";
import "package:test/test.dart";

void main() {
  test("schedule values reject ambiguous local time and invalid intervals", () {
    expect(() => JetStreamSchedule.at(DateTime(2030)), throwsArgumentError);
    expect(
      () => JetStreamSchedule.every(const Duration(milliseconds: 999)),
      throwsArgumentError,
    );
    expect(() => JetStreamSchedule.cron("0 0 * * *"), throwsArgumentError);
    expect(
      () => JetStreamSchedule.cron("0 0 0 * * *", timeZone: "+09:00"),
      throwsArgumentError,
    );
    expect(
      JetStreamSchedule.at(DateTime.utc(1)).pattern,
      "@at 0001-01-01T00:00:00.000Z",
    );
    expect(
      JetStreamSchedule.at(DateTime.utc(9999)).pattern,
      "@at 9999-01-01T00:00:00.000Z",
    );
    for (final year in [-1, 10000]) {
      expect(
        () => JetStreamSchedule.at(DateTime.utc(year)),
        throwsArgumentError,
      );
    }
  });

  test("latest schedule forms derive API level four", () {
    final zoned = JetStreamScheduleOptions(
      schedule: JetStreamSchedule.cron("0 0 0 * * *", timeZone: "Asia/Seoul"),
      target: "orders.ready",
    );
    final rollup = JetStreamScheduleOptions(
      schedule: JetStreamSchedule.predefined(
        JetStreamPredefinedSchedule.hourly,
      ),
      target: "orders.ready",
      rollupGeneratedSubject: true,
    );

    expect(zoned.minimumApiLevel, 4);
    expect(rollup.minimumApiLevel, 4);
  });

  test("schedule durations fit the server signed nanosecond range", () {
    const maximum = Duration(microseconds: 9223372036854775);
    const overflow = Duration(microseconds: 9223372036854776);
    expect(
      JetStreamSchedule.every(maximum).pattern,
      "@every 9223372036854775us",
    );
    expect(
      JetStreamScheduleTtl.duration(maximum).wireValue,
      "9223372036854775us",
    );
    expect(() => JetStreamSchedule.every(overflow), throwsArgumentError);
    expect(() => JetStreamScheduleTtl.duration(overflow), throwsArgumentError);
  });

  test("schedule source must differ from its target", () {
    expect(
      () => JetStreamScheduleOptions(
        schedule: JetStreamSchedule.predefined(
          JetStreamPredefinedSchedule.hourly,
        ),
        target: "orders.ready",
        source: "orders.ready",
      ),
      throwsArgumentError,
    );
  });
}
