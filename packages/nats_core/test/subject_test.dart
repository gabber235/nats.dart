import "package:nats_core/nats_core.dart";
import "package:test/test.dart";

void main() {
  group("publish subjects", () {
    test("accepts literal token sequences", () {
      for (final subject in [
        "a",
        "events.created",
        "_INBOX.instance.token",
        "상태.사용자.변경",
      ]) {
        expect(() => validatePublishSubject(subject), returnsNormally);
      }
    });

    test("rejects wildcards, whitespace, controls, and empty tokens", () {
      for (final subject in [
        "",
        ".a",
        "a.",
        "a..b",
        "a b",
        "a\tb",
        "a.*",
        "a.>",
      ]) {
        expect(
          () => validatePublishSubject(subject),
          throwsA(isA<NatsSubjectException>()),
          reason: subject,
        );
      }
    });

    test("rejects oversized UTF8 and non round trip Unicode", () {
      expect(
        () => validatePublishSubject("a" * 4097),
        throwsA(isA<NatsSubjectException>()),
      );
      expect(
        () => validatePublishSubject("가" * 1366),
        throwsA(isA<NatsSubjectException>()),
      );
      expect(
        () => validatePublishSubject(String.fromCharCode(0xd800)),
        throwsA(isA<NatsSubjectException>()),
      );
    });
  });

  group("subscription subjects", () {
    test("accepts full token wildcards", () {
      for (final subject in ["a", "a.*", "a.>", "*.events.>"]) {
        expect(() => validateSubscriptionSubject(subject), returnsNormally);
      }
    });

    test("rejects partial and misplaced wildcards", () {
      for (final subject in ["a*", "a.>.*", "a.>more", "a..b"]) {
        expect(
          () => validateSubscriptionSubject(subject),
          throwsA(isA<NatsSubjectException>()),
          reason: subject,
        );
      }
    });

    test("rejects oversized and non round trip subscription subjects", () {
      expect(
        () => validateSubscriptionSubject("a" * 4097),
        throwsA(isA<NatsSubjectException>()),
      );
      expect(
        () => validateSubscriptionSubject(String.fromCharCode(0xd800)),
        throwsA(isA<NatsSubjectException>()),
      );
    });
  });

  group("queue groups", () {
    test("accepts a literal value", () {
      expect(() => validateQueueGroup("workers.eu"), returnsNormally);
    });

    test("rejects empty, whitespace, controls, and wildcards", () {
      for (final queue in ["", "two words", "a\tb", "*", ">"]) {
        expect(
          () => validateQueueGroup(queue),
          throwsA(isA<NatsSubjectException>()),
          reason: queue,
        );
      }
    });

    test("rejects oversized and non round trip queue groups", () {
      expect(
        () => validateQueueGroup("q" * 4097),
        throwsA(isA<NatsSubjectException>()),
      );
      expect(
        () => validateQueueGroup(String.fromCharCode(0xd800)),
        throwsA(isA<NatsSubjectException>()),
      );
    });
  });
}
