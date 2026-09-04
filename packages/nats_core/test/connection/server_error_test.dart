import "package:nats_core/nats_core.dart";
import "package:nats_core/src/connection/server_error.dart";
import "package:test/test.dart";

void main() {
  test("permission errors preserve operation, subject, and queue", () {
    expect(
      classifyServerError('Permissions Violation for Publish to "orders.a"'),
      isA<NatsPermissionException>()
          .having(
            (error) => error.operation,
            "operation",
            NatsPermissionOperation.publish,
          )
          .having((error) => error.subject, "subject", "orders.a"),
    );
    expect(
      classifyServerError(
        'Permissions Violation for Subscription to "orders.*" '
        'using queue "workers"',
      ),
      isA<NatsPermissionException>()
          .having(
            (error) => error.operation,
            "operation",
            NatsPermissionOperation.subscription,
          )
          .having((error) => error.subject, "subject", "orders.*")
          .having((error) => error.queue, "queue", "workers"),
    );
    expect(
      classifyServerError(
        'Permissions Violation for Publish with Reply of "_INBOX.x"',
      ),
      isA<NatsPermissionException>()
          .having(
            (error) => error.operation,
            "operation",
            NatsPermissionOperation.publishReply,
          )
          .having((error) => error.subject, "subject", "_INBOX.x"),
    );
  });

  test("transient and connection limit errors have stable types", () {
    expect(
      classifyServerError("Maximum Subscriptions Exceeded"),
      isA<NatsMaximumSubscriptionsException>(),
    );
    expect(
      classifyServerError("Stale Connection"),
      isA<NatsStaleConnectionException>(),
    );
    final serverLimit = classifyServerError("Maximum Connections Exceeded");
    expect(
      serverLimit,
      isA<NatsConnectionLimitException>()
          .having(
            (error) => error.scope,
            "scope",
            NatsConnectionLimitScope.server,
          )
          .having(
            (error) => error.message,
            "message",
            "server connection limit exceeded",
          ),
    );
    final accountLimit = classifyServerError(
      "Maximum Account Active Connections Exceeded",
    );
    expect(
      accountLimit,
      isA<NatsConnectionLimitException>()
          .having(
            (error) => error.scope,
            "scope",
            NatsConnectionLimitScope.account,
          )
          .having(
            (error) => error.message,
            "message",
            "account connection limit exceeded",
          ),
    );
  });

  test("authentication errors expose machine readable reasons", () {
    const cases = {
      "Authorization Violation":
          NatsAuthenticationFailure.authorizationViolation,
      "aUtHeNtIcAtIoN tImEoUt": NatsAuthenticationFailure.authenticationTimeout,
      "User Authentication Expired": NatsAuthenticationFailure.userExpired,
      "User Authentication Revoked": NatsAuthenticationFailure.userRevoked,
      "Account Authentication Expired":
          NatsAuthenticationFailure.accountExpired,
    };
    for (final MapEntry(key: message, value: reason) in cases.entries) {
      expect(
        classifyServerError(message),
        isA<NatsAuthenticationException>().having(
          (error) => error.reason,
          "reason",
          reason,
        ),
      );
    }
  });

  test("unknown server errors preserve exact protocol text", () {
    const message = "Future Server Rejection details";
    expect(
      classifyServerError(message),
      isA<NatsUnknownServerException>().having(
        (error) => error.serverMessage,
        "serverMessage",
        message,
      ),
    );
  });
}
