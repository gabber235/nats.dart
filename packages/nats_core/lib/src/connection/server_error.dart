import "package:nats_core/src/error.dart";

final _publishPermission = RegExp(
  r'^permissions violation for publish to "([^"]+)"',
  caseSensitive: false,
);
final _replyPermission = RegExp(
  r'^permissions violation for publish with reply of "([^"]+)"',
  caseSensitive: false,
);
final _subscriptionPermission = RegExp(
  r'^permissions violation for subscription to "([^"]+)"'
  r'(?: using queue "([^"]+)")?',
  caseSensitive: false,
);

/// Converts protocol error text into a stable public failure type.
NatsException classifyServerError(String message) {
  final normalized = message.toLowerCase();
  if (normalized.startsWith("permissions violation")) {
    return _permissionError(message);
  }
  if (normalized.startsWith("maximum subscriptions exceeded")) {
    return NatsMaximumSubscriptionsException(message);
  }
  if (normalized == "stale connection") {
    return NatsStaleConnectionException(message);
  }
  if (normalized == "maximum connections exceeded") {
    return NatsConnectionLimitException(
      serverMessage: message,
      scope: NatsConnectionLimitScope.server,
    );
  }
  if (normalized == "maximum account active connections exceeded") {
    return NatsConnectionLimitException(
      serverMessage: message,
      scope: NatsConnectionLimitScope.account,
    );
  }
  if (normalized.startsWith("authorization violation")) {
    return NatsAuthenticationException(
      message,
      reason: NatsAuthenticationFailure.authorizationViolation,
    );
  }
  if (normalized.startsWith("authentication timeout")) {
    return NatsAuthenticationException(
      message,
      reason: NatsAuthenticationFailure.authenticationTimeout,
    );
  }
  if (normalized.startsWith("user authentication expired")) {
    return NatsAuthenticationException(
      message,
      reason: NatsAuthenticationFailure.userExpired,
    );
  }
  if (normalized.startsWith("user authentication revoked")) {
    return NatsAuthenticationException(
      message,
      reason: NatsAuthenticationFailure.userRevoked,
    );
  }
  if (normalized.startsWith("account authentication expired")) {
    return NatsAuthenticationException(
      message,
      reason: NatsAuthenticationFailure.accountExpired,
    );
  }
  return NatsUnknownServerException(message);
}

NatsPermissionException _permissionError(String message) {
  final publish = _publishPermission.firstMatch(message);
  if (publish != null) {
    return NatsPermissionException(
      serverMessage: message,
      operation: NatsPermissionOperation.publish,
      subject: publish.group(1),
    );
  }
  final reply = _replyPermission.firstMatch(message);
  if (reply != null) {
    return NatsPermissionException(
      serverMessage: message,
      operation: NatsPermissionOperation.publishReply,
      subject: reply.group(1),
    );
  }
  final subscription = _subscriptionPermission.firstMatch(message);
  if (subscription != null) {
    return NatsPermissionException(
      serverMessage: message,
      operation: NatsPermissionOperation.subscription,
      subject: subscription.group(1),
      queue: subscription.group(2),
    );
  }
  return NatsPermissionException(
    serverMessage: message,
    operation: NatsPermissionOperation.unknown,
  );
}
