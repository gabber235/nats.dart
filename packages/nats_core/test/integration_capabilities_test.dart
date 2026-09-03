import "package:test/test.dart";

import "support/integration_capabilities.dart";

void main() {
  const capability = "NATS_EXPECTS_MESSAGE_TRACING";

  test("message tracing gate accepts explicit manifest capability values", () {
    expect(
      requiredIntegrationCapability(const {capability: "true"}, capability),
      isTrue,
    );
    expect(
      requiredIntegrationCapability(const {capability: "false"}, capability),
      isFalse,
    );
  });

  test("message tracing gate fails closed without a known capability", () {
    expect(
      () => requiredIntegrationCapability(const {}, capability),
      throwsStateError,
    );
    expect(
      () => requiredIntegrationCapability(const {capability: "1"}, capability),
      throwsStateError,
    );
  });
}
