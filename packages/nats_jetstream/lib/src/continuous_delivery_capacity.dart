/// Core queue capacity reserved for coalesced continuous protocol statuses.
const continuousDeliveryStatusMessageAllowance = 2;

/// Core queue bytes reserved for coalesced continuous protocol statuses.
const continuousDeliveryStatusByteAllowance = 2048;

const _maximumSafeInteger = 9007199254740991;

/// Largest caller byte budget whose status allowance remains exact on web.
const maximumContinuousDeliveryPendingBytes =
    _maximumSafeInteger - continuousDeliveryStatusByteAllowance;

/// Adds the sole protocol message allowance to a caller message budget.
int continuousDeliveryCoreMaximumMessages(int maximumPendingMessages) {
  return maximumPendingMessages + continuousDeliveryStatusMessageAllowance;
}

/// Adds the sole protocol byte allowance to a validated caller byte budget.
int continuousDeliveryCoreMaximumBytes(int maximumPendingBytes) {
  if (maximumPendingBytes < 1 ||
      maximumPendingBytes > maximumContinuousDeliveryPendingBytes) {
    throw ArgumentError.value(
      maximumPendingBytes,
      "maximumPendingBytes",
      "Must be between 1 and $maximumContinuousDeliveryPendingBytes",
    );
  }
  return maximumPendingBytes + continuousDeliveryStatusByteAllowance;
}
