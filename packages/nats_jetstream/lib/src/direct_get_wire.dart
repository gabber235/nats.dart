import "dart:convert";

import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/src/direct_get_models.dart";
import "package:nats_jetstream/src/error.dart";
import "package:nats_jetstream/src/models.dart";
import "package:nats_jetstream/src/subject_match.dart";
import "package:nats_jetstream/src/timestamp.dart";

const _protocolHeaders = {
  "nats-stream",
  "nats-sequence",
  "nats-time-stamp",
  "nats-subject",
  "nats-num-pending",
  "nats-last-sequence",
  "nats-upto-sequence",
};
final _maximumUint64 = BigInt.parse("18446744073709551615");

/// Decodes one successful direct get response.
JetStreamStoredMessage decodeDirectStoredMessage(
  NatsMessage message,
  String requestSubject, {
  BigInt? expectedSequence,
  BigInt? minimumSequence,
  List<String> expectedSubjectPatterns = const [],
}) {
  final headers = message.headers;
  final status = headers?.statusCode;
  if (status != null) {
    throw JetStreamDirectGetException(
      subject: requestSubject,
      statusCode: status,
      description: headers?.statusDescription,
    );
  }
  if (headers == null) {
    throw JetStreamProtocolException(
      "Direct get response is missing headers",
      subject: requestSubject,
      fieldPath: "headers",
    );
  }
  final sequence = _requiredPositiveUint64Header(
    headers,
    "Nats-Sequence",
    requestSubject,
  );
  if (expectedSequence != null && sequence != expectedSequence) {
    throw JetStreamProtocolException(
      "Direct get sequence does not match the requested sequence",
      subject: requestSubject,
      fieldPath: "Nats-Sequence",
    );
  }
  if (minimumSequence != null && sequence < minimumSequence) {
    throw JetStreamProtocolException(
      "Direct get sequence precedes the requested minimum sequence",
      subject: requestSubject,
      fieldPath: "Nats-Sequence",
    );
  }
  final storedSubject = _requiredHeader(
    headers,
    "Nats-Subject",
    requestSubject,
  );
  _requireSubjectMatch(storedSubject, expectedSubjectPatterns, requestSubject);
  return JetStreamStoredMessage(
    subject: storedSubject,
    sequence: sequence,
    payload: message.payload,
    timestamp: _requiredTimestamp(headers, requestSubject),
    headers: _userHeaders(headers),
  );
}

String _requiredTimestamp(NatsHeaders headers, String requestSubject) {
  final timestamp = _requiredHeader(headers, "Nats-Time-Stamp", requestSubject);
  try {
    return validateJetStreamRfc3339(timestamp, "Nats-Time-Stamp");
  } on ArgumentError catch (error) {
    throw JetStreamProtocolException(
      "Direct get timestamp must be RFC 3339",
      subject: requestSubject,
      fieldPath: "Nats-Time-Stamp",
      cause: error,
    );
  }
}

/// Decodes and validates one ordered direct batch data response.
JetStreamStoredMessage decodeDirectBatchStoredMessage(
  NatsMessage message,
  String requestSubject,
  BigInt previousSequence, {
  BigInt? minimumSequence,
  List<String> expectedSubjectPatterns = const [],
}) {
  final headers = message.headers;
  if (headers == null) {
    return decodeDirectStoredMessage(message, requestSubject);
  }
  final lastSequence = _requiredUnsignedHeader(
    headers,
    "Nats-Last-Sequence",
    requestSubject,
  );
  _requiredUnsignedHeader(headers, "Nats-Num-Pending", requestSubject);
  if (lastSequence != previousSequence) {
    throw JetStreamProtocolException(
      "Direct batch response chain is discontinuous",
      subject: requestSubject,
      fieldPath: "Nats-Last-Sequence",
    );
  }
  return decodeDirectStoredMessage(
    message,
    requestSubject,
    minimumSequence: minimumSequence,
    expectedSubjectPatterns: expectedSubjectPatterns,
  );
}

/// Returns a raw direct response payload or throws its status.
List<int> decodeDirectPayload(
  NatsMessage message,
  String requestSubject, {
  BigInt? expectedSequence,
  BigInt? minimumSequence,
  List<String> expectedSubjectPatterns = const [],
}) {
  final headers = message.headers;
  if (headers?.statusCode case final status?) {
    throw JetStreamDirectGetException(
      subject: requestSubject,
      statusCode: status,
      description: headers?.statusDescription,
    );
  }
  if (expectedSequence != null ||
      minimumSequence != null ||
      expectedSubjectPatterns.isNotEmpty) {
    if (headers == null) {
      throw JetStreamProtocolException(
        "Direct get response is missing headers",
        subject: requestSubject,
        fieldPath: "headers",
      );
    }
    final sequence = _requiredPositiveUint64Header(
      headers,
      "Nats-Sequence",
      requestSubject,
    );
    if (expectedSequence != null && sequence != expectedSequence) {
      throw JetStreamProtocolException(
        "Direct get sequence does not match the requested sequence",
        subject: requestSubject,
        fieldPath: "Nats-Sequence",
      );
    }
    if (minimumSequence != null && sequence < minimumSequence) {
      throw JetStreamProtocolException(
        "Direct get sequence precedes the requested minimum sequence",
        subject: requestSubject,
        fieldPath: "Nats-Sequence",
      );
    }
    if (expectedSubjectPatterns.isNotEmpty) {
      _requireSubjectMatch(
        _requiredHeader(headers, "Nats-Subject", requestSubject),
        expectedSubjectPatterns,
        requestSubject,
      );
    }
  }
  return message.payload;
}

void _requireSubjectMatch(
  String subject,
  List<String> patterns,
  String requestSubject,
) {
  if (patterns.isEmpty) {
    if (matchesNatsSubjectPattern(">", subject)) return;
  } else if (patterns.any(
    (pattern) => matchesNatsSubjectPattern(pattern, subject),
  )) {
    return;
  }
  throw JetStreamProtocolException(
    "Direct get subject does not match the requested subject selector",
    subject: requestSubject,
    fieldPath: "Nats-Subject",
  );
}

/// Decodes the required end marker of one direct batch.
({BigInt pending, BigInt lastSequence, BigInt? upToSequence})
decodeDirectBatchEnd(NatsMessage message, String requestSubject) {
  final headers = message.headers;
  if (headers == null ||
      headers.statusCode != 204 ||
      message.payload.isNotEmpty) {
    final status = headers?.statusCode;
    if (status != null) {
      throw JetStreamDirectGetException(
        subject: requestSubject,
        statusCode: status,
        description: headers?.statusDescription,
      );
    }
    throw JetStreamProtocolException(
      "Direct batch is missing its end marker",
      subject: requestSubject,
      fieldPath: "status",
    );
  }
  return (
    pending: _requiredUnsignedHeader(
      headers,
      "Nats-Num-Pending",
      requestSubject,
    ),
    lastSequence: _requiredUnsignedHeader(
      headers,
      "Nats-Last-Sequence",
      requestSubject,
    ),
    upToSequence: _optionalUnsignedHeader(
      headers,
      "Nats-UpTo-Sequence",
      requestSubject,
    ),
  );
}

/// Counts the complete bytes retained for one direct response.
int directMessageRetainedBytes(NatsMessage message) =>
    utf8.encode(message.subject).length +
    utf8.encode(message.replySubject ?? "").length +
    message.payload.length +
    (message.headers?.encode().length ?? 0);

String _requiredHeader(
  NatsHeaders headers,
  String name,
  String requestSubject,
) {
  final value = headers.first(name);
  if (value == null || value.isEmpty) {
    throw JetStreamProtocolException(
      "Direct get response is missing $name",
      subject: requestSubject,
      fieldPath: name,
    );
  }
  return value;
}

BigInt _requiredUnsignedHeader(
  NatsHeaders headers,
  String name,
  String requestSubject,
) {
  final value = _requiredHeader(headers, name, requestSubject);
  final parsed = _parseUnsignedHeader(value);
  if (parsed == null) {
    throw JetStreamProtocolException(
      "Direct get response contains invalid $name",
      subject: requestSubject,
      fieldPath: name,
    );
  }
  return parsed;
}

BigInt _requiredPositiveUint64Header(
  NatsHeaders headers,
  String name,
  String requestSubject,
) {
  final parsed = _requiredUnsignedHeader(headers, name, requestSubject);
  if (parsed == BigInt.zero) {
    throw JetStreamProtocolException(
      "Direct get response contains invalid $name",
      subject: requestSubject,
      fieldPath: name,
    );
  }
  return parsed;
}

BigInt? _optionalUnsignedHeader(
  NatsHeaders headers,
  String name,
  String requestSubject,
) {
  final value = headers.first(name);
  if (value == null) return null;
  final parsed = _parseUnsignedHeader(value);
  if (parsed == null) {
    throw JetStreamProtocolException(
      "Direct get response contains invalid $name",
      subject: requestSubject,
      fieldPath: name,
    );
  }
  return parsed;
}

BigInt? _parseUnsignedHeader(String value) {
  if (value.isEmpty || value.length > 20) return null;
  for (final unit in value.codeUnits) {
    if (unit < 48 || unit > 57) return null;
  }
  final parsed = BigInt.parse(value);
  return parsed <= _maximumUint64 ? parsed : null;
}

NatsHeaders? _userHeaders(NatsHeaders headers) {
  final entries = headers.entries
      .where((entry) => !_protocolHeaders.contains(entry.key.toLowerCase()))
      .toList(growable: false);
  return entries.isEmpty ? null : NatsHeaders(entries: entries);
}
