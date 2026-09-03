import "package:nats_jetstream/src/error.dart";

/// Parsed metadata from either JetStream acknowledgement subject layout.
final class JetStreamConsumerAckSubject {
  /// Strictly parses v1 and NATS 2.14 v2 acknowledgement subjects.
  factory JetStreamConsumerAckSubject.parse(String subject) {
    if (subject.length > 4096) {
      throw const JetStreamProtocolException(
        "Consumer acknowledgement subject exceeds 4096 units",
      );
    }
    final tokens = subject.split(".");
    if (tokens.length < 9 || tokens.length > 32) {
      throw const JetStreamProtocolException(
        "Consumer acknowledgement subject has an invalid token count",
      );
    }
    if (tokens[0] != r"$JS" || tokens[1] != "ACK") {
      throw const JetStreamProtocolException(
        "Message is not a JetStream acknowledgement subject",
      );
    }
    if (tokens.length == 9) {
      return JetStreamConsumerAckSubject._(
        version: 1,
        domain: null,
        accountHash: null,
        stream: tokens[2],
        consumer: tokens[3],
        delivered: _positiveUint64(tokens[4], "delivered"),
        streamSequence: _positiveUint64(tokens[5], "streamSequence"),
        consumerSequence: _positiveUint64(tokens[6], "consumerSequence"),
        timestampNanoseconds: _positiveInt64(tokens[7], "timestampNanoseconds"),
        pending: _uint64(tokens[8], "pending"),
        extensions: const [],
      );
    }
    if (tokens.length < 11) {
      throw const JetStreamProtocolException(
        "Consumer acknowledgement subject mixes v1 and v2 layouts",
      );
    }
    return JetStreamConsumerAckSubject._(
      version: 2,
      domain: tokens[2] == "_" ? null : _opaque(tokens[2], "domain"),
      accountHash: _opaque(tokens[3], "accountHash"),
      stream: _opaque(tokens[4], "stream"),
      consumer: _opaque(tokens[5], "consumer"),
      delivered: _positiveUint64(tokens[6], "delivered"),
      streamSequence: _positiveUint64(tokens[7], "streamSequence"),
      consumerSequence: _positiveUint64(tokens[8], "consumerSequence"),
      timestampNanoseconds: _positiveInt64(tokens[9], "timestampNanoseconds"),
      pending: _uint64(tokens[10], "pending"),
      extensions: List.unmodifiable(tokens.skip(11)),
    );
  }

  const JetStreamConsumerAckSubject._({
    required this.version,
    required this.domain,
    required this.accountHash,
    required this.stream,
    required this.consumer,
    required this.delivered,
    required this.streamSequence,
    required this.consumerSequence,
    required this.timestampNanoseconds,
    required this.pending,
    required this.extensions,
  });

  /// Wire layout version, one or two.
  final int version;

  /// JetStream domain in v2, with underscore normalized to absent.
  final String? domain;

  /// Opaque account hash in v2.
  final String? accountHash;

  /// Stream name.
  final String stream;

  /// Consumer name.
  final String consumer;

  /// Delivery attempt count.
  final BigInt delivered;

  /// Stream sequence.
  final BigInt streamSequence;

  /// Consumer delivery sequence.
  final BigInt consumerSequence;

  /// Exact server timestamp in nanoseconds.
  final BigInt timestampNanoseconds;

  /// Messages pending after this delivery.
  final BigInt pending;

  /// Opaque v2 suffix tokens retained for diagnostics.
  final List<String> extensions;
}

final BigInt _maximumUint64 = BigInt.parse("18446744073709551615");
final BigInt _maximumInt64 = BigInt.parse("9223372036854775807");

BigInt _positiveUint64(String value, String field) {
  final parsed = _boundedUnsignedDecimal(value, field, _maximumUint64);
  if (parsed == BigInt.zero) {
    throw JetStreamProtocolException(
      "Consumer acknowledgement field $field must be positive",
      fieldPath: field,
    );
  }
  return parsed;
}

BigInt _uint64(String value, String field) =>
    _boundedUnsignedDecimal(value, field, _maximumUint64);

BigInt _positiveInt64(String value, String field) {
  final parsed = _boundedUnsignedDecimal(value, field, _maximumInt64);
  if (parsed == BigInt.zero) {
    throw JetStreamProtocolException(
      "Consumer acknowledgement field $field must be positive",
      fieldPath: field,
    );
  }
  return parsed;
}

BigInt _boundedUnsignedDecimal(String value, String field, BigInt maximum) {
  if (value.isEmpty || value.length > 20) {
    throw JetStreamProtocolException(
      "Consumer acknowledgement field $field is outside its wire range",
      fieldPath: field,
    );
  }
  for (final codeUnit in value.codeUnits) {
    if (codeUnit < 48 || codeUnit > 57) {
      throw JetStreamProtocolException(
        "Consumer acknowledgement field $field is not unsigned decimal",
        fieldPath: field,
      );
    }
  }
  final parsed = BigInt.parse(value);
  if (parsed > maximum) {
    throw JetStreamProtocolException(
      "Consumer acknowledgement field $field is outside its wire range",
      fieldPath: field,
    );
  }
  return parsed;
}

String _opaque(String value, String field) {
  if (value.isEmpty) {
    throw JetStreamProtocolException(
      "Consumer acknowledgement field $field is empty",
      fieldPath: field,
    );
  }
  return value;
}
