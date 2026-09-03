# nats.dart

Reliable pure Dart packages for NATS Core, JetStream, KV, and Object Store.
They target Dart and Flutter applications that need explicit lifecycle,
bounded memory, typed failures, reconnect safety, and current NATS features.

This repository is pre release. It is not published to pub.dev. APIs may change
before version 1.0. Use a pinned Git revision or a local path.

## Why this exists

NATS clients sit on a deceptively small protocol with difficult failure modes.
Reconnect, ambiguous publication, flow control, browser transports, and
JetStream leadership require explicit ownership. This implementation makes
those boundaries public instead of hiding them behind automatic retries or
unbounded queues.

Choose this project when you need:

* Pure Dart support for native applications and Flutter.
* Native TCP, TLS, TLS first, WebSocket, and secure WebSocket transports.
* Bounded subscriptions, requests, asynchronous publication, watches, and
  transfers.
* Latest JetStream features with typed API level requirements.
* KV and Object Store operations with revision and digest verification.
* Local verification through the same Dagger targets used in CI.

Do not treat the current `0.0.1` packages as stable public APIs. The official
NATS clients remain the safer choice where an officially supported language
client exists.

## Verified scope

* Dart 3.13.2 is the primary SDK. A mandatory Dart 3.11.0 lane resolves the
  locked dependency graph and executes the complete unit suite.
* Flutter 3.47.2 compiles and tests a real consumer importing both packages.
* NATS Server 2.14.6 is mandatory. Baseline lanes cover 2.12.15 and 2.10.29.
* Core covers TCP, TLS, WebSocket, reconnect, authentication, request, and
  drain on the native VM plus real Chromium.
* JetStream covers streams, consumers, publish, KV, and Object Store, including
  three node failure and leadership scenarios.
* Official `nats.go` 1.53.1 exchanges Core, JetStream, KV, and Object Store
  binary data in both directions.

The full mandatory suite runs in Dagger. Every integration test is assigned to
at least one manifest declared lane. Each lane validates its prerequisites
before starting tests, so a missing server or binary fails instead of silently
skipping coverage. A green container lane proves its declared Linux and NATS
topology. It does not substitute for native macOS or Windows verification.

## Add the packages

Until publication, use a pinned Git revision:

```yaml
dependencies:
  nats_core:
    git:
      url: https://github.com/gabber235/nats.dart.git
      ref: YOUR_PINNED_REVISION
      path: packages/nats_core
  nats_jetstream:
    git:
      url: https://github.com/gabber235/nats.dart.git
      ref: YOUR_PINNED_REVISION
      path: packages/nats_jetstream

dependency_overrides:
  nats_core:
    git:
      url: https://github.com/gabber235/nats.dart.git
      ref: YOUR_PINNED_REVISION
      path: packages/nats_core
```

The override is required while these unpublished packages live in one Dart
workspace. It makes `nats_jetstream` use the same `nats_core` Git revision as
the application. Use one exact commit for all three references. For local
development, replace each Git block with a path to the matching package
directory. Run `dart pub get` or `flutter pub get` afterward.

## Core quick start

Start a NATS server, then run:

```dart
import "dart:convert";

import "package:nats_core/nats_core.dart";

Future<void> main() async {
  final connection = await NatsConnection.connect(
    NatsOptions(
      servers: [NatsServer.parse("nats://127.0.0.1:4222")],
      name: "example",
    ),
  );

  final subscription = await connection.subscribe("events.created");
  final received = subscription.messages.first;

  await connection.publish(
    "events.created",
    utf8.encode("42"),
  );
  await connection.flush();

  final message = await received;
  print(utf8.decode(message.payload));

  await subscription.unsubscribe();
  await connection.drain();
}
```

Expected output:

```text
42
```

Use `drain` for graceful shutdown. It rejects new subscriptions and requests,
then drains existing subscriptions while publication remains available for
handler replies. It finally rejects new publications, waits for accepted
requests, flushes, and closes. Use `close` when buffered work must be discarded
immediately.

Respond through the connection so a missing reply subject fails locally with
`NatsMissingReplySubjectException` before publication:

```dart
final service = await connection.subscribe("service.reverse");
final listener = service.messages.listen((request) async {
  await connection.reply(request, request.payload.reversed.toList());
});
```

`connection.serverInfo` exposes typed identity, version, client, protocol,
JetStream, TLS, header, payload, lame duck, and discovery state. Its
`rawFields` retains deeply immutable unknown fields for forward compatibility.
Identity from the initial INFO remains authoritative for that connection
generation.

### Payload sizing

`NatsOptions.maximumInboundPayloadBytes` is the client memory ceiling for one
message, including headers. The effective publication maximum is the lesser of
this value and the server `max_payload`. This keeps publishing and parsing on
one capacity contract.

The default is 8 MiB. Raise it only when the server permits larger messages and
the relevant `subscriptionQueueBytes` values can retain them. A publication
above the effective limit fails locally with `NatsMaxPayloadException` before
any bytes reach the transport.

### Authentication

Authentication is explicit and resolved again for every connection generation.
Use the matching constructor for user and password, token, NKey, JWT credentials
content, or an asynchronous provider. Native applications can load a standard
credentials file without parsing it themselves:

```dart
final authentication = await loadNatsCredentialsFile("/run/secrets/user.creds");
final connection = await NatsConnection.connect(
  NatsOptions(
    servers: [NatsServer.parse("tls://nats.example:4222")],
    authentication: authentication,
  ),
);
```

Browser applications can pass credentials content to
`NatsAuthentication.credentials`. Diagnostics redact authentication values.
Server URLs reject embedded credentials. Asynchronous providers receive the
connection attempt deadline and a cancellation future. Providers should stop
owned work when cancellation completes.

Reconnect closes after the same typed authentication reason occurs twice by
default. Set `NatsReconnectPolicy.retryAuthenticationFailures` only when a
provider deliberately rotates credentials and the configured attempt policy is
the intended recovery bound.

When server permissions grant replies through a caller scoped namespace,
configure `NatsOptions.requestInboxPrefix`. The value must be a literal subject
prefix without wildcards. Every request uses one shared wildcard subscription
below that prefix and one random child subject per reply:

```dart
final connection = await NatsConnection.connect(
  NatsOptions(
    servers: [NatsServer.parse("nats://127.0.0.1:4222")],
    requestInboxPrefix: "_INBOX.user-42",
  ),
);
```

The server must allow subscription to `_INBOX.user-42.*` and publication by
responders to `_INBOX.user-42.>`. Omit the option for the default random
connection namespace.

### Server pool control

`getServers()` returns an immutable credential free snapshot containing
configured and discovered servers. `setServers()` validates the complete
replacement before changing reconnect behavior. `reconnectToServer` can select
one server from each immutable reconnect snapshot and add an interruptible delay
bounded by `NatsReconnectPolicy.maximumDelay`. Without a selector, reconnects
remain round robin. A valid nonempty INFO topology replaces prior implicit
discoveries atomically while configured seeds and the active endpoint remain
owned. Empty or malformed topology updates leave the pool unchanged.
`maximumDiscoveredServers` defaults to 4096. An update above that ceiling is
rejected atomically, preserving the prior reconnect pool.

## JetStream quick start

Start NATS with JetStream enabled through `nats-server -js`, then run:

```dart
import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";

Future<void> main() async {
  final connection = await NatsConnection.connect(
    NatsOptions(
      servers: [NatsServer.parse("nats://127.0.0.1:4222")],
    ),
  );
  final jetStream = JetStreamContext(connection);

  await jetStream.createStream(
    JetStreamStreamConfig(
      name: "EVENTS",
      subjects: const ["events.>"],
    ),
  );

  final acknowledgement = await jetStream.publish(
    "events.created",
    const [1, 2, 3],
    options: JetStreamPublishOptions(messageId: "event-42"),
  );
  final stored = await jetStream.getMessage(
    "EVENTS",
    acknowledgement.sequence,
  );

  print(stored.payload);
  await jetStream.deleteStream("EVENTS");
  await connection.drain();
}
```

Stable message identifiers make caller directed retry after an ambiguous
publication safe through server deduplication. The client never assumes that a
lost acknowledgement means the server rejected the message. Configure the
stream duplicate window to exceed the complete publication retry horizon.

For a JetStream domain, pass its complete API prefix to the context:

```dart
final jetStream = JetStreamContext(
  connection,
  apiPrefix: r"$JS.EDGE.API",
);
```

KV mutations then use the domain publication prefix while stored subject
filters and optimistic concurrency headers remain unprefixed. External KV
mirrors use their declared external API prefix. Object Store API requests use
the domain prefix, but chunk and metadata publications remain on their standard
`$O` subjects because NATS Server domains do not map prefixed Object Store data
subjects. Cross domain Object Store access requires explicit server mappings.

## Main APIs

* `NatsConnection` owns connection, reconnect, request multiplexing, flush,
  drain, and close.
* Connection and ordered consumer diagnostic streams preserve order for active
  listeners. A paused listener keeps only the latest event. They are not audit
  logs.
* `NatsSubscription` exposes a bounded message stream plus explicit unsubscribe
  and drain operations.
* `JetStreamContext` provides acknowledged publication and typed managers.
* `JetStreamConsumerManager` creates, updates, inspects, and deletes consumers.
* `JetStreamKeyValueManager` and `JetStreamObjectStoreManager` own bucket and
  object operations.
* `JetStreamStreamManager` owns stream CRUD, direct reads, bounded snapshot and
  restore, and safe cluster administration.

All caller supplied payloads, headers, options, and metadata retained across an
asynchronous boundary are copied into immutable snapshots. Network operations
own deadlines. Queues own message and byte limits. Stream producers must treat
emitted byte lists as immutable. The client copies each event immediately when
it is delivered, before awaiting network work.

### Arbitrary precision counters

`incrementCounter` accepts Dart `BigInt` increments and preserves the complete
counter value returned by NATS Server. `JetStreamContext.maximumCounterDigits`
is the CPU and allocation ceiling for one decimal increment or acknowledgement.
It defaults to 4096 digits and is further limited by the active Core inbound
payload ceiling. Oversized increments fail before publication. Raise the digit
limit only when the server payload configuration and application workload need
larger counters.

JetStream sequence, size, count, page, and account fields retain their exact
wire value as `BigInt`. This includes `JetStreamStreamState.consumerCount`.

## KV codecs and source transforms

Create a normal bucket, then derive a handle with transparent codecs. The
backing stream is unchanged. Every participant using that bucket must therefore
select compatible codecs.

```dart
final raw = await jetStream.keyValue.createBucket(
  JetStreamKeyValueBucketConfig(name: "CONTACTS"),
);
final contacts = raw.withCodecs(
  const JetStreamKeyValueCodecs(
    key: JetStreamKeyValueBase64KeyCodec(),
    value: JetStreamKeyValueBase64ValueCodec(),
  ),
);

await contacts.putString("Acme Inc.primary", "info@acme.example");
print(await contacts.getString("Acme Inc.primary"));
```

Key codecs receive the complete logical key. Built in Base64 encoding operates
on each dot delimited token and preserves `*` and `>` tokens, so filtered
history, keys, wildcard mutations, and watches remain usable. Value codecs
receive immutable byte snapshots. Codec chains encode in declaration order and
decode in reverse order. The path codec maps path separators to subject tokens
and represents an absolute root with `_root_` by default. It reversibly escapes
dots, percent signs, subject sensitive bytes, and empty interior segments.

KV sources without explicit transforms keep automatic KV to KV mapping.
Supplying `subjectTransforms` preserves the source stream name and mappings
exactly. This permits a normal JetStream stream to feed a KV bucket. Mirrors
retain automatic `KV_` normalization and direct read behavior.

## Object Store fast ingest

Object uploads use sequential acknowledged chunks by default. NATS Server 2.14
can accept chunks through fast batch ingest when both the bucket and each put
explicitly opt in:

```dart
final bucket = await jetStream.objectStore.createStore(
  JetStreamObjectStoreConfig(
    name: "ASSETS",
    experimentalFastIngest: true,
  ),
);

await bucket.putBytes(
  "bundle.bin",
  bytes,
  mode: JetStreamObjectPutMode.experimentalFastIngest,
);
```

This mode is experimental. It fences the complete upload to one connection
generation and rejects any reported internal gap. Metadata becomes visible
only after the server confirms the exact chunk batch. Metadata still uses the
normal revision constraint and readback verification. Omit `mode` to retain
the ordinary sequential path, including on a bucket that permits fast ingest.

## Failure handling

Core failures derive from `NatsException`. JetStream management failures use
`JetStreamApiException`, whose numeric `errorCode` is the stable branching key.
Descriptions are diagnostics and may change between server releases.

```dart
try {
  await jetStream.streamInfo("MISSING");
} on JetStreamApiException catch (error) {
  if (error.errorCode == 10059) {
    print("stream does not exist");
  } else {
    rethrow;
  }
}
```

Reconnect does not make every operation retryable. Requests, restores, cluster
mutations, and publications without a stable deduplication identifier may have
an ambiguous outcome after transport loss. Reconcile server state before
retrying them.

## Development

Requirements:

* Docker or another Dagger compatible container engine.
* Dagger 0.21.9.
* Dart 3.13.2 for direct host commands.

Run the same mandatory container checks as CI:

```sh
dagger check
```

Useful focused lanes:

```sh
dagger call unit stdout
dagger call api-docs stdout
dagger call external-git-consumer stdout
dagger call external-flutter-git-consumer stdout
dagger call minimum-sdk stdout
dagger call flutter-consumer stdout
dagger call core-latest stdout
dagger call jet-stream-latest stdout
dagger call jet-stream-protocol-latest stdout
dagger call jet-stream-cluster-latest stdout
dagger call ordered-consumer-latest stdout
dagger call object-store-latest stdout
dagger call reconnect-latest stdout
dagger call reconnect-suspend-latest stdout
dagger call stream-transfer-latest stdout
dagger call go-interop-latest stdout
dagger call tls-latest
dagger call auth-latest stdout
dagger call web-socket-latest stdout
dagger call soak-smoke stdout
```

Run the reproducible 30 minute resilience soak locally with the same function
used by scheduled CI:

```sh
dagger call soak --duration-seconds=1800 --seed=20260902 stdout
```

The seed and failing iteration are printed on failure. The short smoke uses the
same operation engine and runs as a mandatory Dagger check.

The Flutter consumer check uses the exact Flutter 3.47.2 Linux AMD64 image.
GitHub Linux runners execute it natively. Apple Silicon executes it through
container emulation because the official Flutter Linux release is available
only for AMD64. Each Flutter subprocess has a 15 minute bound. The check
asserts the Flutter version, framework revision, effective user, and HOME
before compiling both public packages. Its application lockfile is checked in
and enforced.

Fast host feedback:

```sh
dart format --output=none --set-exit-if-changed packages test tool
dart analyze --fatal-infos --fatal-warnings
dart run tool/run_unit_tests.dart
dart doc --output build/docs/nats-core packages/nats_core
dart doc --output build/docs/nats-jetstream packages/nats_jetstream
```

Run the complete host verification used by native CI:

```sh
dart run tool/run_native_verification.dart
```

This command downloads the pinned official NATS Server archive when its
verified cache is absent. It enforces dependency resolution, analysis, unit
tests, reconnect integration, and native security integration against that
executable. Mandatory Dagger verification separately owns formatting and API
documentation generation.

Version pins and integration ownership live in
[`tool/compatibility.json`](tool/compatibility.json). The browser lane uses a
digest pinned official Selenium image containing Chromium. TLS test
certificates are checked in under `tool/test_certs` with a test only private
key. It proves WS, WSS, exact origin allow and deny, certificate rejection,
repeated NATS message headers, official nats.go exchange, discovered server
reconnect, path retention, and subscription replay. Chromium owns certificate
trust and exposes only a generic WebSocket failure to Dart. Mutual TLS and
account credentials are generated inside Dagger for each content addressed
fixture execution. Native CI instead uses unmistakably test only credentials
and private keys from
[`tool/native_security_fixtures`](tool/native_security_fixtures). They protect
no service or production data. Every single server Dagger lane requires ordinary
health, optional JetStream health, and a separate authenticated connect plus
flush before tests begin. The probe has one 20 second deadline and reports its
last observation while Dagger retains server logs in the trace. Native CI
downloads the exact official NATS Server 2.14.6 archive for its operating
system and architecture, verifies SHA256 before extraction, and runs portable
reconnect, TLS, TLS first, mutual TLS, WS, WSS, username, token, NKey, and JWT
credentials verification against the real executable. Native runners need
neither Go nor OpenSSL. Linux and macOS also run the process suspension lane.
Dagger keeps its corresponding container lanes mandatory. Sanitized local
native verification evidence is written under `build/verification-evidence`.
It is not uploaded.
CI calls Dagger from
[`.github/workflows/verification.yml`](.github/workflows/verification.yml).
The scheduled soak lives in
[`.github/workflows/resilience.yml`](.github/workflows/resilience.yml).
Runnable package examples live in
[`packages/nats_core/example`](packages/nats_core/example) and
[`packages/nats_jetstream/example`](packages/nats_jetstream/example).

## Security

Never place credentials in server URLs, logs, test names, or exception text.
Authentication values use redacted types, but applications still own secure
secret storage. Report suspected vulnerabilities privately to the repository
owner before opening a public issue.

## License

Licensed under the [Apache License 2.0](LICENSE).
