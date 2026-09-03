@TestOn("vm")
library;

import "dart:async";
import "dart:io";

import "package:nats_core/nats_core.dart";
import "package:test/test.dart";

const _seed = "SUAKYRHVIOREXV7EUZTBHUHL7NUMHPMAS7QMDU3GTIUWEI5LDNOXD43IZY";

void main() {
  final serverUrl = Platform.environment["NATS_AUTH_URL"];
  final tokenServerUrl = Platform.environment["NATS_TOKEN_AUTH_URL"];
  final credentialsServerUrl = Platform.environment["NATS_CREDENTIALS_URL"];
  final credentialsFile = Platform.environment["NATS_CREDENTIALS_FILE"];
  final skipReason =
      serverUrl == null ||
          tokenServerUrl == null ||
          credentialsServerUrl == null ||
          credentialsFile == null
      ? "NATS_AUTH_URL is not configured"
      : false;

  test(
    "user and password authenticate",
    () => _connect(
      serverUrl!,
      NatsAuthentication.userPassword("alice", "wonderland"),
    ),
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "token authenticates",
    () => _connect(tokenServerUrl!, NatsAuthentication.token("latest-token")),
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "official NKey seed authenticates",
    () => _connect(serverUrl!, NatsAuthentication.nkey(_seed)),
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "parsed credentials content signs the server nonce and authenticates",
    () async {
      final contents = await File(credentialsFile!).readAsString();
      await _connect(
        credentialsServerUrl!,
        NatsAuthentication.credentials(contents),
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "credentials file signs the server nonce and authenticates",
    () async {
      await _connect(
        credentialsServerUrl!,
        await loadNatsCredentialsFile(credentialsFile!),
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "provider receives the server nonce",
    () async {
      String? nonce;
      await _connect(
        serverUrl!,
        NatsAuthentication.provider((context) async {
          nonce = context.nonce;
          return NatsConnectCredentials(
            username: "alice",
            password: "wonderland",
          );
        }),
      );
      expect(nonce, isNotEmpty);
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "rejected credentials expose the authorization failure reason",
    () async {
      await expectLater(
        NatsConnection.connect(
          NatsOptions(
            servers: [NatsServer.parse(serverUrl!)],
            authentication: NatsAuthentication.userPassword("alice", "wrong"),
          ),
        ),
        throwsA(
          isA<NatsAuthenticationException>().having(
            (error) => error.reason,
            "reason",
            NatsAuthenticationFailure.authorizationViolation,
          ),
        ),
      );
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "permission failures preserve operation context without disconnecting",
    () async {
      final connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(serverUrl!)],
          authentication: NatsAuthentication.userPassword(
            "alice",
            "wonderland",
          ),
        ),
      );
      addTearDown(connection.close);
      final events = StreamIterator<NatsServerError>(
        connection.events
            .where((event) => event is NatsServerError)
            .cast<NatsServerError>(),
      );

      final subscriptionEvent = events.moveNext();
      final deniedSubscription = await connection.subscribe(
        "denied.orders",
        queue: "workers",
      );
      await connection.flush();
      expect(
        await subscriptionEvent.timeout(const Duration(seconds: 2)),
        isTrue,
      );
      expect(
        events.current.error,
        isA<NatsPermissionException>()
            .having(
              (error) => error.operation,
              "operation",
              NatsPermissionOperation.subscription,
            )
            .having((error) => error.subject, "subject", "denied.orders")
            .having((error) => error.queue, "queue", "workers"),
      );

      final publishEvent = events.moveNext();
      await connection.publish("denied.orders", const [1]);
      await connection.flush();
      expect(await publishEvent.timeout(const Duration(seconds: 2)), isTrue);
      expect(
        events.current.error,
        isA<NatsPermissionException>()
            .having(
              (error) => error.operation,
              "operation",
              NatsPermissionOperation.publish,
            )
            .having((error) => error.subject, "subject", "denied.orders"),
      );
      expect(connection.isConnected, isTrue);

      await deniedSubscription.unsubscribe();
      await events.cancel();
      await connection.close();
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "caller scoped request inbox works with restricted permissions",
    () async {
      final connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(serverUrl!)],
          authentication: NatsAuthentication.userPassword(
            "alice",
            "wonderland",
          ),
          requestInboxPrefix: "allowed.inbox",
        ),
      );
      addTearDown(connection.close);
      final responder = await connection.subscribe("allowed.request");
      final responderListener = responder.messages.listen((message) {
        connection.reply(message, message.payload).ignore();
      });
      addTearDown(responderListener.cancel);
      addTearDown(responder.unsubscribe);

      final response = await connection.request("allowed.request", const [
        1,
        2,
        3,
      ]);

      expect(response.subject, startsWith("allowed.inbox."));
      expect(response.payload, [1, 2, 3]);
      await responderListener.cancel();
      await responder.unsubscribe();
      await connection.close();
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );

  test(
    "maximum subscriptions is typed and leaves the connection usable",
    () async {
      final connection = await NatsConnection.connect(
        NatsOptions(
          servers: [NatsServer.parse(serverUrl!)],
          authentication: NatsAuthentication.nkey(_seed),
        ),
      );
      addTearDown(connection.close);
      final events = StreamIterator<NatsServerError>(
        connection.events
            .where((event) => event is NatsServerError)
            .cast<NatsServerError>(),
      );
      final first = await connection.subscribe("limit.first");
      final second = await connection.subscribe("limit.second");

      final errorEvent = events.moveNext();
      final rejected = await connection.subscribe("limit.third");
      await connection.flush();
      expect(await errorEvent.timeout(const Duration(seconds: 2)), isTrue);
      expect(events.current.error, isA<NatsMaximumSubscriptionsException>());
      expect(connection.isConnected, isTrue);
      await connection.publish("limit.first", const [1]);
      await connection.flush();

      await first.unsubscribe();
      await second.unsubscribe();
      await rejected.unsubscribe();
      await events.cancel();
      await connection.close();
    },
    skip: skipReason,
    timeout: const Timeout(Duration(seconds: 30)),
  );
}

Future<void> _connect(
  String serverUrl,
  NatsAuthentication authentication,
) async {
  final connection = await NatsConnection.connect(
    NatsOptions(
      servers: [NatsServer.parse(serverUrl)],
      authentication: authentication,
    ),
  );
  await connection.flush();
  await connection.close();
}
