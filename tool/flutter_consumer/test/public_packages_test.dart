import "dart:async";

import "package:flutter/widgets.dart";
import "package:flutter_test/flutter_test.dart";
import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";

const _seed = "SUAKYRHVIOREXV7EUZTBHUHL7NUMHPMAS7QMDU3GTIUWEI5LDNOXD43IZY";

void main() {
  testWidgets("Flutter consumer compiles both public packages", (tester) async {
    final server = NatsServer.parse("nats://127.0.0.1:4222");
    final stream = JetStreamStreamConfig(
      name: "EVENTS",
      subjects: const ["events.>"],
    );

    await tester.pumpWidget(
      Directionality(
        textDirection: TextDirection.ltr,
        child: Text("${server.uri.host}:${stream.name}"),
      ),
    );

    expect(find.text("127.0.0.1:EVENTS"), findsOneWidget);
  });

  test("NKey signing works with the Flutter web compiler", () async {
    final credentials = await NatsAuthentication.nkey(_seed).authenticate(
      NatsAuthContext(
        serverInfo: const {"nonce": "challenge"},
        nonce: "challenge",
        deadline: DateTime.timestamp().add(const Duration(seconds: 5)),
        cancelled: Completer<void>().future,
      ),
    );

    expect(credentials.signature, isNotEmpty);
  });
}
