import "package:flutter/widgets.dart";
import "package:flutter_test/flutter_test.dart";
import "package:nats_core/nats_core.dart";
import "package:nats_jetstream/nats_jetstream.dart";

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
}
