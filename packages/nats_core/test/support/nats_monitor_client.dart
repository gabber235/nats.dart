import "dart:async";
import "dart:convert";
import "dart:io";

final class NatsMonitorClient {
  const NatsMonitorClient(this.baseUri);

  final Uri baseUri;

  Future<void> waitUntilClientGone(
    String clientName, {
    Duration timeout = const Duration(seconds: 5),
  }) async {
    final deadline = DateTime.timestamp().add(timeout);
    Object? lastState;
    while (DateTime.timestamp().isBefore(deadline)) {
      final connections = await _openConnections();
      lastState = connections;
      if (!connections.any((connection) => connection["name"] == clientName)) {
        return;
      }
      await Future<void>.delayed(const Duration(milliseconds: 20));
    }
    throw StateError("Client remained in connz after close: $lastState");
  }

  Future<List<Map<String, Object?>>> _openConnections() async {
    final client = HttpClient();
    try {
      final request = await client.getUrl(
        baseUri.replace(path: "/connz", queryParameters: {"state": "open"}),
      );
      final response = await request.close();
      final decoded = jsonDecode(await utf8.decodeStream(response));
      if (decoded case {"connections": final List<Object?> connections}) {
        return connections.whereType<Map<String, Object?>>().toList(
          growable: false,
        );
      }
      return const [];
    } finally {
      client.close(force: true);
    }
  }
}
