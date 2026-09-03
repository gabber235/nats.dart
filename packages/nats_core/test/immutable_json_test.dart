import "package:nats_core/src/immutable_json.dart";
import "package:test/test.dart";

void main() {
  test("immutable JSON accepts its exact nesting ceiling", () {
    final value = _nestedLists(65);

    expect(freezeJsonValue(value), isA<List<Object?>>());
  });

  test("immutable JSON rejects nesting beyond its ceiling", () {
    final value = _nestedLists(66);

    expect(() => freezeJsonValue(value), throwsArgumentError);
  });
}

Object? _nestedLists(int count) {
  Object? value = true;
  for (var index = 0; index < count; index += 1) {
    value = <Object?>[value];
  }
  return value;
}
