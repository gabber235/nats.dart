/// Returns the publication subject that reaches [storedSubject] through one
/// JetStream API domain.
String jetStreamPublicationSubject(String apiPrefix, String storedSubject) =>
    apiPrefix == r"$JS.API" ? storedSubject : "$apiPrefix.$storedSubject";
