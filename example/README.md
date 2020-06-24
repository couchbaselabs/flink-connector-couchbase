# Flink Connector for Couchbase Example

Here's a tiny example that uses the Couchbase connector
to copy documents from one bucket to another.

---

To package your job for submission to Flink, use: `gradlew shadowJar`. Afterwards, you'll find the
jar to use in the `build/libs` folder.

To run and test your application with an embedded instance of Flink use: `gradlew run`.
