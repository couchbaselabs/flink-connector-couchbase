# Flink Connector for Couchbase

Experimental. Unsupported. Great plans for the future.

## Maven Coordinates:

```xml
<dependency>
    <groupId>com.couchbase.client</groupId>
    <artifactId>flink-connector-couchbase_${scalaBinaryVersion}</artifactId>
    <version>${connectorVersion}</version>
</dependency>
```

`${scalaBinaryVersion}` = `2.11` or `2.12`, depending on the flavor of Flink you're using.

`${connectorVersion}` = the version of this connector. See the git tags for valid values.


---

The `example` directory contains an example of how to use the connector.
