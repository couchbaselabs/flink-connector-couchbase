# Flink Connector for Couchbase

This is a set of Source and Sink classes that can be used to read and write JSON documents to a Couchbase cluster.

The library is provided as-is without any guarantees and is considered to be in alpha-testing stage.

## Maven Coordinates:

```xml
<dependency>
    <groupId>com.couchbase.client</groupId>
    <artifactId>flink-connector-couchbase_2.12</artifactId>
    <version>${connectorVersion}</version>
</dependency>
```

`${connectorVersion}` = the version of this connector. See the git tags for valid values.


---

The `examples` directory contains basic examples of how to use the connector.

## Couchbase sources
### CouchbaseDcpSource
This source connects to DCP port on every node on the cluster and receives document changes as they are processed by the cluster.
Received changes are parsed from the binary format into `CouchbaseDocumentChange` objects and grouped into splits according to received DCP snapshots
(that is, each split corresponds to a single DCP snapshot).

The sink supports restoring DCP stream from previously stored vbucket offsets, however returned to the sink splits that
were not processed by Flink workers do not roll DCP stream offsets back.

### CouchbaseQuerySource
This source connects to QUERY service of a Couchbase cluster, executes provided to the sink query and streams fetched 
by the query documents in splits of configurable size. 

Query source supports providing positional and named arguments to the query. If both named and positional arguments provided,
only named arguments will be used. Neither arguments nor query cannot be changed after sink starts streaming documents.

Before starting, source enumerates all matching documents. Then it streams selected documents by wrapping the provided query 
into a pagination query `SELECT META().id, data.* FROM ($QUERY) as data LIMIT $SPLI_SIZE OFFSET $SPLIT_OFFSET`.

The sink does not detect concurrent modification of the result set and does not account for that while streaming the documents.

### CouchbaseSource
This source has been deprecated as it uses deprecated Flink API.

The source works similarly to the `CouchbaseDcpSource`.

## Sinks
### CouchbaseCollectionSink
This sink accepts `JsonDocument` objects and upserts them into a collection. 

Received documents are first buffered into an `ArrayList` and then flushed upon Flink's request in a transaction.

### CouchbaseJsonSink
This sink has been deprecated in favor of CouchbaseCollectionSink as it uses deprecated Flink APIs.

# Questions and support
Please use Github issues for questions and support. The project is maintained by `dmitrii.chechetkin@couchbase.com`.