package com.couchbase.connector.flink;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.transactions.TransactionGetResult;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public class CouchbaseSink implements Sink<JsonDocument> {

    private static final Logger LOG = LoggerFactory.getLogger(CouchbaseSink.class);
    private final String clusterUrl;
    private final String username;
    private final String password;
    private final String bucket;
    private final String scope;
    private final String collectionName;
    private transient long docnum;
    private Duration connectTimeout = Duration.of(5, ChronoUnit.SECONDS);
    private transient Cluster cluster;

    public CouchbaseSink(String clusterUrl, String username, String password, String bucket, String scope, String collection) {
        this.clusterUrl = clusterUrl;
        this.username = username;
        this.password = password;
        this.bucket = bucket;
        this.scope = scope;
        this.collectionName = collection;

    }

    public CouchbaseSink(String clusterUrl, String username, String password, String bucket) {
        this(clusterUrl, username, password, bucket, "_default", "_default");
    }

    public CouchbaseSink withConnectTimeout(Duration timeout) {
        this.connectTimeout = timeout;
        return this;
    }

    @Override
    public SinkWriter<JsonDocument> createWriter(InitContext initContext) throws IOException {
        cluster = Cluster.connect(clusterUrl, username, password);
        cluster.waitUntilReady(connectTimeout);
        return new CouchbaseSink.CollectionWriter();
    }

    private class CollectionWriter implements SinkWriter<JsonDocument> {
        private List<JsonDocument> buffer = new ArrayList<>();

        public CollectionWriter() {
        }

        @Override
        public void write(JsonDocument document, Context context) throws IOException, InterruptedException {
            buffer.add(document);
        }

        @Override
        public void flush(boolean b) throws IOException, InterruptedException {
            if (buffer == null || buffer.isEmpty()) {
                return;
            }

            Collection collection = cluster.bucket(bucket).scope(scope).collection(collectionName);
            cluster.transactions().run(tx -> {
                buffer.forEach(doc -> {
                    try {
                        tx.replace(tx.get(collection, doc.id()), JsonObject.fromJson(doc.content()));
                    } catch (DocumentNotFoundException dnfe) {
                        tx.insert(collection, doc.id(), doc.content());
                    }
                });
            });

            docnum += buffer.size();
            LOG.debug("Flushed {} documents to collection `{}`.`{}`.`{}` (total: {})", buffer.size(), bucket, scope, collectionName, docnum);
            buffer.clear();
        }

        @Override
        public void close() throws Exception {

        }
    }
}
