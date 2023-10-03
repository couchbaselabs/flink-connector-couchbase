package com.couchbase.connector.flink;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CouchbaseSink implements Sink<JsonDocument> {
    private String clusterUrl;
    private String username;
    private String password;
    private String bucket;
    private String scope;
    private String collection;

    public CouchbaseSink(String clusterUrl, String username, String password, String bucket, String scope, String collection) {
        this.clusterUrl = clusterUrl;
        this.username = username;
        this.password = password;
        this.bucket = bucket;
        this.scope = scope;
        this.collection = collection;
    }

    public CouchbaseSink(String clusterUrl, String username, String password, String bucket) {
        this(clusterUrl, username, password, bucket, "_default", "_default");
    }
    @Override
    public SinkWriter<JsonDocument> createWriter(InitContext initContext) throws IOException {
        return new CouchbaseSink.CollectionWriter(clusterUrl, username, password, bucket, scope, collection);
    }

    private static class CollectionWriter implements SinkWriter<JsonDocument> {
        private String clusterUrl;
        private String username;
        private String password;
        private String bucket;
        private String scope;
        private String collectionName;
        private Cluster cluster;
        private List<JsonDocument> buffer = new ArrayList<>();
        public CollectionWriter(String clusterUrl, String username, String password, String bucket, String scope, String collection) {
            this.clusterUrl = clusterUrl;
            this.username = username;
            this.password = password;
            this.bucket = bucket;
            this.scope = scope;
            this.collectionName = collection;
        }

        @Override
        public void write(JsonDocument document, Context context) throws IOException, InterruptedException {
            buffer.add(document);
        }

        @Override
        public void flush(boolean b) throws IOException, InterruptedException {
            if (cluster == null) {
                cluster = Cluster.connect(clusterUrl, username, password);
            }

            Collection collection = cluster.bucket(bucket).scope(scope).collection(collectionName);
            buffer.forEach(doc -> collection.upsert(doc.id(), doc.content()));
        }

        @Override
        public void close() throws Exception {

        }
    }
}
