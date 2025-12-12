/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.connector.flink;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A Couchbase sink that writes received {@link JsonDocument}s into a collection
 */
public class CouchbaseCollectionSink implements Sink<JsonDocument> {

    private static final Logger LOG = LoggerFactory.getLogger(CouchbaseCollectionSink.class);
    private final String clusterUrl;
    private final String username;
    private final String password;
    private final String bucket;
    private final String scope;
    private final String collectionName;
    private transient long docnum;
    private Duration connectTimeout = Duration.of(5, ChronoUnit.SECONDS);
    private transient Cluster cluster;

    /**
     * Configures the sink with specified collection
     * 
     * @param clusterUrl cluster connection string
     * @param username   cluster username
     * @param password   cluster password
     * @param bucket     bucket name for the target collection
     * @param scope      scope name for the target collection
     * @param collection target collection name
     */
    public CouchbaseCollectionSink(String clusterUrl, String username, String password, String bucket, String scope,
            String collection) {
        this.clusterUrl = clusterUrl;
        this.username = username;
        this.password = password;
        this.bucket = bucket;
        this.scope = scope;
        this.collectionName = collection;

    }

    /**
     * Configures the sink to store documents in the default collection for
     * specified bucket
     * 
     * @param clusterUrl cluster connection string
     * @param username   cluster username
     * @param password   cluster password
     * @param bucket     bucket name for the default collection
     */
    public CouchbaseCollectionSink(String clusterUrl, String username, String password, String bucket) {
        this(clusterUrl, username, password, bucket, "_default", "_default");
    }

    public CouchbaseCollectionSink withConnectTimeout(Duration timeout) {
        this.connectTimeout = timeout;
        return this;
    }

    @Override
    public SinkWriter<JsonDocument> createWriter(WriterInitContext initContext) throws IOException {
        cluster = Cluster.connect(clusterUrl, username, password);
        cluster.waitUntilReady(connectTimeout);
        return new CouchbaseCollectionSink.CollectionWriter();
    }

    private class CollectionWriter implements SinkWriter<JsonDocument> {
        private JsonDocument[] buffer = new JsonDocument[0];
        private int bufferSize = 0;

        public CollectionWriter() {
        }

        @Override
        public void write(JsonDocument document, Context context) throws IOException, InterruptedException {
            if (buffer.length < bufferSize + 1) {
                int newBufferLength = buffer.length == 0 ? 100 : buffer.length * 2;
                JsonDocument[] newBuffer = new JsonDocument[newBufferLength];
                System.arraycopy(buffer, 0, newBuffer, 0, bufferSize);
                buffer = newBuffer;
            }
            buffer[bufferSize++] = document;
        }

        @Override
        public void flush(boolean b) throws IOException, InterruptedException {
            if (bufferSize == 0) {
                return;
            }

            Collection collection = cluster.bucket(bucket).scope(scope).collection(collectionName);
            cluster.transactions().run(tx -> {
                for (int i = 0; i < bufferSize; i++) {
                    JsonDocument doc = buffer[i];
                    try {
                        tx.replace(tx.get(collection, doc.id()), JsonObject.fromJson(doc.content()));
                    } catch (DocumentNotFoundException dnfe) {
                        tx.insert(collection, doc.id(), doc.content());
                    }
                }
                docnum += bufferSize;
                LOG.debug("Flushed {} documents to collection `{}`.`{}`.`{}` (total: {})", bufferSize, bucket, scope,
                        collectionName, docnum);
                bufferSize = 0;
            });
        }

        @Override
        public void close() throws Exception {
            cluster.close();
        }
    }
}
