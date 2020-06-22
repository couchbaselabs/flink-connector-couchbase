/*
 * Copyright 2020 Couchbase, Inc.
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

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static java.util.Objects.requireNonNull;

public class CouchbaseJsonSink extends RichSinkFunction<JsonDocument> {
  private Cluster cluster;
  private Collection collection;

  private String connectionString;
  private String username;
  private String password;
  private String bucketName;

  public CouchbaseJsonSink(String connectionString, String username, String password, String bucketName) {
    this.connectionString = requireNonNull(connectionString);
    this.username = requireNonNull(username);
    this.password = requireNonNull(password);
    this.bucketName = requireNonNull(bucketName);
  }

  @Override
  public void invoke(JsonDocument doc, Context context) throws Exception {
    collection.upsert(doc.id(), doc, upsertOptions()
        .transcoder(RawJsonTranscoder.INSTANCE));
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    cluster = Cluster.connect(connectionString, username, password);
    collection = cluster.bucket(bucketName).defaultCollection();
  }

  @Override
  public void close() throws Exception {
    cluster.disconnect();
  }
}
