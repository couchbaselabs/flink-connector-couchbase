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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.Set;

public class CouchbaseJsonSink extends RichSinkFunction<JsonDocument> {

  private CouchbaseCluster cluster;
  private Bucket bucket;

  private Set<String> seedNodes;
  private String username;
  private String password;
  private String bucketName;

  public CouchbaseJsonSink(Set<String> seedNodes, String username, String password, String bucketName) {
    this.seedNodes = seedNodes;
    this.username = username;
    this.password = password;
    this.bucketName = bucketName;
  }

  @Override
  public void invoke(JsonDocument doc, Context context) throws Exception {
    bucket.upsert(doc);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    cluster = CouchbaseCluster.create(new ArrayList<>(seedNodes));
    cluster.authenticate(username, password);
    bucket = cluster.openBucket(bucketName);
  }

  @Override
  public void close() throws Exception {
    cluster.disconnect();
  }
}
