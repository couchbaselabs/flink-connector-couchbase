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

import static java.util.Objects.requireNonNull;

public class CouchbaseDocumentChange {
  private final String bucket;
  private final String scope;
  private final String collection;
  private long seqno;

  public Type type() {
    return type;
  }

  public String bucket() {
    return bucket;
  }

  public String scope() {
    return scope;
  }

  public String collection() {
    return collection;
  }

  public enum Type {
    MUTATION,
    DELETION,
    EXPIRATION;
  }

  private final Type type;
  private final String key;
  private final byte[] content;
  private final int partition;

  public CouchbaseDocumentChange(Type type, String bucket, String scope, String collection, String key, int partition,
      byte[] content, long seqno) {
    this.type = requireNonNull(type);
    this.key = requireNonNull(key);
    this.content = content == null ? new byte[0] : content;
    this.partition = partition;
    this.seqno = seqno;
    this.bucket = bucket;
    this.scope = scope;
    this.collection = collection;
  }

  public boolean isMutation() {
    return type == Type.MUTATION;
  }

  public boolean isDeletion() {
    return type == Type.DELETION || type == Type.EXPIRATION;
  }

  public boolean isDeletionDueToExpiration() {
    return type == Type.EXPIRATION;
  }

  public String key() {
    return key;
  }

  public byte[] content() {
    return content;
  }

  public JsonDocument document() {
    return new JsonDocument(key, content);
  }

  public long partition() {
    return partition;
  }

  public long seqno() {
    return seqno;
  }
}
