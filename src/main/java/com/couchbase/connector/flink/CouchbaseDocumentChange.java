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
  public enum Type {
    MUTATION,
    DELETION,
    EXPIRATION;
  }

  private final Type type;
  private final String key;
  private final byte[] content;
  private final int partition;

  public CouchbaseDocumentChange(Type type, String key, int partition, byte[] content) {
    this.type = requireNonNull(type);
    this.key = requireNonNull(key);
    this.content = content == null ? new byte[0] : content;
    this.partition = partition;
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

  public int partition() {
    return partition;
  }
}
