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

package com.couchbase.connector.flink.internal.dcp;

import com.couchbase.client.dcp.highlevel.StreamOffset;

import java.io.Serializable;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class DcpVbucketAndOffset implements Serializable {
  private static final long serialVersionUID = 1;

  private int vbucket;
  private StreamOffset offset;

  public DcpVbucketAndOffset() {
  }

  public DcpVbucketAndOffset(int vbucket, StreamOffset offset) {
    this.vbucket = vbucket;
    this.offset = requireNonNull(offset);
  }

  public int getVbucket() {
    return vbucket;
  }

  public void setVbucket(int vbucket) {
    this.vbucket = vbucket;
  }

  public StreamOffset getOffset() {
    return offset;
  }

  public void setOffset(StreamOffset offset) {
    this.offset = offset;
  }

  @Override
  public String toString() {
    return vbucket + "/" + offset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DcpVbucketAndOffset that = (DcpVbucketAndOffset) o;
    return vbucket == that.vbucket &&
        offset.equals(that.offset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(vbucket, offset);
  }
}
