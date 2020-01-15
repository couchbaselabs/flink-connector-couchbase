/*
 * Copyright 2018 Couchbase, Inc.
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

package com.couchbase.connector.flink.dcp;

import java.io.Serializable;

public class DcpSnapshotMarker implements Serializable {
  private static final long serialVersionUID = 1;

  private final long startSeqno;
  private final long endSeqno;

  public DcpSnapshotMarker(long startSeqno, long endSeqno) {
    this.startSeqno = startSeqno;
    this.endSeqno = endSeqno;
  }

  public long getStartSeqno() {
    return startSeqno;
  }

  public long getEndSeqno() {
    return endSeqno;
  }

  @Override
  public String toString() {
    return "[" + startSeqno + "-" + endSeqno + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DcpSnapshotMarker snapshot = (DcpSnapshotMarker) o;

    if (startSeqno != snapshot.startSeqno) {
      return false;
    }
    return endSeqno == snapshot.endSeqno;
  }

  @Override
  public int hashCode() {
    int result = (int) (startSeqno ^ (startSeqno >>> 32));
    result = 31 * result + (int) (endSeqno ^ (endSeqno >>> 32));
    return result;
  }
}
