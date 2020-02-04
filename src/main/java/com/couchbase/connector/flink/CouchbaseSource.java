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

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.highlevel.DatabaseChangeListener;
import com.couchbase.client.dcp.highlevel.Deletion;
import com.couchbase.client.dcp.highlevel.DocumentChange;
import com.couchbase.client.dcp.highlevel.FlowControlMode;
import com.couchbase.client.dcp.highlevel.Mutation;
import com.couchbase.client.dcp.highlevel.StreamFailure;
import com.couchbase.client.dcp.highlevel.StreamOffset;
import com.couchbase.connector.flink.dcp.DcpVbucketAndOffset;
import com.couchbase.connector.flink.dcp.PartitionHelper;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;

public class CouchbaseSource extends RichParallelSourceFunction<CouchbaseDocumentChange>
    implements CheckpointedFunction {

  private static final Logger log = LoggerFactory.getLogger(CouchbaseSource.class);
  private static final Exception poisonPill = new Exception();

  private volatile boolean running;
  private volatile String taskDescription;

  private transient ListState<DcpVbucketAndOffset> state;

  private final BlockingQueue<Throwable> fatalErrorQueue = new LinkedBlockingQueue<>();

  private transient Counter mutations;
  private transient Counter deletions;
  private transient Counter expirations;
  private Client client;

  private transient Object checkpointLock;

  private static final int MAX_VBUCKETS = 1024;

  // @GuardedBy(checkpointLock)
  private final StreamOffset[] vbucketToStreamOffset = new StreamOffset[MAX_VBUCKETS];

  @Override
  public void open(Configuration parameters) throws Exception {
    MetricGroup metrics = getRuntimeContext().getMetricGroup();
    mutations = metrics.counter("dcpMutations");
    deletions = metrics.counter("dcpDeletions");
    expirations = metrics.counter("dcpExpirations");

    client = Client.builder()
        .productName("flink-connector", "0.1")
        .hostnames("localhost")
        .credentials("Administrator", "password")
        .bucket("travel-sample")
        .build();
  }

  @Override
  public void run(SourceContext<CouchbaseDocumentChange> ctx) throws Exception {
    running = true;

    final RuntimeContext runtime = getRuntimeContext();
    this.taskDescription = runtime.getTaskNameWithSubtasks();
    this.checkpointLock = ctx.getCheckpointLock();

    client.listener(new DatabaseChangeListener() {
      @Override
      public void onFailure(StreamFailure streamFailure) {
        fatalErrorQueue.add(streamFailure.getCause());
      }

      @Override
      public void onMutation(Mutation mutation) {
        mutations.inc();
        onDocumentChange(mutation);
      }

      @Override
      public void onDeletion(Deletion deletion) {
        (deletion.isDueToExpiration() ? expirations : deletions).inc();
        onDocumentChange(deletion);
      }

      private void onDocumentChange(DocumentChange change) {
        CouchbaseDocumentChange.Type type = change.isMutation() ? CouchbaseDocumentChange.Type.MUTATION : CouchbaseDocumentChange.Type.DELETION;
        final CouchbaseDocumentChange item = new CouchbaseDocumentChange(type, change.getKey(), change.getVbucket(), change.getContent());

        synchronized (ctx.getCheckpointLock()) {
          ctx.collect(item);
          vbucketToStreamOffset[change.getVbucket()] = change.getOffset();
        }
      }

    }, FlowControlMode.AUTOMATIC);

    // todo need a better way to respond to failures here, and make sure cancelling the task
    // aborts the connection attempt (if it's taking a long time)
    client.connect().await();
    final int numPartitions = client.numPartitions();

    final List<Integer> myPartitions = PartitionHelper.getAssignedPartitions(numPartitions, getRuntimeContext());
    log.info("{} handling partitions: {}", taskDescription, myPartitions);

    final Map<Integer, StreamOffset> resumeOffsets = myPartitions.stream()
        .collect(toMap(p -> p, p -> Optional.ofNullable(vbucketToStreamOffset[p]).orElse(StreamOffset.ZERO)));

    if (resumeOffsets.isEmpty()) {
      // Shouldn't happen unless there are more subtasks than vbuckets
      log.warn("No work for {}", taskDescription);
      return;
    }

    client.resumeStreaming(resumeOffsets).await();

    try {
      while (running) {
        Throwable t = fatalErrorQueue.take();
        if (t == poisonPill) {
          break;
        }
        if (t instanceof Exception) {
          throw (Exception) t;
        }
        if (t instanceof Error) {
          throw (Error) t;
        }
        throw new RuntimeException(t);
      }
    } finally {
      disconnectDcpClient();
    }

  }

  @Override
  public void cancel() {
    running = false;
    fatalErrorQueue.offer(poisonPill);
  }

  private void disconnectDcpClient() {
    try {
      log.info("Disconnecting Couchbase DCP connection...");
      client.disconnect().await(5, SECONDS);
      log.info("DCP disconnection complete.");

    } catch (Exception e) {
      log.warn("DCP client disconnection failed or timed out.", e);
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    if (!running) {
      log.debug("snapshotState() called on closed source");
      return;
    }

    if (!Thread.holdsLock(checkpointLock)) {
      throw new AssertionError("Thread didn't hold checkpoint lock!");
    }

    state.clear();
    for (int i = 0; i < vbucketToStreamOffset.length; i++) {
      StreamOffset offset = vbucketToStreamOffset[i];
      if (offset != null) {
        state.add(new DcpVbucketAndOffset(i, offset));
        log.debug("snapshot state for vbucket {}: {}", i, offset);
      }
    }
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    state = context.getOperatorStateStore().getUnionListState(
        new ListStateDescriptor<>("couchbase-stream-offsets", TypeInformation.of(DcpVbucketAndOffset.class)));


    if (!context.isRestored()) {
      log.info("No restore state for CouchbaseSource.");
      return;
    }

    if (checkpointLock == null) {
      throw new AssertionError("oops, checkpoint lock not initialized yet");
    }

    synchronized (checkpointLock) {
      state.get().forEach(i -> vbucketToStreamOffset[i.getVbucket()] = i.getOffset());
    }

    Map<Integer, StreamOffset> display = new TreeMap<>(); // sorted just for pretty output
    for (int i = 0; i < vbucketToStreamOffset.length; i++) {
      if (vbucketToStreamOffset[i] != null) {
        display.put(i, vbucketToStreamOffset[i]);

      }
      log.debug("Restore state for CouchbaseSource: {}", display);
    }
  }
}
