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
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.RollbackMessage;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.IllegalReferenceCountException;
import com.couchbase.connector.flink.dcp.ArrayConcurrentIntMap;
import com.couchbase.connector.flink.dcp.ConcurrentIntMap;
import com.couchbase.connector.flink.dcp.DcpSnapshotMarker;
import com.couchbase.connector.flink.dcp.DcpStreamOffset;
import com.couchbase.connector.flink.dcp.DcpVbucketAndOffset;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_DELETION_OPCODE;
import static com.couchbase.client.dcp.message.MessageUtil.DCP_EXPIRATION_OPCODE;
import static com.couchbase.client.dcp.message.MessageUtil.DCP_MUTATION_OPCODE;
import static java.util.Objects.requireNonNull;

public class CouchbaseSource extends RichParallelSourceFunction<CouchbaseDocumentChange> implements ListCheckpointed<DcpVbucketAndOffset> {
  private static final Logger log = LoggerFactory.getLogger(CouchbaseSource.class);
  private volatile boolean isRunning;
  private volatile String taskDescription;

  private final BlockingQueue<Throwable> fatalErrorQueue = new LinkedBlockingQueue<>();

  private Counter mutations;
  private Counter deletions;
  private Counter expirations;
  private Counter unknowns;
  private Client client;

  private static final int MAX_VBUCKETS = 2048; // normally 1024, but some experimental builds use more.
  private final ConcurrentIntMap<DcpStreamOffset> vbucketToStreamOffset = new ArrayConcurrentIntMap<>(MAX_VBUCKETS);

  @Override
  public void open(Configuration parameters) throws Exception {
    MetricGroup metrics = getRuntimeContext().getMetricGroup();
    mutations = metrics.counter("dcpMutations");
    deletions = metrics.counter("dcpDeletions");
    expirations = metrics.counter("dcpExpirations");
    unknowns = metrics.counter("dcpUnknowns");

    client = Client.configure()
        .hostnames("localhost")
        .username("Administrator")
        .password("password")
        .bucket("travel-sample")
        .build();
  }

  @Override
  public void run(SourceContext<CouchbaseDocumentChange> ctx) throws Exception {
    isRunning = true;

    final RuntimeContext runtime = getRuntimeContext();
    final int taskIndex = runtime.getIndexOfThisSubtask();
    final int numTasks = runtime.getNumberOfParallelSubtasks();
    this.taskDescription = runtime.getTaskNameWithSubtasks();

    final ConcurrentIntMap<DcpSnapshotMarker> vbucketToSnapshotMarker = new ArrayConcurrentIntMap<>(MAX_VBUCKETS);

    // If we are in a rollback scenario, rollback the partition and restart the stream.
    client.controlEventHandler((flowController, event) -> {
      try {
        if (DcpSnapshotMarkerRequest.is(event)) {
          final int vbucket = DcpSnapshotMarkerRequest.partition(event);
          final long startSeqno = DcpSnapshotMarkerRequest.startSeqno(event);
          final long endSeqno = DcpSnapshotMarkerRequest.endSeqno(event);
          vbucketToSnapshotMarker.put(vbucket, new DcpSnapshotMarker(startSeqno, endSeqno));
          flowController.ack(event);
          return;
        }
        if (RollbackMessage.is(event)) {
          final short partition = RollbackMessage.vbucket(event);
          client.rollbackAndRestartStream(partition, RollbackMessage.seqno(event))
              .subscribe(
                  () -> log.info("Rollback for partition {} complete in {}", partition, taskDescription),
                  e -> fatalErrorQueue.add(new RuntimeException("Rollback for partition " + partition + " failed.")));
        }
      } finally {
        event.release();
      }
    });

    client.dataEventHandler((flowController, event) -> {
      try {
        int vbucket = MessageUtil.getVbucket(event);
        long vbuuid = client.sessionState().get(vbucket).getLastUuid();
        DcpSnapshotMarker snapshot = vbucketToSnapshotMarker.get(vbucket);
        if (snapshot == null) {
          throw new IllegalStateException("Missing DCP snapshot boundaries for vbucket " + vbucket);
        }
        final DcpStreamOffset offset = getOffset(event, vbuuid, vbucketToSnapshotMarker.get(vbucket));

        final CouchbaseDocumentChange.Type t = getType(event);
        if (t == null) {
          synchronized (ctx.getCheckpointLock()) {
            vbucketToStreamOffset.put(vbucket, offset);
          }
          return; // ignore unknowns
        }

        final CouchbaseDocumentChange change = new CouchbaseDocumentChange(t, MessageUtil.getKeyAsString(event), MessageUtil.getContentAsByteArray(event));

        synchronized (ctx.getCheckpointLock()) {
          vbucketToStreamOffset.put(vbucket, offset);
          // todo emit in a separate thread in case it blocks due to backpressure?
          ctx.collect(change);
        }

      } catch (Throwable t) {
        fatalErrorQueue.add(t);

      } finally {
        ackAndRelease(flowController, event);
      }
    });

    // todo need a better way to respond to failures here, and make sure cancelling the task
    // aborts the connection attempt (if it's taking a long time)
    client.connect().await();
    client.initializeState(StreamFrom.BEGINNING, StreamTo.INFINITY).await();
    int numPartitions = client.numPartitions();

    List<Integer> partitions = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      partitions.add(i);
    }
    List<Integer> myPartitions = chunks(partitions, numTasks).get(taskIndex);
    client.startStreaming(toBoxedShortArray(myPartitions)).await();


    while (isRunning) {
      Throwable t = fatalErrorQueue.take();
      if (t instanceof Exception) {
        throw (Exception) t;
      }
      if (t instanceof Error) {
        throw (Error) t;
      }
      throw new RuntimeException(t);
    }
  }

  private DcpStreamOffset getOffset(ByteBuf event, long vbuuid, DcpSnapshotMarker snapshot) {
    final long seqno = DcpMutationMessage.bySeqno(event); // same for all data event type
    return new DcpStreamOffset(vbuuid, seqno, snapshot);

  }

  /**
   * @param buffer a stream data event
   * @return (nullable) type of the data event, or null if unrecognized
   * @throws IllegalArgumentException if the buffer is not a stream event
   */
  private CouchbaseDocumentChange.Type getType(ByteBuf buffer) {
    if (buffer.getByte(0) != MessageUtil.MAGIC_REQ) {
      throw new IllegalArgumentException("not a data stream event");
    }
    final byte opcode = buffer.getByte(1);
    switch (opcode) {
      case DCP_MUTATION_OPCODE:
        mutations.inc();
        return CouchbaseDocumentChange.Type.MUTATION;

      case DCP_DELETION_OPCODE:
        deletions.inc();
        return CouchbaseDocumentChange.Type.DELETION;

      case DCP_EXPIRATION_OPCODE:
        expirations.inc();
        return CouchbaseDocumentChange.Type.EXPIRATION;

      default:
        unknowns.inc();
        log.debug("unrecognized data event opcode: {}", opcode);
        return null;
    }
  }

  private static void ackAndRelease(ChannelFlowController flowController, ByteBuf buffer) throws IllegalReferenceCountException {
    try {
      flowController.ack(buffer);

    } catch (IllegalReferenceCountException e) {
      throw e;

    } catch (Exception e) {
      log.warn("Flow control ack failed (channel already closed?)", e);
    }

    buffer.release();
  }

  @Override
  public void cancel() {
    isRunning = false;
    client.disconnect().await();
  }

  /**
   * Splits the given list into the requested number of chunks.
   * The smallest and largest chunks are guaranteed to differ in size by no more than 1.
   * If the requested number of chunks is greater than the number of items,
   * some chunks will be empty.
   */
  private static <T> List<List<T>> chunks(List<T> items, int chunks) {
    if (chunks <= 0) {
      throw new IllegalArgumentException("chunks must be > 0");
    }
    requireNonNull(items);

    final int maxChunkSize = ((items.size() - 1) / chunks) + 1; // size / chunks, rounded up
    final int numFullChunks = chunks - (maxChunkSize * chunks - items.size());

    final List<List<T>> result = new ArrayList<>(chunks);

    int startIndex = 0;
    for (int i = 0; i < chunks; i++) {
      int endIndex = startIndex + maxChunkSize;
      if (i >= numFullChunks) {
        endIndex--;
      }
      result.add(items.subList(startIndex, endIndex));
      startIndex = endIndex;
    }
    return result;
  }

  private static Short[] toBoxedShortArray(Iterable<? extends Number> numbers) {
    final List<Short> result = new ArrayList<>();
    for (Number i : numbers) {
      result.add(i.shortValue());
    }
    return result.toArray(new Short[0]);
  }


  @Override
  public List<DcpVbucketAndOffset> snapshotState(long checkpointId, long timestamp) throws Exception {
    final List<DcpVbucketAndOffset> result = new ArrayList<>();

    for (int i = 0, max = client.numPartitions(); i < max; i++) {
      final DcpStreamOffset offset = vbucketToStreamOffset.get(i);
      if (offset != null) {
        result.add(new DcpVbucketAndOffset(i, offset));
      }
    }

    return result;
  }

  @Override
  public void restoreState(List<DcpVbucketAndOffset> state) throws Exception {
    throw new UnsupportedOperationException();
  }
}
