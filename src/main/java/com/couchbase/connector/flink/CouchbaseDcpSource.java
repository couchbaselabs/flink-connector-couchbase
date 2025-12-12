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

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.highlevel.*;
import com.couchbase.client.dcp.highlevel.internal.CollectionIdAndKey;
import com.couchbase.client.dcp.highlevel.internal.CollectionsManifest;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpExpirationMessage;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Unbounded DCP source that connects to cluster nodes and sends
 * CouchbaseDocumentChanges down a Flink stream
 * Replaces {@link CouchbaseSource} as it uses newer Flink APIs
 */
public class CouchbaseDcpSource
        implements Source<CouchbaseDocumentChange, CouchbaseDcpSource.VBucketSplit, Map<Integer, StreamOffset>>,
        DataEventHandler, ControlEventHandler {
    private static final Logger LOG = LoggerFactory.getLogger(CouchbaseDcpSource.class);
    /**
     * List of received DCP snapshots that haven't been assigned to any worker
     * threads
     */
    private List<VBucketSplit> unassignedSplits = Collections.synchronizedList(new LinkedList<>());
    /**
     * Currently populating DCP snapshot
     */
    private VBucketSplit currentSplit;
    /**
     * DCP client
     */
    private Client client;
    private String seedNodes;
    private String username;
    private String password;
    private String bucketName;
    private String scopeName;
    private String[] collectionNames;
    /**
     * Flink metrics for received mutation events
     */
    private Counter mutationCounter;

    /**
     * Internal mutation events counter
     */
    private long mutations;
    /**
     * Flink metrics for received deletion events
     */
    private Counter deletionCounter;
    /**
     * Internal deletion events counter
     */
    private long deletions;
    /**
     * Flink metrics for received expiration events
     */
    private Counter expirationCounter;
    /**
     * Internal expiration events counter
     */
    private long expirations;
    /**
     * State flag used to detect that the source has been closed
     */
    private boolean closed;
    /**
     * Continuously updated map of DCP offsets used as Flink stream snapshot for
     * restoring the stream in case of failure
     */
    private Map<Integer, StreamOffset> vbucketOffsets = new ConcurrentHashMap<>();
    /**
     * Latest received DCP marker
     */
    private SnapshotMarker currentMarker = null;
    /**
     * Source boundedness. Default value is CONTINOUS_UNBOUNDED, will be set to
     * BOUNDED if event limit is set
     */
    private Boundedness boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
    /**
     * Limits how many events should this source read from DCP. Default value is -1
     * (unlimited).
     */
    private long maxEvents = -1L;
    /**
     * Flink context used for communication with workers
     */
    private SplitEnumeratorContext<VBucketSplit> context;
    /**
     * A list of workers whose split requests couldn't be fulfilled because there
     * was not enough events.
     */
    private ArrayList<Integer> waitingForSplits = new ArrayList<>();
    private Duration connectTimeout = Duration.of(5, ChronoUnit.SECONDS);

    /**
     * Constructs CouchbaseDcpSource.
     * 
     * @param seedNodes       Initial list of Couchbase cluster nodes to connect to.
     *                        See {@link Client.Builder#seedNodes(String...)}
     * @param username        Couchbase cluster username. See
     *                        {@link Client.Builder#credentials(String, String)}
     * @param password        Couchbase cluster password. See
     *                        {@link Client.Builder#credentials(String, String)}
     * @param bucketName      Name of a Couchbase bucket to read events from. See
     *                        {@link Client.Builder#bucket(String)}
     * @param scopeName       (Optional) Name of a Couchbase scope to read events
     *                        from. If null, will read from all scopes in provided
     *                        bucket. See @{link
     *                        {@link Client.Builder#scopeName(String}}
     * @param collectionNames (Optional) Names of Couchbase collections to read
     *                        events from. See
     *                        {@link Client.Builder#collectionNames(String...)}
     */
    public CouchbaseDcpSource(@Nonnull String seedNodes, @Nonnull String username, @Nonnull String password,
            @Nonnull String bucketName, @Nullable String scopeName, @Nullable String... collectionNames) {
        this.seedNodes = seedNodes;
        this.username = username;
        this.password = password;
        this.bucketName = bucketName;
        this.scopeName = scopeName;
        this.collectionNames = collectionNames;

        if (collectionNames != null && collectionNames.length > 0 && scopeName == null) {
            throw new IllegalArgumentException("Scope name is required when specifying collection name(s)");
        }
        LOG.debug("Initialized couchbase datasource with seed nodes '{}'", seedNodes);
    }

    /**
     * Constructs CouchbaseDcpSource that reads events from all scopes and
     * collections in a bucket.
     * 
     * @param seedNodes  Initial list of Couchbase cluster nodes to connect to. See
     *                   {@link Client.Builder#seedNodes(String...)}
     * @param username   Couchbase cluster username. See
     *                   {@link Client.Builder#credentials(String, String)}
     * @param password   Couchbase cluster password. See
     *                   {@link Client.Builder#credentials(String, String)}
     * @param bucketName Name of a Couchbase bucket to read events from. See
     *                   {@link Client.Builder#bucket(String)}
     */
    public CouchbaseDcpSource(String seedNodes, String username, String password, String bucketName) {
        this(seedNodes, username, password, bucketName, null, null);
    }

    /**
     * @return {@link Boundedness} of this source
     */
    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    /**
     * Sets boundedness for this source. Used in tests.
     * 
     * @param boundedness
     * @return this
     */
    CouchbaseDcpSource setBoundedness(Boundedness boundedness) {
        this.boundedness = boundedness;
        LOG.debug("Boundedness set to {}", boundedness);
        return this;
    }

    public CouchbaseDcpSource withConnectTimeout(Duration duration) {
        this.connectTimeout = duration;
        return this;
    }

    /**
     * Limits the total number of mutation, deletion and expiration events to be
     * read by this source.
     * The source will report downstream about reaching this limit by sending a
     * "noMoreSplits" signal
     * 
     * @param max maximum number of events
     * @return this
     */
    public CouchbaseDcpSource setMaxEvents(long max) {
        LOG.debug("Max events limited to {}", max);
        setBoundedness(Boundedness.BOUNDED);
        this.maxEvents = max;
        return this;
    }

    /**
     * Creates an enumerator that is used by Flink to read chunks of the stream
     * called Splits. See {@link SourceSplit}
     * 
     * @param enumContext
     * @return a new split enumerator in which every split corresponds to a DCP
     *         snapshot
     */
    @Override
    public SplitEnumerator<VBucketSplit, Map<Integer, StreamOffset>> createEnumerator(
            SplitEnumeratorContext<VBucketSplit> enumContext) {
        LOG.debug("Creating new Enumerator");
        return new VBucketSplitEnumerator(enumContext);
    }

    /**
     * Re-creates a split enumerator and restores stream offsets using stream
     * snapshot data. See @{link
     * {@link Source#restoreEnumerator(SplitEnumeratorContext, Object)}}
     * 
     * @param enumContext enumerator context
     * @param checkpoint  snapshot data
     * @return re-created enumerator
     */
    @Override
    public SplitEnumerator<VBucketSplit, Map<Integer, StreamOffset>> restoreEnumerator(
            SplitEnumeratorContext<VBucketSplit> enumContext, Map<Integer, StreamOffset> checkpoint) {
        LOG.debug("Restored offsets: {}", checkpoint);
        vbucketOffsets.putAll(checkpoint);
        return new VBucketSplitEnumerator(enumContext);
    }

    /**
     * Creates a new split serializer. See {@link SimpleVersionedSerializer}
     * 
     * @return created split serializer
     */
    @Override
    public SimpleVersionedSerializer<VBucketSplit> getSplitSerializer() {
        return new VBucketSplitSerializer();
    }

    /**
     * Creates a new snapshot serializer. See {@link SimpleVersionedSerializer}
     * 
     * @return created snapshot serializer
     */
    @Override
    public SimpleVersionedSerializer<Map<Integer, StreamOffset>> getEnumeratorCheckpointSerializer() {
        return new CheckpointSerializer();
    }

    /**
     * Creates a new {@link SourceReader} which reads
     * {@link CouchbaseDocumentChange} objects from assigned to its worker split
     * 
     * @param readerContext worker context
     * @return created {@link SourceReader}
     */
    @Override
    public SourceReader<CouchbaseDocumentChange, VBucketSplit> createReader(SourceReaderContext readerContext) {
        return new VBucketSplitReader(readerContext);
    }

    /**
     * Creates a new DCP client and connects it to the cluster *if that haven't been
     * done before or if this source was previously closed*
     * 
     * @param context Flink context
     */
    private void ensureStarted(SplitEnumeratorContext<VBucketSplit> context) {
        this.context = context;
        if (this.client == null) {
            LOG.debug("Starting couchbase source with seed nodes '{}'", seedNodes);
            ensureMetrics(context);
            Client.Builder builder = Client.builder()
                    .userAgent("flink-connector", "0.2.0")
                    .seedNodes(this.seedNodes)
                    .credentials(this.username, this.password)
                    .bucket(this.bucketName)
                    .flowControl(128 * 1024 * 1024);
            if (scopeName != null) {
                builder.scopeName(scopeName);
            }
            if (collectionNames != null && collectionNames.length > 0) {
                builder.collectionNames(collectionNames);
            }
            this.client = builder.build();
            this.client.dataEventHandler(this);
            this.client.controlEventHandler(this);
            try {
                this.client.connect().block(connectTimeout);
                if (!vbucketOffsets.isEmpty()) {
                    this.client.resumeStreaming(vbucketOffsets).block(connectTimeout);
                } else {
                    this.client.initializeState(StreamFrom.NOW, StreamTo.INFINITY).block(connectTimeout);
                    this.client.startStreaming().block(connectTimeout);
                }
                LOG.debug("connected DCP stream from '{}' with offsets {}", seedNodes, vbucketOffsets);
            } catch (Exception e) {
                this.client = null;
                LOG.error("failed to connect DCP stream from '{}'", seedNodes, e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Helper method that updates internal mutation counter and reports mutation
     * events metric to Flink
     * 
     * @param context flink context
     */
    private void incMutations(SplitEnumeratorContext<VBucketSplit> context) {
        ensureMetrics(context);
        if (mutationCounter != null) {
            mutationCounter.inc();
        }
        if (maxEvents > -1L) {
            mutations++;
        }
    }

    /**
     * Helper method that updates internal deletion counter and reports deletion
     * events metric to Flink
     * 
     * @param context flink context
     */
    private void incDeletions(SplitEnumeratorContext<VBucketSplit> context) {
        ensureMetrics(context);
        if (deletionCounter != null) {
            deletionCounter.inc();
        }
        if (maxEvents > -1L) {
            deletions++;
        }
    }

    /**
     * Helper method that updates internal expirations counter and reports
     * expiration events metric to Flink
     * 
     * @param context flink context
     */
    private void incExpirations(SplitEnumeratorContext<VBucketSplit> context) {
        ensureMetrics(context);
        if (expirationCounter != null) {
            expirationCounter.inc();
        }
        if (maxEvents > -1L) {
            expirations++;
        }
    }

    /**
     * Helper method that fetches metric objects from Flink (if metric reporting is
     * enabled)
     * 
     * @param context flink context
     */
    private void ensureMetrics(SplitEnumeratorContext<VBucketSplit> context) {
        if (mutationCounter == null) {
            MetricGroup metrics = context.metricGroup();
            if (metrics != null) {
                mutationCounter = metrics.counter("dcpMutations");
                deletionCounter = metrics.counter("dcpDeletions");
                expirationCounter = metrics.counter("dcpExpirations");
            }
        }
    }

    /**
     * Closes this source by disconnecting DCP stream, destroying the DCP client and
     * reporting to workers that there will be no more splits
     */
    private void close() {
        if (currentSplit != null && !currentSplit.isEmpty()) {
            unassignedSplits.add(currentSplit);
        }
        currentSplit = null;
        closed = true;
        LOG.debug("Closing DCP source {}", this);
        if (this.client != null) {
            this.client.close();
            LOG.debug("disconnected DCP stream from '{}'", seedNodes);
            this.client = null;
        }
    }

    /**
     * DCP event handler that transforms raw DCP events into
     * {@link CouchbaseDocumentChange}s, puts them into {@Link VBucketSplit}s,
     * assigns received splits to waiting for splits workers and keeps track of DCP
     * snapshots.
     * 
     * @param flowController DCP flow controller used to acknowledge received
     *                       messages
     * @param event          DCP message data
     */
    @Override
    public void onEvent(ChannelFlowController flowController, ByteBuf event) {
        if (DcpMutationMessage.is(event)) {
            deliverEvent(CouchbaseDocumentChange.Type.MUTATION, event);
            incMutations(context);
        } else if (DcpDeletionMessage.is(event)) {
            deliverEvent(CouchbaseDocumentChange.Type.DELETION, event);
            incDeletions(context);
        } else if (DcpExpirationMessage.is(event)) {
            deliverEvent(CouchbaseDocumentChange.Type.EXPIRATION, event);
            incExpirations(context);
        } else if (DcpSnapshotMarkerRequest.is(event)) {
            LOG.debug("received DCP snapshot for vbucket {}: {} - {}", MessageUtil.getVbucket(event),
                    DcpSnapshotMarkerRequest.startSeqno(event), DcpSnapshotMarkerRequest.endSeqno(event));
            if (currentSplit != null && !currentSplit.isEmpty()) {
                if (!waitingForSplits.isEmpty()) {
                    int subtask = waitingForSplits.remove(0);
                    context.assignSplit(currentSplit, subtask);
                    LOG.debug("Directly assigned vbucket {} split {} - {} to waiting subtask {}", currentSplit.vbuuid(),
                            currentSplit.startOffset(), currentSplit.endOffset(), subtask);
                } else {
                    unassignedSplits.add(currentSplit);
                }
            }
            currentSplit = new VBucketSplit(
                    MessageUtil.getVbucket(event),
                    DcpSnapshotMarkerRequest.startSeqno(event),
                    DcpSnapshotMarkerRequest.endSeqno(event));
            currentMarker = new SnapshotMarker(DcpSnapshotMarkerRequest.startSeqno(event),
                    DcpSnapshotMarkerRequest.endSeqno(event));

        } else {
            LOG.debug("Unknown DCP message type: {} {}", event.getByte(0), event.getByte(1));
        }

        flowController.ack(event);
        event.release();

        if (maxEvents > -1 && maxEvents <= mutations + expirations + deletions) {
            LOG.debug("Max events limit ({}) reached (mutations: {}, expirations: {}, deletions: {})", maxEvents,
                    mutations, expirations, deletions);
            close();
        }
    }

    /**
     * Helper method that transforms DCP events into
     * {@link CouchbaseDocumentChange}s and puts them into the current
     * {@link VBucketSplit} for further processing by Flink workers
     * 
     * @param type  detected type of the DCP event
     * @param event DCP event data
     */
    private void deliverEvent(CouchbaseDocumentChange.Type type, ByteBuf event) {
        int vbucket = MessageUtil.getVbucket(event);
        long seqNo = event.getLong(24);

        CollectionsManifest manifest = this.client.sessionState().get(vbucket).getCollectionsManifest();

        CollectionIdAndKey ciak = this.client.sessionState().get(vbucket).getKeyExtractor()
                .getCollectionIdAndKey(event);
        CollectionsManifest.CollectionInfo collectionInfo = manifest.getCollection(ciak.collectionId());
        LOG.debug("Received DCP {} message for document '{}/{}' in vbucket {}", type, collectionInfo.name(), ciak.key(),
                vbucket);
        CouchbaseDocumentChange change = new CouchbaseDocumentChange(
                type,
                bucketName,
                collectionInfo.scope().name(),
                collectionInfo.name(),
                ciak.key(),
                vbucket,
                MessageUtil.getContentAsByteArray(event),
                seqNo);
        currentSplit.add(change);

        vbucketOffsets.put(vbucket, new StreamOffset(
                vbucket,
                change.seqno(),
                currentMarker,
                manifest.getId()));
    }

    /**
     * A class that holds splits (chunks) of the @{link CouchbaseDocumentChange}
     * stream
     */
    public static class VBucketSplit implements SourceSplit {
        private final long startOffset;
        private final String id;

        private final List<CouchbaseDocumentChange> changes = new ArrayList<>();
        private final int vbuuid;
        private final long endOffset;

        private VBucketSplit(int vbuuid, long start, long end) {
            this.vbuuid = vbuuid;
            this.startOffset = start;
            this.endOffset = end;
            this.id = String.format("%d:%d-%d", vbuuid, start, end);
        }

        public static VBucketSplit get(String id) {
            return null;
        }

        @Override
        public String splitId() {
            return id;
        }

        public boolean isEmpty() {
            return changes.isEmpty();
        }

        public Iterator<CouchbaseDocumentChange> iterator() {
            return changes.iterator();
        }

        public void add(CouchbaseDocumentChange change) {
            changes.add(change);
        }

        public int vbuuid() {
            return vbuuid;
        }

        public long endOffset() {
            return endOffset;
        }

        public long startOffset() {
            return startOffset;
        }

        public CouchbaseDocumentChange poll() {
            return changes.remove(0);
        }
    }

    /**
     * An enumerator that handles split requests from Flink workers. See
     * {@link SplitEnumerator}.
     */
    public class VBucketSplitEnumerator implements SplitEnumerator<VBucketSplit, Map<Integer, StreamOffset>> {

        private SplitEnumeratorContext<VBucketSplit> context;

        private VBucketSplitEnumerator(SplitEnumeratorContext<VBucketSplit> context) {
            this.context = context;
        }

        @Override
        public void start() {
            ensureStarted(context);
        }

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            if (!unassignedSplits.isEmpty()) {
                VBucketSplit split = unassignedSplits.remove(0);
                LOG.debug("Assigned vbucket {} split {} - {} to subtask {}", split.vbuuid(), split.startOffset(),
                        split.endOffset(), subtaskId);
                context.assignSplit(split, subtaskId);
            } else if (closed) {
                LOG.debug("Signalling to subtask {} that there's no more splits", subtaskId);
                context.signalNoMoreSplits(subtaskId);
            } else {
                if (!waitingForSplits.contains(subtaskId)) {
                    LOG.debug("No splits available for subtask {} (closed: {})", subtaskId, closed);
                    waitingForSplits.add(subtaskId);
                }
            }
        }

        /**
         * Adds splits that failed to be processed by Flink workers to the end of
         * unassigned splits queue
         * Adding splits back DOES NOT roll the stream back to the failed split as
         * further splits may have been already processed
         * 
         * @param splits    splits to be returned to the source
         * @param subtaskId id of the failed subtask
         */
        @Override
        public void addSplitsBack(List<VBucketSplit> splits, int subtaskId) {
            LOG.debug("Returning splits: \n\t{}", splits.stream()
                    .map(s -> String.format("vbucket {}: {} - {}", s.vbuuid(), s.startOffset(), s.endOffset()))
                    .collect(Collectors.joining("\n\t")));
            unassignedSplits.addAll(splits);
        }

        @Override
        public void addReader(int subtaskId) {

        }

        @Override
        public Map<Integer, StreamOffset> snapshotState(long checkpointId) throws Exception {
            return new HashMap<>(vbucketOffsets);
        }

        @Override
        public void close() throws IOException {
        }
    }

    public static class VBucketSplitSerializer implements SimpleVersionedSerializer<VBucketSplit> {

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(VBucketSplit obj) throws IOException {
            JsonObject result = JsonObject.create();
            result.put("vbuuid", obj.vbuuid());
            result.put("startOffset", obj.startOffset());
            result.put("endOffset", obj.endOffset());
            JsonArray changes = JsonArray.create();
            result.put("changes", changes);
            obj.iterator().forEachRemaining(documentChange -> {
                JsonObject change = JsonObject.create();
                changes.add(change);
                change.put("seqno", documentChange.seqno());
                change.put("partition", documentChange.partition());
                change.put("type", documentChange.type().name());
                change.put("key", documentChange.key());
                change.put("content", Base64.getEncoder().encodeToString(documentChange.content()));
                change.put("bucket", documentChange.bucket());
                change.put("scope", documentChange.scope());
                change.put("collection", documentChange.collection());
            });

            return result.toBytes();
        }

        @Override
        public VBucketSplit deserialize(int version, byte[] serialized) throws IOException {
            JsonObject split = JsonObject.fromJson(serialized);
            VBucketSplit result = new VBucketSplit(
                    split.getInt("vbuuid"),
                    split.getLong("startOffset"),
                    split.getLong("endOffset"));

            JsonArray changes = split.getArray("changes");
            changes.forEach(c -> {
                JsonObject change = (JsonObject) c;
                result.add(new CouchbaseDocumentChange(
                        CouchbaseDocumentChange.Type.valueOf(change.getString("type")),
                        change.getString("bucket"),
                        change.getString("scope"),
                        change.getString("collection"),
                        change.getString("key"),
                        change.getInt("partition"),
                        Base64.getDecoder().decode(change.getString("content")),
                        change.getLong("seqno")));
            });
            return result;
        }
    }

    private class CheckpointSerializer implements SimpleVersionedSerializer<Map<Integer, StreamOffset>> {
        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(Map<Integer, StreamOffset> obj) throws IOException {
            JsonObject result = JsonObject.create();
            obj.forEach((vb, offset) -> {
                JsonObject ofs = JsonObject.create();
                result.put(String.valueOf(vb), ofs);
                ofs.put("vbuuid", offset.getVbuuid());
                ofs.put("seqno", offset.getSeqno());
                ofs.put("cuuid", offset.getCollectionsManifestUid());
            });
            return result.toBytes();
        }

        @Override
        public Map<Integer, StreamOffset> deserialize(int version, byte[] serialized) throws IOException {
            Map<Integer, StreamOffset> result = new HashMap<>();
            JsonObject marker = JsonObject.fromJson(serialized);
            marker.toMap().forEach((s, o) -> {
                JsonObject obj = (JsonObject) o;
                result.put(Integer.valueOf(s), new StreamOffset(
                        obj.getLong("vbuuid"),
                        obj.getLong("seqno"),
                        // todo: fixme so that proper shapshot marker is deserialized here
                        SnapshotMarker.NONE,
                        obj.getLong("cuuid")));
            });
            return result;
        }
    }

    private static class VBucketSplitReader implements SourceReader<CouchbaseDocumentChange, VBucketSplit> {
        private final SourceReaderContext context;
        private final List<VBucketSplit> splits = new ArrayList<>();
        private VBucketSplit current;
        private boolean noMoreSplits;
        private CompletableFuture<Void> availability = new CompletableFuture<>();

        public VBucketSplitReader(SourceReaderContext readerContext) {
            this.context = readerContext;
        }

        @Override
        public void start() {
            if (splits.isEmpty()) {
                context.sendSplitRequest();
            }
        }

        @Override
        public InputStatus pollNext(ReaderOutput<CouchbaseDocumentChange> output) throws Exception {
            if (current == null || current.isEmpty()) {
                if (splits.isEmpty()) {
                    if (noMoreSplits) {
                        LOG.debug("Reader {}: signalling end of input", context.getIndexOfSubtask());
                        return InputStatus.END_OF_INPUT;
                    }
                    context.sendSplitRequest();
                    return InputStatus.NOTHING_AVAILABLE;
                } else {
                    this.current = splits.remove(0);
                }
            }

            output.collect(this.current.poll());
            return InputStatus.MORE_AVAILABLE;
        }

        @Override
        public List<VBucketSplit> snapshotState(long checkpointId) {
            List<VBucketSplit> snapshot = new ArrayList<>();
            if (this.current != null) {
                snapshot.add(current);
            }
            snapshot.addAll(splits);
            return snapshot;
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return this.availability;
        }

        @Override
        public void addSplits(List<VBucketSplit> splits) {
            this.splits.addAll(splits);
            if (!availability.isDone()) {
                availability.complete(null);
            }
        }

        @Override
        public void notifyNoMoreSplits() {
            LOG.debug("Reader {}: notified no more splits");
            noMoreSplits = true;
        }

        @Override
        public void close() throws Exception {

        }
    }
}
