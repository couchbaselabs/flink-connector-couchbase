package com.couchbase.connector.flink;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.dcp.highlevel.*;
import com.couchbase.client.dcp.highlevel.internal.CollectionIdAndKey;
import com.couchbase.client.dcp.highlevel.internal.CollectionsManifest;
import com.couchbase.client.dcp.message.DcpControlRequest;
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

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CouchbaseSource implements Source<CouchbaseDocumentChange, CouchbaseSource.VBucketSplit, Map<Integer, StreamOffset>>, DataEventHandler, ControlEventHandler {
    private static final Logger LOG = LoggerFactory.getLogger(CouchbaseSource.class);
    private List<VBucketSplit> unassignedSplits = Collections.synchronizedList(new LinkedList<>());
    private VBucketSplit currentSplit;
    private Client client;
    private String seedNodes;
    private String username;
    private String password;
    private String bucketName;
    private String scopeName;
    private String collectionName;
    private Counter mutationCounter;

    private long mutations;
    private Counter deletionCounter;
    private long deletions;
    private Counter expirationCounter;
    private long expirations;
    private boolean closed;
    private Map<Integer, StreamOffset> vbucketOffsets = new ConcurrentHashMap<>();
    private SnapshotMarker currentMarker = null;
    private Boundedness boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
    private long maxEvents = -1L;
    private SplitEnumeratorContext<VBucketSplit> context;
    private ArrayList<Integer> waitingForSplits = new ArrayList<>();

    public CouchbaseSource(String seedNodes, String username, String password, String bucketName, String scopeName, String collectionName) {
        this.seedNodes = seedNodes;
        this.username = username;
        this.password = password;
        this.bucketName = bucketName;
        this.scopeName = scopeName;
        this.collectionName = collectionName;
        LOG.debug("Initialized couchbase datasource with seed nodes '{}'", seedNodes);
    }

    public CouchbaseSource(String seedNodes, String username, String password, String bucketName) {
        this(seedNodes, username, password, bucketName, null, null);
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    CouchbaseSource setBoundedness(Boundedness boundedness) {
        this.boundedness = boundedness;
        LOG.debug("Boundedness set to {}", boundedness);
        return this;
    }

    public CouchbaseSource setMaxEvents(long max) {
        LOG.debug("Max events limited to {}", max);
        setBoundedness(Boundedness.BOUNDED);
        this.maxEvents = max;
        return this;
    }

    @Override
    public SplitEnumerator<VBucketSplit, Map<Integer, StreamOffset>> createEnumerator(SplitEnumeratorContext<VBucketSplit> enumContext) throws Exception {
        LOG.debug("Creating new Enumerator");
        return new VBucketSplitEnumerator(enumContext);
    }

    @Override
    public SplitEnumerator<VBucketSplit, Map<Integer, StreamOffset>> restoreEnumerator(SplitEnumeratorContext<VBucketSplit> enumContext, Map<Integer, StreamOffset> checkpoint) throws Exception {
        LOG.debug("Restored offsets: {}", checkpoint);
        vbucketOffsets.putAll(checkpoint);
        return new VBucketSplitEnumerator(enumContext);
    }

    @Override
    public SimpleVersionedSerializer<VBucketSplit> getSplitSerializer() {
        return new VBucketSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Map<Integer, StreamOffset>> getEnumeratorCheckpointSerializer() {
        return new CheckpointSerializer();
    }

    @Override
    public SourceReader<CouchbaseDocumentChange, VBucketSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new VBucketSplitReader(readerContext);
    }

    private void ensureStarted(SplitEnumeratorContext<VBucketSplit> context) {
        this.context = context;
        if (this.client == null) {
            LOG.info("Starting couchbase source with seed nodes '{}'", seedNodes);
            ensureMetrics(context);
            this.client = Client.builder()
                    .userAgent("flink-connector", "0.2.0")
                    .seedNodes(this.seedNodes)
                    .credentials(this.username, this.password)
                    .bucket(this.bucketName)
                    .flowControl(128 * 1024 * 1024)
                    .build();
            this.client.dataEventHandler(this);
            this.client.controlEventHandler(this);
            try {
                this.client.connect().await();
                if (!vbucketOffsets.isEmpty()) {
                    this.client.resumeStreaming(vbucketOffsets).await();
                } else {
                    this.client.initializeState(StreamFrom.NOW, StreamTo.INFINITY).await();
                    this.client.startStreaming().await();
                }
                LOG.info("connected DCP stream from '{}' with offsets {}", seedNodes, vbucketOffsets);
            } catch (Exception e) {
                this.client = null;
                LOG.error("failed to connect DCP stream from '{}'", seedNodes, e);
                throw new RuntimeException(e);
            }
        }
    }

    private void incMutations(SplitEnumeratorContext<VBucketSplit> context) {
        ensureMetrics(context);
        if (mutationCounter != null) {
            mutationCounter.inc();
        }
        if (maxEvents > -1L) {
            mutations++;
        }
    }

    private void incDeletions(SplitEnumeratorContext<VBucketSplit> context) {
        ensureMetrics(context);
        if (deletionCounter != null) {
            deletionCounter.inc();
        }
        if (maxEvents > -1L) {
            deletions++;
        }
    }

    private void incExpirations(SplitEnumeratorContext<VBucketSplit> context) {
        ensureMetrics(context);
        if (expirationCounter != null) {
            expirationCounter.inc();
        }
        if (maxEvents > -1L) {
            expirations++;
        }
    }

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

    private void close() {
        if (this.client != null) {
            this.client.close();
            LOG.info("disconnected DCP stream from '{}'", seedNodes);
            this.client = null;
        }
        closed = true;
        if (!waitingForSplits.isEmpty()) {
            waitingForSplits.forEach(i -> context.signalNoMoreSplits(i));
        }
    }

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
            LOG.debug("received DCP snapshot for vbucket {}: {} - {}", MessageUtil.getVbucket(event), DcpSnapshotMarkerRequest.startSeqno(event), DcpSnapshotMarkerRequest.endSeqno(event));
            if (currentSplit != null) {
                if (!waitingForSplits.isEmpty()) {
                    int subtask = waitingForSplits.remove(0);
                    context.assignSplit(currentSplit, subtask);
                    LOG.debug("Directly assigned vbucket {} split {} - {} to waiting subtask {}", currentSplit.vbuuid(), currentSplit.startOffset(), currentSplit.endOffset(), subtask);
                } else {
                    unassignedSplits.add(currentSplit);
                }
            }
            currentSplit = new VBucketSplit(
                    MessageUtil.getVbucket(event),
                    DcpSnapshotMarkerRequest.startSeqno(event),
                    DcpSnapshotMarkerRequest.endSeqno(event)
            );
            currentMarker = new SnapshotMarker(DcpSnapshotMarkerRequest.startSeqno(event), DcpSnapshotMarkerRequest.endSeqno(event));

        } else {
            LOG.info("Unknown DCP message type: {} {}", event.getByte(0), event.getByte(1));
        }

        flowController.ack(event);
        event.release();

        if (maxEvents > -1 && maxEvents < mutations + expirations + deletions) {
            LOG.debug("Max events limit ({}) reached (mutations: {}, expirations: {}, deletions: {})", maxEvents, mutations, expirations, deletions);
            close();
        }
    }

    private void deliverEvent(CouchbaseDocumentChange.Type type, ByteBuf event) {
        short vbucket = MessageUtil.getVbucket(event);
        long seqNo = event.getLong(24);

        CollectionsManifest manifest = this.client.sessionState().get(vbucket).getCollectionsManifest();

        CollectionIdAndKey ciak = this.client.sessionState().get(vbucket).getKeyExtractor().getCollectionIdAndKey(event);
        CollectionsManifest.CollectionInfo collectionInfo = manifest.getCollection(ciak.collectionId());
        LOG.debug("Received DCP {} message for document '{}/{}' in vbucket {}", type, collectionInfo.name(), ciak.key(), vbucket);
        CouchbaseDocumentChange change = new CouchbaseDocumentChange(
                type,
                bucketName,
                collectionInfo.scope().name(),
                collectionInfo.name(),
                ciak.key(),
                vbucket,
                MessageUtil.getContentAsByteArray(event),
                seqNo
        );
        currentSplit.add(change);

        vbucketOffsets.put((int) vbucket, new StreamOffset(
                vbucket,
                change.seqno(),
                currentMarker,
                manifest.getId()
        ));
    }

    public interface StreamFailureEvent extends SourceEvent {
        StreamFailure failure();
    }

    public static class VBucketSplit implements SourceSplit {
        private final long startOffset;
        private final String id;

        private final List<CouchbaseDocumentChange> changes = new ArrayList<>();
        private final int vbuuid;
        private final long endOffset;

        public VBucketSplit(int vbuuid, long start, long end) {
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
    }

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
                LOG.debug("Assigned vbucket {} split {} - {} to subtask {}", split.vbuuid(), split.startOffset(), split.endOffset(), subtaskId);
                context.assignSplit(split, subtaskId);
            } else if (closed) {
                LOG.debug("Signalling to subtask {} that there's no more splits", subtaskId);
                context.signalNoMoreSplits(subtaskId);
            } else {
                LOG.debug("No splits available for subtask {}", subtaskId);
                if (waitingForSplits.contains(subtaskId)) {
                    waitingForSplits.add(subtaskId);
                }
            }
        }

        @Override
        public void addSplitsBack(List<VBucketSplit> splits, int subtaskId) {
            LOG.debug("Returning splits: \n\t{}", splits.stream()
                    .map(s -> String.format("vbucket {}: {} - {}", s.vbuuid(), s.startOffset(), s.endOffset()))
                    .collect(Collectors.joining("\n\t"))
            );
            unassignedSplits.addAll(0, splits);
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
                    split.getLong("endOffset")
            );

            JsonArray changes = split.getArray("changes");
            changes.forEach(c -> {
                JsonObject change = (JsonObject) c;
                result.add(new CouchbaseDocumentChange(
                        CouchbaseDocumentChange.Type.valueOf(change.getString("type")),
                        change.getString("bucket"),
                        change.getString("scope"),
                        change.getString("collection"),
                        change.getString("key"),
                        change.getLong("partition"),
                        Base64.getDecoder().decode(change.getString("content")),
                        change.getLong("seqno")
                ));
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
                        SnapshotMarker.NONE,
                        obj.getLong("cuuid")
                ));
            });
            return result;
        }
    }

    private class VBucketSplitReader implements SourceReader<CouchbaseDocumentChange, VBucketSplit> {
        private final SourceReaderContext context;
        private final List<VBucketSplit> splits = new ArrayList<>();
        private VBucketSplit current;
        private boolean noMoreSplits;
        private Iterator<CouchbaseDocumentChange> iterator;
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
            while (this.iterator == null || !this.iterator.hasNext()) {
                if (this.current != null) {
                    // done processing a split
                    vbucketOffsets.put(this.current.vbuuid(), new StreamOffset(
                            this.current.vbuuid(),
                            this.current.endOffset(),
                            new SnapshotMarker(this.current.startOffset(), this.current.endOffset()),
                            0
                    ));
                }
                if (splits.isEmpty()) {
                    if (noMoreSplits) {
                        return InputStatus.END_OF_INPUT;
                    }
                    context.sendSplitRequest();
                    return InputStatus.NOTHING_AVAILABLE;
                } else {
                    this.current = splits.remove(0);
                    this.iterator = this.current.iterator();
                }
            }

            output.collect(this.iterator.next());
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
            noMoreSplits = true;
        }

        @Override
        public void close() throws Exception {

        }
    }
}
