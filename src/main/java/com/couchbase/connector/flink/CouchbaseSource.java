package com.couchbase.connector.flink;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.highlevel.*;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

public class CouchbaseSource implements Source<CouchbaseDocumentChange, CouchbaseSource.VBucketSplit, Map<Integer, StreamOffset>> {
    private List<VBucketSplit> unassignedSplits = Collections.synchronizedList(new LinkedList<>());
    private VBucketSplit currentSplit;
    private Client client;
    private String seedNodes;
    private String username;
    private String password;
    private String bucketName;
    private String scopeName;
    private String collectionName;
    private Counter mutations;
    private Counter deletions;
    private Counter expirations;
    private boolean closed;
    private Map<Integer, StreamOffset> vbucketOffsets = new ConcurrentHashMap<>();

    public CouchbaseSource(String seedNodes, String username, String password, String bucketName, String scopeName, String collectionName) {
        this.seedNodes = seedNodes;
        this.username = username;
        this.password = password;
        this.bucketName = bucketName;
        this.scopeName = scopeName;
        this.collectionName = collectionName;
    }

    public CouchbaseSource(String seedNodes, String username, String password, String bucketName) {
        this(seedNodes, username, password, bucketName, null, null);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<VBucketSplit, Map<Integer, StreamOffset>> createEnumerator(SplitEnumeratorContext<VBucketSplit> enumContext) throws Exception {
        return new VBucketSplitEnumerator(enumContext);
    }

    @Override
    public SplitEnumerator<VBucketSplit, Map<Integer, StreamOffset>> restoreEnumerator(SplitEnumeratorContext<VBucketSplit> enumContext, Map<Integer, StreamOffset> checkpoint) throws Exception {
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
        if (this.client == null) {
            MetricGroup metrics = context.metricGroup();
            mutations = metrics.counter("dcpMutations");
            deletions = metrics.counter("dcpDeletions");
            expirations = metrics.counter("dcpExpirations");
            this.client = Client.builder()
                    .userAgent("flink-connector", "0.2.0")
                    .seedNodes(this.seedNodes)
                    .credentials(this.username, this.password)
                    .bucket(this.bucketName)
                    .flowControl(128 * 1024 * 1024)
                    .build();
            this.client.listener(new DatabaseChangeListener() {
                @Override
                public void onFailure(StreamFailure streamFailure) {
                    StreamFailureEvent event = () -> streamFailure;
                    IntStream.of(context.currentParallelism())
                            .forEach(i -> {
                                context.sendEventToSourceReader(i, event);
                                context.signalNoMoreSplits(i);
                            });
                }

                @Override
                public void onSnapshot(SnapshotDetails snapshotDetails) {
                    if (currentSplit != null) {
                        unassignedSplits.add(currentSplit);
                    }
                    currentSplit = new VBucketSplit(
                            snapshotDetails.getVbucket(),
                            snapshotDetails.getMarker().getStartSeqno(),
                            snapshotDetails.getMarker().getEndSeqno()
                    );

                    DatabaseChangeListener.super.onSnapshot(snapshotDetails);
                }

                @Override
                public void onDeletion(Deletion deletion) {
                    if (deletion.isDueToExpiration()) {
                        expirations.inc();
                    } else {
                        deletions.inc();
                    }
                    currentSplit.add(new CouchbaseDocumentChange(
                            deletion.isDueToExpiration() ? CouchbaseDocumentChange.Type.EXPIRATION : CouchbaseDocumentChange.Type.DELETION,
                            bucketName,
                            deletion.getCollection().scope().name(),
                            deletion.getCollection().name(),
                            deletion.getKey(),
                            deletion.getOffset().getVbuuid(),
                            deletion.getContent(),
                            deletion.getOffset().getSeqno()
                    ));
                    DatabaseChangeListener.super.onDeletion(deletion);
                }

                @Override
                public void onMutation(Mutation mutation) {
                    mutations.inc();
                    currentSplit.add(new CouchbaseDocumentChange(
                            CouchbaseDocumentChange.Type.MUTATION,
                            bucketName,
                            mutation.getCollection().scope().name(),
                            mutation.getCollection().name(),
                            mutation.getKey(),
                            mutation.getOffset().getVbuuid(),
                            mutation.getContent(),
                            mutation.getOffset().getSeqno()
                    ));
                    DatabaseChangeListener.super.onMutation(mutation);
                }
            }, FlowControlMode.AUTOMATIC);
            this.client.connect().await();
            this.client.resumeStreaming(vbucketOffsets).await();
        }
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
                context.assignSplit(unassignedSplits.remove(0), subtaskId);
            } else if (closed) {
                context.signalNoMoreSplits(subtaskId);
            }
        }

        @Override
        public void addSplitsBack(List<VBucketSplit> splits, int subtaskId) {
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
            if (client != null) {
                client.close();
            }
            closed = true;
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
