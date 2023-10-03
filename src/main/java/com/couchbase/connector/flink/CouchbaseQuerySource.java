package com.couchbase.connector.flink;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CouchbaseQuerySource implements Source<JsonDocument, CouchbaseQuerySource.QueryResultSplit, Long> {
    private static final Logger LOG = LoggerFactory.getLogger(CouchbaseQuerySource.class);

    private final String clusterUrl;
    private final String username;
    private final String password;
    private transient Cluster cluster;
    private String queryTemplate;
    private JsonObject namedQueryArguments = JsonObject.create();
    private JsonArray positionQueryArguments = JsonArray.create();

    private long pageSize = 100;

    private boolean started = false;

    public CouchbaseQuerySource(String clusterUrl, String username, String password) {
        this.clusterUrl = clusterUrl;
        this.username = username;
        this.password = password;
    }

    public CouchbaseQuerySource pageSize(long pageSize) {
        assertNotStarted();
        this.pageSize = pageSize;
        return this;
    }

    public CouchbaseQuerySource query(String query) {
        assertNotStarted();
        this.queryTemplate = query;
        return this;
    }

    public CouchbaseQuerySource withArguments(Object... arguments) {
        assertNotStarted();
        if (!namedQueryArguments.isEmpty()) {
            throw new IllegalStateException("Unable to use positional arguments together with named arguments");
        }
        for (int i = 0; i < arguments.length; i++) {
            positionQueryArguments.add(arguments[i]);
        }
        return this;
    }

    public CouchbaseQuerySource withArgument(String name, Object argument) {
        assertNotStarted();
        if (!positionQueryArguments.isEmpty()) {
            throw new IllegalStateException("Unable to use named arguments together with positional arguments");
        }
        namedQueryArguments.put(name, argument);
        return this;
    }

    private void assertNotStarted() {
        if (started) {
            throw new IllegalStateException("Unable to change source parameters after streaming has started");
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<QueryResultSplit, Long> createEnumerator(SplitEnumeratorContext<QueryResultSplit> splitEnumeratorContext) throws Exception {
        return new CouchbaseQuerySource.SplitEnumeratorImpl(splitEnumeratorContext);
    }

    @Override
    public SplitEnumerator<QueryResultSplit, Long> restoreEnumerator(SplitEnumeratorContext<QueryResultSplit> splitEnumeratorContext, Long page) throws Exception {
        return new CouchbaseQuerySource.SplitEnumeratorImpl(splitEnumeratorContext, page);
    }

    @Override
    public SimpleVersionedSerializer<QueryResultSplit> getSplitSerializer() {
        return new CouchbaseQuerySource.SplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Long> getEnumeratorCheckpointSerializer() {
        return new CouchbaseQuerySource.EnumeratorCheckpointSerializer();
    }

    @Override
    public SourceReader<JsonDocument, QueryResultSplit> createReader(SourceReaderContext sourceReaderContext) throws Exception {
        return new SourceReaderImpl(sourceReaderContext);
    }

    public class QueryResultSplit implements SourceSplit {
        private long pageNumber;
        private List<JsonDocument> documents;

        public QueryResultSplit(long pageNumber) {
            this(pageNumber, null);
        }

        QueryResultSplit(long pageNumber, List<JsonDocument> documents) {
            this.pageNumber = pageNumber;
            this.documents = documents;
        }

        public long pageNumber() {
            return pageNumber;
        }

        @Override
        public String splitId() {
            return String.valueOf(pageNumber);
        }

        public boolean isEmpty() {
            if (documents == null) {
                fetchDocuments();
            }
            return documents.isEmpty();
        }

        private void fetchDocuments() {
            QueryOptions options = QueryOptions.queryOptions();
            if (!namedQueryArguments.isEmpty()) {
                options.parameters(namedQueryArguments);
            } else if (!positionQueryArguments.isEmpty()) {
                options.parameters(positionQueryArguments);
            }

            if (cluster == null) {
                cluster = Cluster.connect(clusterUrl, username, password);
            }

            String query = String.format("SELECT META().id as id, data as document FROM (%s) as data LIMIT %d OFFSET %d", queryTemplate, pageSize, pageNumber * pageSize);
            LOG.info("Split #{} query: {}", pageNumber, query);
            QueryResult result = cluster.query(
                    query,
                    options
            );

            if (result == null) {
                throw new IllegalStateException("Couchbase SDK returned NULL query result");
            }

            documents = result.rowsAsObject().stream()
                    .map(row -> new JsonDocument(row.getString("id"), row.getObject("document").toBytes()))
                    .collect(Collectors.toList());
        }

        public JsonDocument poll() {
            if (documents == null) {
                fetchDocuments();
            }
            return documents.remove(0);
        }

        public List<JsonDocument> documents() {
            return documents;
        }
    }

    public class SourceReaderImpl implements SourceReader<JsonDocument, QueryResultSplit> {
        private final SourceReaderContext context;
        private final CompletableFuture<Void> available = new CompletableFuture<>();

        private final List<QueryResultSplit> splits = new ArrayList<>();

        private boolean noMoreSplits = false;

        private QueryResultSplit currentPage;

        protected SourceReaderImpl(SourceReaderContext sourceReaderContext) {
            this.context = sourceReaderContext;
        }

        @Override
        public void start() {
            started = true;
        }

        @Override
        public InputStatus pollNext(ReaderOutput<JsonDocument> readerOutput) {
            if (currentPage != null && !currentPage.isEmpty()) {
                readerOutput.collect(currentPage.poll());
                return InputStatus.MORE_AVAILABLE;
            } else if (!splits.isEmpty()) {
                currentPage = splits.remove(0);
                if (!currentPage.isEmpty()) {
                    readerOutput.collect(currentPage.poll());
                } else {
                    throw new IllegalStateException(String.format("New split #%d is empty", currentPage.pageNumber()));
                }
                return InputStatus.MORE_AVAILABLE;
            } else if (!noMoreSplits) {
                LOG.info("Reader {}: requesting more splits", context.getIndexOfSubtask());
                context.sendSplitRequest();
                return InputStatus.NOTHING_AVAILABLE;
            } else {
                LOG.info("Reader {}: signalling end of input", context.getIndexOfSubtask());
                return InputStatus.END_OF_INPUT;
            }
        }

        @Override
        public List<QueryResultSplit> snapshotState(long l) {
            return splits;
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return available;
        }

        @Override
        public void addSplits(List<QueryResultSplit> list) {
            splits.addAll(list);
            available.complete(null);
        }

        @Override
        public void notifyNoMoreSplits() {
            noMoreSplits = true;
        }

        @Override
        public void close() throws Exception {
            splits.clear();
            currentPage = null;
            if (cluster != null) {
                cluster.disconnect();
                cluster = null;
            }
        }
    }

    public class SplitEnumeratorImpl implements SplitEnumerator<QueryResultSplit, Long> {
        private final SplitEnumeratorContext<QueryResultSplit> context;

        private final List<QueryResultSplit> splits = new ArrayList<>();

        private long nextPage = 0;

        private long totalDocuments = 0;
        private int finalizedTasks = 0;

        public SplitEnumeratorImpl(SplitEnumeratorContext<QueryResultSplit> splitEnumeratorContext) {
            context = splitEnumeratorContext;
        }

        public SplitEnumeratorImpl(SplitEnumeratorContext<QueryResultSplit> splitEnumeratorContext, Long page) {
            this(splitEnumeratorContext);
            nextPage = page;
        }

        @Override
        public void start() {
            started = true;
            cluster = Cluster.connect(clusterUrl, username, password);
            QueryOptions qo = QueryOptions.queryOptions();
            if (!namedQueryArguments.isEmpty()) {
                qo.parameters(namedQueryArguments);
            } else if (!positionQueryArguments.isEmpty()) {
                qo.parameters(positionQueryArguments);
            }
            String countQuery = String.format("SELECT count(*) totalDocuments FROM (%s) as data", queryTemplate);
            LOG.info("Count query: {}", countQuery);
            QueryResult countResult = cluster.query(
                    countQuery,
                    qo
            );

            if (countResult == null) {
                throw new IllegalStateException("failed to count documents: couchbase SDK returned null query result");
            }

            totalDocuments = countResult.rowsAsObject().get(0).getLong("totalDocuments");
            LOG.info("Total document count: {}", totalDocuments);
        }

        @Override
        public void handleSplitRequest(int taskId, @Nullable String requesterHostName) {
            if (splits.isEmpty()) {
                if (nextPage * pageSize >= totalDocuments) {
                    LOG.info("Signalling task {}/{} that no documents left. Next page: {} ({} documents), total documents: {}", taskId, context.currentParallelism(), nextPage, nextPage * pageSize, totalDocuments);
                    context.signalNoMoreSplits(taskId);
                    finalizedTasks++;
                    return;
                }
                context.assignSplit(new QueryResultSplit(nextPage), taskId);
                LOG.info("Assigned page {} to task {}", nextPage, taskId);
                nextPage++;
            } else {
                context.assignSplit(splits.remove(0), taskId);
            }
        }

        @Override
        public void addSplitsBack(List<QueryResultSplit> list, int subTask) {
            splits.addAll(0, list);
            LOG.info("returned splits from task {}: {}", subTask, list.stream().map(QueryResultSplit::splitId).collect(Collectors.joining(",")));
        }

        @Override
        public void addReader(int i) {

        }

        @Override
        public Long snapshotState(long l) throws Exception {
            return nextPage;
        }

        @Override
        public void close() throws IOException {
            LOG.info("Closing split enumerator");
            if (cluster != null) {
                cluster.disconnect();
                cluster = null;
            }
            IntStream.range(0, context.currentParallelism()).forEach(context::signalNoMoreSplits);
        }
    }

    public class EnumeratorCheckpointSerializer implements SimpleVersionedSerializer<Long> {
        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(Long aLong) throws IOException {
            return aLong.toString().getBytes();
        }

        @Override
        public Long deserialize(int i, byte[] bytes) throws IOException {
            return Long.valueOf(new String(bytes));
        }
    }

    public class SplitSerializer implements SimpleVersionedSerializer<QueryResultSplit> {
        private static final String KEY_PAGE_NUMBER = "pageNumber";
        private static final String KEY_DOCUMENTS = "documents";
        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(QueryResultSplit queryResultSplit) throws IOException {
            JsonObject result = JsonObject.create();
            JsonArray documents = JsonArray.create();
            List<JsonDocument> documentData = queryResultSplit.documents();
            if (documentData != null) {
                documentData.forEach(d -> {
                    JsonObject document = JsonObject.create();
                    document.put("id", d.id());
                    document.put("document", JsonObject.fromJson(d.content()));
                });
                result.put(KEY_DOCUMENTS, documents);
            }

            result.put(KEY_PAGE_NUMBER, queryResultSplit.pageNumber());
            return result.toBytes();
        }

        @Override
        public QueryResultSplit deserialize(int version, byte[] bytes) throws IOException {
            if (version == 0) {
                JsonObject data = JsonObject.fromJson(bytes);
                long pageNumber = data.getLong(KEY_PAGE_NUMBER);
                if (data.containsKey(KEY_DOCUMENTS)) {
                    List<JsonDocument> documents = new ArrayList<>();
                    data.getArray(KEY_DOCUMENTS).forEach(d -> {
                        JsonObject row = (JsonObject) d;
                        documents.add(new JsonDocument(row.getString("id"), row.getObject("document").toBytes()));
                    });
                    return new QueryResultSplit(pageNumber, documents);
                } else {
                    return new QueryResultSplit(pageNumber);
                }
            } else {
                throw new IllegalArgumentException(String.format("Unsupported serializer version: %d", version));
            }
        }
    }
}
