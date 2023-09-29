package com.couchbase.connector.flink;

import com.couchbase.client.java.json.JsonObject;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class CouchbaseQuerySource implements Source<JsonObject, CouchbaseQuerySource.QueryResultSplit, Integer> {

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<QueryResultSplit, Integer> createEnumerator(SplitEnumeratorContext<QueryResultSplit> splitEnumeratorContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<QueryResultSplit, Integer> restoreEnumerator(SplitEnumeratorContext<QueryResultSplit> splitEnumeratorContext, Integer integer) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<QueryResultSplit> getSplitSerializer() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<Integer> getEnumeratorCheckpointSerializer() {
        return null;
    }

    @Override
    public SourceReader<JsonObject, QueryResultSplit> createReader(SourceReaderContext sourceReaderContext) throws Exception {
        return null;
    }

    public class QueryResultSplit implements SourceSplit {

    }
}
