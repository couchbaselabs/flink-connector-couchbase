package com.couchbase.connector.flink.util;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.connector.flink.CouchbaseDocumentChange;
import com.couchbase.connector.flink.JsonDocument;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestSink<IN> implements SinkFunction<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(TestSink.class);
    private static final List results = Collections.synchronizedList(new ArrayList<>());

    public TestSink() {
        results.clear();
    }
    @Override
    public void invoke(IN value, Context context) throws Exception {
        if (value instanceof CouchbaseDocumentChange) {
            LOG.debug("Test sink received an item#{}: {}", results.size() + 1, ((CouchbaseDocumentChange) value).key());
        } else {
            LOG.debug("Test sink received an item#{}: {}", results.size() + 1, value);
        }
        results.add(value);
    }

    public List getResults() {
        return results;
    }
}
