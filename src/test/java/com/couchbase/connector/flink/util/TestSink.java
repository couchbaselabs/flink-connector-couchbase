package com.couchbase.connector.flink.util;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class TestSink<IN> implements SinkFunction<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(TestSink.class);
    private static final ArrayList results = new ArrayList();

    public TestSink() {
        results.clear();
    }
    @Override
    public void invoke(IN value, Context context) throws Exception {
        LOG.debug("Test sink received an item#{}: {}", results.size() + 1, value);
        results.add(value);
    }

    public ArrayList getResults() {
        return results;
    }
}
