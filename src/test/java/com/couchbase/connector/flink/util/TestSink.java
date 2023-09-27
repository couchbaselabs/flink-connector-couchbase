package com.couchbase.connector.flink.util;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;

public class TestSink<IN> extends ArrayList<IN> implements SinkFunction<IN> {
    @Override
    public void invoke(IN value, Context context) throws Exception {
        add(value);
    }
}
