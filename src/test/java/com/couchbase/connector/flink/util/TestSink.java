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
