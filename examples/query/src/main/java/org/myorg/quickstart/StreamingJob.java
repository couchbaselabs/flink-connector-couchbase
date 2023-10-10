/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart;

import com.couchbase.connector.flink.CouchbaseDocumentChange;
import com.couchbase.connector.flink.CouchbaseJsonSink;
import com.couchbase.connector.flink.CouchbaseSource;
import com.couchbase.connector.flink.JsonDocument;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Skeleton for a Couchbase SQL++ query Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {
  private static final Logger log = LoggerFactory.getLogger(StreamingJob.class);

  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(SECONDS.toMillis(3));
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(SECONDS.toMillis(1));

    env.setMaxParallelism(2);
    env.setParallelism(2);

    CouchbaseSource source = new CouchbaseQuerySource("localhost", "Administrator", "password", "travel-sample")
            .query("SELECT * FROM `airport` WHERE `city` = $city")
            .withArgument("city", "Matrix");

    env.addSource(source, "Airports in the Matrix")
        .shuffle()
        .map(new MapFunction<JsonDocument, JsonDocument>() {
          @Override
          public JsonDocument map(JsonDocument document) throws Exception {
            try {
              return document;
            } catch (Exception e) {
              e.printStackTrace();
              System.out.println("key: " + document.key());
              System.out.println("content: '" + new String(document.content(), UTF_8) + "'");
              throw e;

            }
          }
        })

        .addSink(new CouchbaseCollectionSink("localhost", "Administrator", "password", "sink"))
        .name("Write to Couchbase");

    // execute program
    env.execute("Flink Streaming Java API Skeleton");
  }
}
