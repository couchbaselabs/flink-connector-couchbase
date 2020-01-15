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

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.connector.flink.CouchbaseDocumentChange;
import com.couchbase.connector.flink.CouchbaseJsonSink;
import com.couchbase.connector.flink.CouchbaseSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Skeleton for a Flink Streaming Job.
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

//    Configuration config = new Configuration();
//    config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);


    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(SECONDS.toMillis(1));
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(SECONDS.toMillis(1));

//    env.addSource(new WikipediaEditsSource())
//        .name("Wikipedia Edits")
//        .map(edit -> JsonDocument.create(
//            edit.getTitle(),
//            JsonObject.fromJson(new ObjectMapper().writeValueAsString(edit))))

    env.addSource(new CouchbaseSource())
        .filter(CouchbaseDocumentChange::isMutation)
        .filter(change -> change.content().length > 0)
        // todo figure out how to handle deletions?
        .map(new MapFunction<CouchbaseDocumentChange, JsonDocument>() {
          @Override
          public JsonDocument map(CouchbaseDocumentChange documentChange) throws Exception {
            try {
              return JsonDocument.create(documentChange.key(), JsonObject.fromJson(new String(documentChange.content(), UTF_8)));
            } catch (Exception e) {
              e.printStackTrace();
              System.out.println("key: " + documentChange.key());
              System.out.println("content: '" + new String(documentChange.content(), UTF_8) + "'");
              throw e;

            }
          }
        })

        .addSink(new CouchbaseJsonSink(singleton("localhost"), "Administrator", "password", "default"))
        //.map(WikipediaEditEvent::getTitle)
        //env.readTextFile("/tmp/flinkin.txt")
        //source
        //.filter(value -> value.("x"))
        .name("Write to Couchbase");

    //.split()
    //.writeAsText("/tmp/flinkout.txt", FileSystem.WriteMode.OVERWRITE);


    /*
     * Here, you can start creating your execution plan for Flink.
     *
     * Start with getting some data from the environment, like
     * 	env.readTextFile(textPath);
     *
     * then, transform the resulting DataStream<String> using operations
     * like
     * 	.filter()
     * 	.flatMap()
     * 	.join()
     * 	.coGroup()
     *
     * and many more.
     * Have a look at the programming guide for the Java API:
     *
     * http://flink.apache.org/docs/latest/apis/streaming/index.html
     *
     */

    // execute program
    env.execute("Flink Streaming Java API Skeleton");
  }


  /*
   .filter(new RichFilterFunction<WikipediaEditEvent>() {
          private Counter counter;

          @Override
          public void open(Configuration parameters) throws Exception {
            this.counter = getRuntimeContext().getMetricGroup().counter("edits");
          }

          @Override
          public boolean filter(WikipediaEditEvent value) throws Exception {
            log.warn("Incrementing counter!");
            counter.inc();
            return true;
          }
        }
   */
}
