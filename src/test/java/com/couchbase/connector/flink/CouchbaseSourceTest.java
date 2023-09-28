package com.couchbase.connector.flink;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.connector.flink.util.TestSink;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.couchbase.CouchbaseService;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.stream.IntStream;

public class CouchbaseSourceTest {
    private static final Logger LOG = LoggerFactory.getLogger(CouchbaseSourceTest.class);
    @ClassRule
    public static CouchbaseContainer couchbase = new CouchbaseContainer("couchbase/server:enterprise-7.2.0")
            .withBucket(new BucketDefinition("flink-test"))
            .withEnabledServices(CouchbaseService.KV, CouchbaseService.INDEX, CouchbaseService.QUERY)
            .withStartupTimeout(Duration.of(1, ChronoUnit.MINUTES))
            .withStartupAttempts(3)
            .withExposedPorts(
                    9119, 9998, 11213, 21200, 21300,

                    4369, 8091, 8092, 8093, 8094, 9100, 9101, 9102, 9103, 9104, 9105,
                    9110, 9111, 9112, 9112, 9113, 9114, 9115, 9116, 9117, 9118,
                    9120, 9121, 9122, 9130, 9999, 11209, 11210, 21100,

                    8091, 8092, 8093, 8094, 8095, 8096, 8097, 9123, 9140, 11120, 11280
           )
            .withCredentials("Administrator", "password");

    @ClassRule
    public static MiniClusterWithClientResource flink = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build()
    );

    private static final int docnum = (int) (Math.random() * 100);
    @BeforeClass
    public static void setUp() {
        LOG.info("Starting couchbase container.");
        couchbase.start();
    }

    private void sendTestDocuments() {
        new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            LOG.info("Sending test data...");
            int attempts = 10;
            Throwable lastError = null;
            for (; attempts > 0; --attempts) {
                try {
                    Cluster cluster = Cluster.connect(couchbase.getConnectionString(), couchbase.getUsername(), couchbase.getPassword());
                    Bucket testBucket = cluster.bucket("flink-test");
                    testBucket.waitUntilReady(Duration.of(1, ChronoUnit.MINUTES));
                    Scope testScope = testBucket.scope("_default");
                    Collection testCollection = testScope.collection("_default");
                    IntStream.range(0, docnum + 1)
                            .forEach(i -> testCollection.insert(String.valueOf(i), i % 2));
                    lastError = null;
                    break;
                } catch (Throwable e) {
                    while (e.getCause() != null && e.getCause() != e) {
                        e = e.getCause();
                    }
                    LOG.error("Failed to send test data ({}), attempts left: {}", e.getLocalizedMessage(), attempts);
                    lastError = e;
                    try {
                        Thread.sleep(30000);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }

            if (attempts == 0 || lastError != null) {
                throw new RuntimeException("Gave up sending test data", lastError);
            }

            LOG.info("Sent test data");
        }).start();
    }

    @AfterClass
    public static void tearDown() {

    }

    @Test
    public void couchbaseSource() throws Exception {
         CouchbaseSource source = new CouchbaseSource(couchbase.getConnectionString(), couchbase.getUsername(), couchbase.getPassword(), "flink-test")
                 .setBoundedness(Boundedness.BOUNDED)
                 .setMaxEvents(docnum);

         Assert.assertEquals("Invalid source boundedness", Boundedness.BOUNDED, source.getBoundedness());
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

         env.setParallelism(1);
         env.getCheckpointConfig().setCheckpointInterval(1000L);

        TestSink<CouchbaseDocumentChange> resultSink = new TestSink<>();

        // just going with the flow...
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "couchbase_dcp_stream")
                .addSink(resultSink);

        sendTestDocuments();
        JobExecutionResult result = env.execute("couchbase_dcp_source_test").getJobExecutionResult();

        Assert.assertEquals("Invalid document count", docnum, resultSink.getResults().size());
    }
}