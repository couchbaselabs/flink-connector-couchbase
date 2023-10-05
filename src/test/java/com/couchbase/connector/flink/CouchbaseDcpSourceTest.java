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
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

public class CouchbaseDcpSourceTest {
    private static final Logger LOG = LoggerFactory.getLogger(CouchbaseDcpSourceTest.class);
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

    private static final int docnum = 50 + (int) (Math.random() * 10);
    @BeforeClass
    public static void setUp() {
        LOG.info("Starting couchbase container.");
        couchbase.start();
    }

    private CompletableFuture sendTestDocuments() {
        CompletableFuture result = new CompletableFuture();
        new Thread(() -> {
            try {
                Thread.sleep(3000);
                LOG.info("Sending test data...");
                Cluster cluster = Cluster.connect(couchbase.getConnectionString(), couchbase.getUsername(), couchbase.getPassword());
                Bucket testBucket = cluster.bucket("flink-test");
                testBucket.waitUntilReady(Duration.of(1, ChronoUnit.MINUTES));
                Scope testScope = testBucket.scope("_default");
                Collection testCollection = testScope.collection("_default");
                IntStream.range(0, docnum)
                        .peek(i -> LOG.debug("Creating document {}", i))
                        .forEach(i -> testCollection.insert(String.valueOf(i), i % 2));

                LOG.info("Sent test data");
            } catch (Exception e) {
                LOG.error("Failed to create test data", e);
                result.completeExceptionally(e);
            }
        }).start();
        return result;
    }

    @AfterClass
    public static void tearDown() {
        couchbase.stop();
    }

    @Test
    public void couchbaseSource() throws Exception {
         CouchbaseDcpSource source = new CouchbaseDcpSource(couchbase.getConnectionString(), couchbase.getUsername(), couchbase.getPassword(), "flink-test")
                 .setBoundedness(Boundedness.BOUNDED)
                 .setMaxEvents(docnum);

         Assert.assertEquals("Invalid source boundedness", Boundedness.BOUNDED, source.getBoundedness());
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

         env.setParallelism(2);
         env.getCheckpointConfig().setCheckpointInterval(1000L);

        TestSink<CouchbaseDocumentChange> resultSink = new TestSink<>();

        // just going with the flow...
        env.fromSource(source, WatermarkStrategy.noWatermarks(), CouchbaseDcpSource.class.getSimpleName())
                .addSink(resultSink);

        sendTestDocuments().handleAsync((none, error) -> {
            try {
                env.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });

        JobExecutionResult result = env.execute("couchbase_dcp_source_test").getJobExecutionResult();

        Assert.assertEquals("Invalid document count", docnum, resultSink.getResults().size());
    }
}