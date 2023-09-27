package com.couchbase.connector.flink;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.connector.flink.util.TestSink;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.couchbase.CouchbaseService;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.stream.IntStream;

public class CouchbaseSourceTest {
    private static final Logger LOG = LoggerFactory.getLogger(CouchbaseSourceTest.class);
    @ClassRule
    public static GenericContainer couchbase = new CouchbaseContainer("couchbase/server:enterprise-7.2.0")
            .withBucket(new BucketDefinition("flink-test"))
            .withEnabledServices(CouchbaseService.KV, CouchbaseService.INDEX, CouchbaseService.QUERY)
            .withStartupTimeout(Duration.of(1, ChronoUnit.MINUTES))
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

    private static CouchbaseSource subject = new CouchbaseSource(
            "localhost",
            "Administrator",
            "password",
            "flink-test"
    );

    private static final int docnum = (int) (Math.random() * 1000);
    @BeforeClass
    public static void setUp() {
        LOG.info("Initializing test data...");
        int attempts = 10;
        for (; attempts >= 0; attempts--) {
            try {
                Thread.sleep(10000);
                Cluster cluster = Cluster.connect("localhost", "Administrator", "password");
                Bucket testBucket = cluster.bucket("flink-test");
                Scope testScope = testBucket.scope("_default");
                Collection testCollection = testScope.collection("_default");
                IntStream.of(docnum)
                        .forEach(i -> testCollection.insert(String.valueOf(i), i % 2));
                break;
            } catch (Exception e) {
                LOG.error("Failed ot initialize test data, attempts left: " + attempts, e);
            }
        }

        if (attempts == 0) {
            throw new RuntimeException("Exhausted test data initialization attempts");
        }

        LOG.info("Initialized test data");
    }

    @AfterClass
    public static void tearDown() {

    }

    @Test
    public void couchbaseSource() throws Exception {
         CouchbaseSource source = new CouchbaseSource("localhost", "Administrator", "password", "flink-test")
                 .setBoundedness(Boundedness.BOUNDED)
                 .setMaxEvents(docnum);

         Assert.assertEquals("Invalid source boundedness", Boundedness.BOUNDED, source.getBoundedness());
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

         env.setParallelism(1);
         env.getCheckpointConfig().setCheckpointInterval(1000L);

        TestSink<CouchbaseDocumentChange> resultSink = new TestSink<>();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "couchbase_dcp_stream")
                .addSink(resultSink);

        env.execute("couchbase_dcp_source_test");

        Assert.assertEquals("Invalid document count", docnum, resultSink.size());
    }
}