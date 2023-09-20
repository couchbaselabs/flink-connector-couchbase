package com.couchbase.connector.flink;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.manager.bucket.BucketSettings;
import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.TestEnvironment;
import org.junit.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.couchbase.CouchbaseService;

import java.util.stream.IntStream;

public class CouchbaseSourceTest {
    @ClassRule
    public static GenericContainer couchbase = new CouchbaseContainer("couchbase/server:enterprise-7.2.0")
            .withBucket(new BucketDefinition("flink-test"))
            .withEnabledServices(CouchbaseService.KV, CouchbaseService.INDEX, CouchbaseService.QUERY)
            .withCredentials("Administrator", "password");

    @ClassRule
    public static MiniClusterWithClientResource flink = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build()
    );

    private static CouchbaseSource subject = new CouchbaseSource(
            "test-subject",
            "localhost",
            "Administrator",
            "password",
            "flink-test"
    );

    private int docunum = (int) (Math.random() * 1000);
    @BeforeClass
    public static void setUp() {
        try {
            Cluster cluster = Cluster.connect("localhost", "Administrator", "password");
            Bucket testBucket = cluster.bucket("flink-test");
            Scope testScope = testBucket.scope("_default");
            Collection testCollection = testScope.collection("_default");
            IntStream.of(docnum)
                    .forEach(i -> testCollection.insert(String.valueOf(i), i % 2));
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize test data", e);
        }
    }

    @AfterClass
    public static void tearDown() {

    }

    @Test
    public void couchbaseSource() {
         CouchbaseSource source = new CouchbaseSource("localhost", "Administrator", "password", "flink-test");
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

         env.setParallelism(1);
         env.getCheckpointConfig().setCheckpointInterval(1000L);
         
    }
}