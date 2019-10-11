// Copyright 2018, Confluent Inc.

package io.confluent.aegis.proxy;

import io.confluent.aegis.config.AegisConfig;
import io.confluent.kafka.test.cluster.EmbeddedKafkaCluster;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.confluent.aegis.config.AegisConfig.BROKER_ENDPOINTS_CONFIG;
import static io.confluent.aegis.config.AegisConfig.TENANT_ENDPOINTS_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(IntegrationTest.class)
public class AegisIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(AegisIntegrationTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    private MiniAegisCluster aegisCluster;

    private EmbeddedKafkaCluster kafkaCluster;

    @After
    public void shutdown() throws Exception {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        if (aegisCluster != null) {
            futures.add(aegisCluster.shutdown());
            aegisCluster = null;
        }
        if (kafkaCluster != null) {
            kafkaCluster.shutdown();
            kafkaCluster = null;
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).get();
    }

    @Test
    public void testCreateBrokerAndProxy() throws Exception {
        kafkaCluster = new EmbeddedKafkaCluster();
        kafkaCluster.startZooKeeper();
        kafkaCluster.startBrokers(1, new Properties());
        aegisCluster = new MiniAegisClusterBuilder().
            setKafkaCluster(kafkaCluster).
            add(new MiniAegisBuilder(new AegisConfig())).
            build();
    }

    // Temporarily disabled, failing since flexible versions introduction
    @Ignore
    @Test
    public void testAdminRpcs() throws Exception {
        kafkaCluster = new EmbeddedKafkaCluster();
        kafkaCluster.startZooKeeper();
        kafkaCluster.startBrokers(1, new Properties());
        aegisCluster = new MiniAegisClusterBuilder().
            setKafkaCluster(kafkaCluster).
            add(new MiniAegisBuilder(new AegisConfig())).
            build();
        try (AdminClient client = AdminClient.
                create(aegisCluster.createClientProps(0))) {
            assertEquals(kafkaCluster.brokers().get(0).clusterId(),
                client.describeCluster().clusterId().get());
            client.createTopics(Collections.singleton(
                new NewTopic("foo", 1, (short) 1))).all();
            TestUtils.waitForCondition(() -> {
                try {
                    return client.listTopics().names().get().contains("foo");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, "Failed to list the foo topic.");
        }
    }

    @Test
    public void testAegisWithoutBrokers() throws Exception {
        aegisCluster = new MiniAegisClusterBuilder().
            add(new MiniAegisBuilder(new AegisConfig(
                TENANT_ENDPOINTS_CONFIG, "PLAINTEXT://localhost:0",
                BROKER_ENDPOINTS_CONFIG, "PLAINTEXT://localhost:0"))).
            build();
        Properties adminClientProps = aegisCluster.createClientProps(0);
        adminClientProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 1);
        try (AdminClient client = AdminClient.create(adminClientProps)) {
            try {
                client.createTopics(Collections.singleton(
                    new NewTopic("foo", 1, (short) 1))).all().get();
                fail("Expected createTopics to fail because the broker is not running.");
            } catch (ExecutionException e) {
                log.trace("without brokers, createTopics failed", e);
                assertTrue(e.getCause().getMessage().contains("Timed out"));
            }
        }
    }

    // Temporarily disabled, failing since flexible versions introduction
    @Ignore
    @Test
    public void testAdminRpcsWithMultipleBrokers() throws Exception {
        kafkaCluster = new EmbeddedKafkaCluster();
        kafkaCluster.startZooKeeper();
        kafkaCluster.startBrokers(3, new Properties());
        aegisCluster = new MiniAegisClusterBuilder().
            setKafkaCluster(kafkaCluster).
            add(new MiniAegisBuilder(new AegisConfig())).
            add(new MiniAegisBuilder(new AegisConfig())).
            add(new MiniAegisBuilder(new AegisConfig())).
            build();
        try (AdminClient client = AdminClient.
            create(aegisCluster.createClientProps(0))) {
            assertEquals(kafkaCluster.brokers().get(0).clusterId(),
                client.describeCluster().clusterId().get());
            client.createTopics(Collections.singleton(
                new NewTopic("foo", 1, (short) 1))).all();
            TestUtils.waitForCondition(() -> {
                try {
                    return client.listTopics().names().get().contains("foo");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, "Failed to list the foo topic.");
        }
    }
}
