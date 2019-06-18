// (Copyright) [2016 - 2016] Confluent, Inc.
package io.confluent.metrics.reporter.integration;

import kafka.zk.KafkaZkClient;
import kafka.zookeeper.ZooKeeperClient;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.IntegrationTest;

import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import io.confluent.metrics.reporter.ConfluentMetricsReporterConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.CoreUtils;
import kafka.utils.TestUtils;
import kafka.zk.EmbeddedZookeeper;
import scala.Option;
import scala.Option$;
import scala.collection.JavaConversions;

/**
 * Customized test harness of a 2 broker Kafka cluster to run metric reporter. This is essentially
 * Kafka's ZookeeperTestHarness and KafkaServerTestHarness traits combined and ported to Java.
 * We need customization to set brokerList property in the reporter.
 */
@Category(IntegrationTest.class)
public class MetricReporterClusterTestHarness {
    protected static final Option<Properties> EMPTY_SASL_PROPERTIES = Option$.MODULE$.<Properties>empty();

    // ZK Config
    protected EmbeddedZookeeper zookeeper;
    protected String zkConnect;
    protected KafkaZkClient zkClient;
    protected int zkConnectionTimeout = 30000; // a larger connection timeout is required for SASL tests
    // because SASL connections tend to take longer.
    protected int zkSessionTimeout = 6000;

    // Kafka Config
    protected List<KafkaConfig> configs = null;
    protected List<KafkaServer> servers = null;
    protected String brokerList = null;
    protected boolean enableMetricsReporterOnBroker = true;


    public MetricReporterClusterTestHarness() {
    }

    private boolean zkAcls() {
        return securityProtocol() == SecurityProtocol.SASL_PLAINTEXT ||
                securityProtocol() == SecurityProtocol.SASL_SSL;
    }

    @Before
    public void setUp() throws Exception {
        zookeeper = new EmbeddedZookeeper();
        zkConnect = String.format("localhost:%d", zookeeper.port());
        Time time = Time.SYSTEM;
        zkClient = new KafkaZkClient(
            new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, Integer.MAX_VALUE, time,
            "testMetricGroup", "testMetricType"),
            zkAcls(),
            time);

        configs = new ArrayList<>();
        servers = new ArrayList<>();

        // add broker 0
        KafkaConfig config = kafkaConfig(0);
        configs.add(config);
        KafkaServer server = TestUtils.createServer(config, time);
        servers.add(server);

        String broker0List = TestUtils.getBrokerListStrFromServers(JavaConversions.asScalaBuffer(servers),
                                                                   securityProtocol());

        // add broker 1
        config = enableMetricsReporterOnBroker ? kafkaConfigWithMetricReporter(1, broker0List) : kafkaConfig(1);
        configs.add(config);
        server = TestUtils.createServer(config, Time.SYSTEM);
        servers.add(server);

        // create the consumer offset topic
        TestUtils.createTopic(
            zkClient,
            Topic.GROUP_METADATA_TOPIC_NAME,
            config.getInt(KafkaConfig.OffsetsTopicPartitionsProp()),
            1,
            JavaConversions.asScalaBuffer(servers),
            servers.get(0).groupCoordinator().offsetsTopicConfigs()
        );

        brokerList = TestUtils.getBrokerListStrFromServers(JavaConversions.asScalaBuffer(servers),
                                                           securityProtocol());
    }

    protected void injectProperties(Properties props) {
        props.setProperty(KafkaConfig.AutoCreateTopicsEnableProp(), "false");
        props.setProperty(KafkaConfig.NumPartitionsProp(), "1");
    }

    protected void injectMetricReporterProperties(Properties props, String brokerList) {
        props.setProperty(KafkaConfig.MetricReporterClassesProp(), "io.confluent.metrics.reporter.ConfluentMetricsReporter");
        props.setProperty(ConfluentMetricsReporterConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.setProperty(ConfluentMetricsReporterConfig.TOPIC_REPLICAS_CONFIG, "1");
        props.setProperty(ConfluentMetricsReporterConfig.PUBLISH_PERIOD_CONFIG, "500");
        props.setProperty(ConfluentMetricsReporterConfig.WHITELIST_CONFIG, "");

        // force flush every message so that we can generate some Yammer timer metrics
        props.setProperty(KafkaConfig.LogFlushIntervalMessagesProp(), "1");
    }

    protected KafkaConfig kafkaConfig(int brokerId) {
        final Option<java.io.File> noFile = scala.Option.apply(null);
        final Option<SecurityProtocol> noInterBrokerSecurityProtocol = scala.Option.apply(null);
        Properties props = TestUtils.createBrokerConfig(
                brokerId, zkConnect, false, false, TestUtils.RandomPort(), noInterBrokerSecurityProtocol,
                noFile, EMPTY_SASL_PROPERTIES, true, false, TestUtils.RandomPort(), false, TestUtils.RandomPort(), false,
                TestUtils.RandomPort(), Option.<String>empty(), 1, false, 1, (short) 1);
        injectProperties(props);
        return KafkaConfig.fromProps(props);
    }

    private KafkaConfig kafkaConfigWithMetricReporter(int brokerId, String brokerList) {
        final Option<java.io.File> noFile = scala.Option.apply(null);
        final Option<SecurityProtocol> noInterBrokerSecurityProtocol = scala.Option.apply(null);
        Properties props = TestUtils.createBrokerConfig(
                brokerId, zkConnect, false, false, TestUtils.RandomPort(), noInterBrokerSecurityProtocol,
                noFile, EMPTY_SASL_PROPERTIES, true, false, TestUtils.RandomPort(), false, TestUtils.RandomPort(), false,
                TestUtils.RandomPort(), Option.<String>empty(), 1, false, 1, (short) 1);
        injectProperties(props);
        injectMetricReporterProperties(props, brokerList);
        return KafkaConfig.fromProps(props);
    }

    protected SecurityProtocol securityProtocol() {
        return SecurityProtocol.PLAINTEXT;
    }

    @After
    public void tearDown() throws Exception {
        if (servers != null) {
            for (KafkaServer server : servers) {
                server.shutdown();
            }

            // Remove any persistent data
            for (KafkaServer server : servers) {
                CoreUtils.delete(server.config().logDirs());
            }
        }

        if (zkClient != null) {
            zkClient.close();
        }

        if (zookeeper != null) {
            zookeeper.shutdown();
        }
    }
}
