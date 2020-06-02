// (Copyright) [2020] Confluent, Inc.
package io.confluent.databalancer.integration;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import io.confluent.databalancer.KafkaDataBalanceManager;
import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.test.IntegrationTest;

import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.zk.EmbeddedZookeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;


@Category(IntegrationTest.class)
public abstract class DataBalancerClusterTestHarness {
  protected Logger logger;

  protected EmbeddedZookeeper zookeeper;
  protected ConfluentAdmin adminClient;
  protected Properties generalProperties;
  protected EmbeddedSBKKafkaCluster kafkaCluster;
  protected List<KafkaConfig> configs = null;
  protected List<KafkaServer> servers = null;
  protected String brokerList = null;

  private static Duration balancerStartTimeout = Duration.ofSeconds(120);

  // To be defined by the test class -- how many brokers initially in the cluster.
  protected abstract int initialBrokerCount();

  // To allow the test case to update any properties it might specifically need.
  protected Properties injectTestSpecificProperties(Properties props) {
    return props; // no-op
  }

  @SuppressWarnings("deprecation")
  @Before
  public void setUp() throws Exception {
    logger = LoggerFactory.getLogger(this.getClass());
    generalProperties = injectTestSpecificProperties(new Properties());
    kafkaCluster = new EmbeddedSBKKafkaCluster();
    kafkaCluster.startZooKeeper();
    kafkaCluster.startBrokers(initialBrokerCount(), generalProperties);

    servers = kafkaCluster.brokers();
    brokerList = TestUtils.getBrokerListStrFromServers(JavaConverters.asScalaBuffer(servers),
        SecurityProtocol.PLAINTEXT);

    Map<String, Object> adminClient = new HashMap<>();
    adminClient.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    this.adminClient = KafkaCruiseControlUtils.createAdmin(adminClient);

    awaitBalanceEngineActivation();
  }

  @After
  public void tearDown() throws Exception {
    if (kafkaCluster != null) {
      kafkaCluster.shutdown();
    }
    if (adminClient != null) {
      adminClient.close();
    }
  }

  public KafkaServer controllerKafkaServer() {
    return servers.stream().filter(s -> s.kafkaController().isActive()).findFirst().get();
  }

  public KafkaServer notControllerKafkaServer() {
    return servers.stream().filter(s -> !s.kafkaController().isActive()).findFirst().get();
  }

  private void awaitBalanceEngineActivation() throws InterruptedException {
    KafkaServer controllerServer = controllerKafkaServer();
    KafkaDataBalanceManager dataBalancer = (KafkaDataBalanceManager) controllerServer.kafkaController().dataBalancer().get();
    org.apache.kafka.test.TestUtils.waitForCondition(dataBalancer::isActive,
        balancerStartTimeout.toMillis(),
        String.format("The databalancer did not start in %s", balancerStartTimeout)
    );
  }

  public void addBroker(int newBrokerId) {
    kafkaCluster.startBroker(newBrokerId, generalProperties);
  }

  public void info(String msg) {
    info(msg, Collections.emptyList());
  }

  public void info(String msg, Object... arguments) {
    String line = String.join("", Collections.nCopies(10, "-"));
    String separator = String.format("%s-%s-%s", line, this.getClass().getName(), line);

    logger.info(separator);
    logger.info(msg, arguments);
    logger.info(separator);
  }
}
