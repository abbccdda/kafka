// (Copyright) [2020] Confluent, Inc.
package io.confluent.databalancer.integration;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import io.confluent.databalancer.KafkaDataBalanceManager;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.BrokerRemovalDescription;
import org.apache.kafka.clients.admin.BrokerRemovalError;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.common.errors.PlanComputationException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


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

  // To allow a test case to override properties based on broker ID
  protected Map<Integer, Map<String, String>> brokerOverrideProps() {
    return Collections.emptyMap();
  }

  @SuppressWarnings("deprecation")
  @Before
  public void setUp() throws Exception {
    logger = LoggerFactory.getLogger(this.getClass());
    generalProperties = injectTestSpecificProperties(new Properties());
    kafkaCluster = new EmbeddedSBKKafkaCluster();
    kafkaCluster.startZooKeeper();
    kafkaCluster.startBrokers(initialBrokerCount(), generalProperties, brokerOverrideProps());

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

  protected boolean retryRemoval(BrokerRemovalDescription brokerRemovalDescription, int brokerToRemoveId) throws ExecutionException, InterruptedException {
    info("Broker removal failed due to", brokerRemovalDescription.removalError()
        .orElse(new BrokerRemovalError(Errors.NONE, null))
        .exception());
    info("Re-scheduling removal...");
    adminClient.removeBrokers(Collections.singletonList(brokerToRemoveId)).all().get();
    return false;
  }

  protected boolean isCompletedRemoval(BrokerRemovalDescription brokerRemovalDescription) {
    boolean reassignmentCompleted = brokerRemovalDescription.partitionReassignmentsStatus() == BrokerRemovalDescription.PartitionReassignmentsStatus.COMPLETE;
    boolean removalCompleted = brokerRemovalDescription.brokerShutdownStatus() == BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE;

    return removalCompleted && reassignmentCompleted;
  }

  protected boolean isInProgressRemoval(BrokerRemovalDescription brokerRemovalDescription) {
    boolean reassignmentInProgress = brokerRemovalDescription.partitionReassignmentsStatus() == BrokerRemovalDescription.PartitionReassignmentsStatus.PENDING
        || brokerRemovalDescription.partitionReassignmentsStatus() == BrokerRemovalDescription.PartitionReassignmentsStatus.IN_PROGRESS;
    boolean removalInProgress = brokerRemovalDescription.brokerShutdownStatus() == BrokerRemovalDescription.BrokerShutdownStatus.PENDING;

    return removalInProgress || reassignmentInProgress;
  }

  protected boolean isFailedRemoval(BrokerRemovalDescription brokerRemovalDescription) {
    boolean reassignmentFailed = brokerRemovalDescription.partitionReassignmentsStatus() == BrokerRemovalDescription.PartitionReassignmentsStatus.CANCELED
        || brokerRemovalDescription.partitionReassignmentsStatus() == BrokerRemovalDescription.PartitionReassignmentsStatus.FAILED;
    boolean removalFailed = brokerRemovalDescription.brokerShutdownStatus() == BrokerRemovalDescription.BrokerShutdownStatus.FAILED
        || brokerRemovalDescription.brokerShutdownStatus() == BrokerRemovalDescription.BrokerShutdownStatus.CANCELED;

    return removalFailed || reassignmentFailed;
  }

  protected boolean isFailedPlanComputationInRemoval(BrokerRemovalDescription brokerRemovalDescription) {
    return brokerRemovalDescription.removalError().isPresent()
        && brokerRemovalDescription.removalError().get().exception() instanceof PlanComputationException;
  }
}
