// (Copyright) [2020] Confluent, Inc.
package io.confluent.databalancer.integration;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import io.confluent.databalancer.KafkaDataBalanceManager;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.BrokerRemovalDescription;
import org.apache.kafka.clients.admin.BrokerRemovalError;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.errors.PlanComputationException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


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
  private static Duration balancerStopTimeout = Duration.ofSeconds(120);
  protected static Duration removalFinishTimeout = Duration.ofMinutes(3);
  protected static Duration removalPollInterval = Duration.ofSeconds(2);

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

  private void awaitBalanceEngineDisabled() throws InterruptedException {
    KafkaServer controllerServer = controllerKafkaServer();
    KafkaDataBalanceManager dataBalancer = (KafkaDataBalanceManager) controllerServer.kafkaController().dataBalancer().get();
    org.apache.kafka.test.TestUtils.waitForCondition(() -> !dataBalancer.isActive(),
        balancerStopTimeout.toMillis(),
        String.format("The databalancer did not stop in %s", balancerStopTimeout)
    );
  }


  public void addBroker(int newBrokerId) {
    Properties brokerProps = new Properties();
    Map<String, String> brokerOverrideProps = brokerOverrideProps().getOrDefault(newBrokerId, Collections.emptyMap());
    brokerProps.putAll(generalProperties);
    brokerProps.putAll(brokerOverrideProps);
    kafkaCluster.startBroker(newBrokerId, brokerProps);
  }

  /**
   * Attempts to remove broker from the cluster, and verifies post-removal cluster state.
   * @param server KafkaServer to shut down
   * @param exited AtomicBoolean indicating shutdown status of server
   */
  protected void removeBroker(KafkaServer server, AtomicBoolean exited) throws InterruptedException, ExecutionException {
    int brokerToRemoveId = server.config().brokerId();

    Exit.setExitProcedure((statusCode, message) -> {
      info("Shutting down {} as part of broker removal test", server.config().brokerId());
      server.shutdown();
      exited.set(true);
    });

    info("Removing broker with id {}", brokerToRemoveId);
    adminClient.removeBrokers(Collections.singletonList(brokerToRemoveId)).all().get();

    AtomicReference<ApiException> failException = new AtomicReference<>();
    // await removal completion and retry removal in case something went wrong
    org.apache.kafka.test.TestUtils.waitForCondition(() -> {
              Map<Integer, BrokerRemovalDescription> descriptionMap = adminClient.describeBrokerRemovals().descriptions().get();
              if (descriptionMap.isEmpty()) {
                return false;
              }
              BrokerRemovalDescription brokerRemovalDescription = descriptionMap.get(brokerToRemoveId);

              if (isCompletedRemoval(brokerRemovalDescription)) {
                return true;
              } else if (isFailedPlanComputationInRemoval(brokerRemovalDescription)) {
                // a common failure is not having enough metrics for plan computation - simply retry it
                return retryRemoval(brokerRemovalDescription, brokerToRemoveId);
              } else if (isFailedRemoval(brokerRemovalDescription)) {
                String errMsg = String.format("Broker removal failed for an unexpected reason - description object %s", brokerRemovalDescription);
                failException.set((ApiException) brokerRemovalDescription.removalError().get().exception());
                info(errMsg);
                return true;
              } else {
                info("Removal is still pending. PAR: {} BSS: {}",
                        brokerRemovalDescription.partitionReassignmentsStatus(), brokerRemovalDescription.brokerShutdownStatus());
                return false;
              }
            },
            removalFinishTimeout.toMillis(),
            removalPollInterval.toMillis(),
            () -> "Broker removal did not complete successfully in time!"
    );
    if (failException.get() != null) {
      throw failException.get();
    }

    assertTrue("Expected Exit to be called", exited.get());
    org.apache.kafka.test.TestUtils.waitForCondition(() -> adminClient.describeCluster().nodes().get().size() == initialBrokerCount() - 1,
            60_000L, "Cluster size did not shrink!");
    assertEquals("Expected one broker removal to be stored in memory", 1,
            adminClient.describeBrokerRemovals().descriptions().get().size());
    assertEquals("Broker should have no partitions after removal",
            Collections.emptyList(), DataBalancerIntegrationTestUtils.partitionsOnBroker(brokerToRemoveId, adminClient));
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

  /**
   * Disables SBK via the dynamic config and awaits until it is stopped
   */
  protected void disableSBK() throws InterruptedException, ExecutionException {
    ConfigResource defaultBrokerResource = new ConfigResource(ConfigResource.Type.BROKER, "");
    ConfigEntry entry = new ConfigEntry(ConfluentConfigs.BALANCER_ENABLE_CONFIG, "false");
    Collection<AlterConfigOp> configs = Collections.singleton(new AlterConfigOp(entry, AlterConfigOp.OpType.SET));
    adminClient.incrementalAlterConfigs(Collections.singletonMap(defaultBrokerResource, configs)).all().get();

    awaitBalanceEngineDisabled();
    info("SBK was disabled");
  }

  /**
   * Enables SBK via the dynamic config and awaits until it is stopped
   */
  protected void enableSBK() throws InterruptedException, ExecutionException {
    ConfigResource defaultBrokerResource = new ConfigResource(ConfigResource.Type.BROKER, "");
    ConfigEntry entry = new ConfigEntry(ConfluentConfigs.BALANCER_ENABLE_CONFIG, "true");
    Collection<AlterConfigOp> configs = Collections.singleton(new AlterConfigOp(entry, AlterConfigOp.OpType.SET));
    adminClient.incrementalAlterConfigs(Collections.singletonMap(defaultBrokerResource, configs)).all().get();

    awaitBalanceEngineActivation();
    info("SBK was enabled via the dynamic config");
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

  /**
   * Check whether the broker removal operation is in progress,
   * ensuring it is past the initial plan computation phase (as that is likely to fail until we get enough windows)
   */
  protected boolean isRemovalInProgress(BrokerRemovalDescription brokerRemovalDescription) {
    boolean reassignmentInProgress = brokerRemovalDescription.partitionReassignmentsStatus() == BrokerRemovalDescription.PartitionReassignmentsStatus.IN_PROGRESS;
    boolean shutdownInProgress = brokerRemovalDescription.brokerShutdownStatus() == BrokerRemovalDescription.BrokerShutdownStatus.PENDING;
    boolean shutdownComplete = brokerRemovalDescription.brokerShutdownStatus() == BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE;

    return (reassignmentInProgress && shutdownComplete) || (reassignmentInProgress && shutdownInProgress);
  }

  protected boolean isFailedRemoval(BrokerRemovalDescription brokerRemovalDescription) {
    boolean reassignmentFailed = brokerRemovalDescription.partitionReassignmentsStatus() == BrokerRemovalDescription.PartitionReassignmentsStatus.CANCELED
        || brokerRemovalDescription.partitionReassignmentsStatus() == BrokerRemovalDescription.PartitionReassignmentsStatus.FAILED;
    boolean removalFailed = brokerRemovalDescription.brokerShutdownStatus() == BrokerRemovalDescription.BrokerShutdownStatus.FAILED
        || brokerRemovalDescription.brokerShutdownStatus() == BrokerRemovalDescription.BrokerShutdownStatus.CANCELED;

    return removalFailed || reassignmentFailed;
  }

  protected boolean isCanceledRemoval(BrokerRemovalDescription brokerRemovalDescription) {
    boolean reassignmentCanceled = brokerRemovalDescription.partitionReassignmentsStatus() == BrokerRemovalDescription.PartitionReassignmentsStatus.CANCELED;
    boolean removalCanceled = brokerRemovalDescription.brokerShutdownStatus() == BrokerRemovalDescription.BrokerShutdownStatus.CANCELED;
    return reassignmentCanceled || removalCanceled;
  }

  protected boolean isFailedPlanComputationInRemoval(BrokerRemovalDescription brokerRemovalDescription) {
    return brokerRemovalDescription.removalError().isPresent()
        && brokerRemovalDescription.removalError().get().exception() instanceof PlanComputationException;
  }
}
