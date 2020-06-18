// (Copyright) [2019 - 2019] Confluent, Inc.
package io.confluent.kafka.multitenant.integration.test;

import kafka.server.BrokerBackpressureConfig;
import kafka.server.ClientQuotaManager;
import kafka.server.DiskUsageBasedThrottlingConfig;
import kafka.server.DiskUsageBasedThrottlingConfig$;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.clients.admin.AdminClient;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import io.confluent.kafka.multitenant.integration.cluster.PhysicalCluster;
import io.confluent.kafka.multitenant.quota.TenantQuotaCallback;
import io.confluent.kafka.server.plugins.policy.AlterConfigPolicy;
import io.confluent.kafka.server.plugins.policy.TopicPolicyConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.server.ThreadUsageMetrics;

import scala.collection.JavaConverters;

import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;


/**
 * Set of unit tests that test backpressure-specific functionality added to Apache Kafka classes:
 * QuotaFactory and ClientQuotaManager, ClientQuotaRequestManager.
 * Since we initially support backpressure in clusters that have tenant quotas enabled via
 * TenantQuotaCallback, which is part of the plugins, these tests are also part of plugins tests.
 */
@Category(IntegrationTest.class)
public class BrokerBackpressureTest {

  private static final int BROKER_COUNT = 1;

  private final Integer numIoThreads = 8;
  private final Integer numNetworkThreads = 4;
  private final Integer maxQueueSize = 500;
  private final AlterConfigsOptions configsOptions = new AlterConfigsOptions().timeoutMs(30000);
  private final ConfigResource defaultBrokerConfigResource =
      new ConfigResource(ConfigResource.Type.BROKER, "");

  private IntegrationTestHarness testHarness;

  @Before
  public void setUp() {
    testHarness = new IntegrationTestHarness(BROKER_COUNT);
  }

  @After
  public void tearDown() throws Exception {
    testHarness.shutdown();
  }

  private Properties brokerProps() {
    Properties props = new Properties();
    props.put(KafkaConfig$.MODULE$.AlterConfigPolicyClassNameProp(), AlterConfigPolicy.class.getName());
    props.put(KafkaConfig$.MODULE$.NumNetworkThreadsProp(), numNetworkThreads.toString());
    props.put(KafkaConfig$.MODULE$.NumIoThreadsProp(), numIoThreads.toString());
    props.put(KafkaConfig$.MODULE$.QueuedMaxRequestsProp(), maxQueueSize.toString());
    props.put(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG, "1");
    return props;
  }

  private Properties brokerPropsWithTenantQuotas() {
    Properties props = brokerProps();
    props.put(KafkaConfig$.MODULE$.ClientQuotaCallbackClassProp(), TenantQuotaCallback.class.getName());
    // EXTERNAL is what is set by IntegrationTestHarness
    props.put(ConfluentConfigs.MULTITENANT_LISTENER_NAMES_CONFIG, "EXTERNAL");
    return props;
  }

  private Properties brokerPropsWithInvalidMultitenantListenerName() {
    Properties props = brokerProps();
    props.put(KafkaConfig$.MODULE$.ClientQuotaCallbackClassProp(), TenantQuotaCallback.class.getName());
    props.put(ConfluentConfigs.MULTITENANT_LISTENER_NAMES_CONFIG, "INVALID");
    return props;
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testNoTenantQuotasNoBackpressureConfig() throws Exception {
    final PhysicalCluster physicalCluster = testHarness.start(brokerProps());

    KafkaServer broker = physicalCluster.kafkaCluster().brokers().get(0);
    assertFalse("Expected consume backpressure to be disabled",
                broker.quotaManagers().fetch().backpressureEnabled());
    assertFalse("Expected produce backpressure to be disabled",
                broker.quotaManagers().produce().backpressureEnabled());
    assertFalse("Expected request backpressure to be disabled",
                broker.quotaManagers().request().backpressureEnabled());

    assertFalse(broker.quotaManagers().fetch().tenantLevelQuotasEnabled());
    assertFalse(broker.quotaManagers().produce().tenantLevelQuotasEnabled());
    assertFalse(broker.quotaManagers().request().tenantLevelQuotasEnabled());

    assertEquals(numIoThreads * 100.0, ThreadUsageMetrics.ioThreadsCapacity(broker.metrics()), 1.0);
    assertEquals(numNetworkThreads * 100.0,
                 ThreadUsageMetrics.networkThreadsCapacity(broker.metrics(), JavaConverters.asScalaBuffer(Collections.singletonList("EXTERNAL"))), 1.0);
  }

  @Test
  public void testNoBackpressureConfig() throws Exception {
    final PhysicalCluster physicalCluster = testHarness.start(brokerPropsWithTenantQuotas());

    KafkaServer broker = physicalCluster.kafkaCluster().brokers().get(0);
    assertFalse("Expected consume backpressure to be disabled",
                broker.quotaManagers().fetch().backpressureEnabled());
    assertFalse("Expected produce backpressure to be disabled",
                broker.quotaManagers().produce().backpressureEnabled());
    assertFalse("Expected request backpressure to be disabled",
                broker.quotaManagers().request().backpressureEnabled());

    assertTrue(broker.quotaManagers().fetch().tenantLevelQuotasEnabled());
    assertTrue(broker.quotaManagers().produce().tenantLevelQuotasEnabled());
    assertTrue(broker.quotaManagers().request().tenantLevelQuotasEnabled());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testFetchBackpressureOnlyConfig() throws Exception {
    Properties props = brokerPropsWithTenantQuotas();
    props.put(ConfluentConfigs.BACKPRESSURE_TYPES_CONFIG, "fetch");
    final PhysicalCluster physicalCluster = testHarness.start(props);

    KafkaServer broker = physicalCluster.kafkaCluster().brokers().get(0);
    assertTrue("Expected consume backpressure to be enabled",
               broker.quotaManagers().fetch().backpressureEnabled());
    assertFalse("Expected produce backpressure to be disabled",
                broker.quotaManagers().produce().backpressureEnabled());
    assertFalse("Expected request backpressure to be disabled",
                broker.quotaManagers().request().backpressureEnabled());

    assertEquals(numIoThreads * 100.0, ThreadUsageMetrics.ioThreadsCapacity(broker.metrics()), 1.0);
    assertEquals(numNetworkThreads * 100.0,
                 ThreadUsageMetrics.networkThreadsCapacity(broker.metrics(),
                     JavaConverters.asScalaBuffer(Collections.singletonList("EXTERNAL"))),
                 1.0);
  }

  @Test
  public void testFetchAndProduceBackpressureOnlyConfig() throws Exception {
    Properties props = brokerPropsWithTenantQuotas();
    props.put(ConfluentConfigs.BACKPRESSURE_TYPES_CONFIG, "fetch,produce");
    final PhysicalCluster physicalCluster = testHarness.start(props);

    KafkaServer broker = physicalCluster.kafkaCluster().brokers().get(0);
    assertTrue("Expected consume backpressure to be enabled",
               broker.quotaManagers().fetch().backpressureEnabled());
    assertTrue("Expected produce backpressure to be enabled",
               broker.quotaManagers().produce().backpressureEnabled());
    assertFalse("Expected request backpressure to be disabled",
                broker.quotaManagers().request().backpressureEnabled());
  }

  @Test
  public void testFetchAndProduceAndRequestBackpressureConfig() throws Exception {
    Properties props = brokerPropsWithTenantQuotas();
    props.put(ConfluentConfigs.BACKPRESSURE_TYPES_CONFIG, "fetch,produce,request");
    final PhysicalCluster physicalCluster = testHarness.start(props);

    KafkaServer broker = physicalCluster.kafkaCluster().brokers().get(0);
    assertTrue("Expected consume backpressure to be enabled",
               broker.quotaManagers().fetch().backpressureEnabled());
    assertTrue("Expected produce backpressure to be enabled",
               broker.quotaManagers().produce().backpressureEnabled());
    assertTrue("Expected request backpressure to be enabled",
               broker.quotaManagers().request().backpressureEnabled());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testRequestBackpressureConfig() throws Exception {
    Properties props = brokerPropsWithTenantQuotas();
    props.put(ConfluentConfigs.BACKPRESSURE_TYPES_CONFIG, "request");
    final PhysicalCluster physicalCluster = testHarness.start(props);

    KafkaServer broker = physicalCluster.kafkaCluster().brokers().get(0);
    assertFalse("Expected consume backpressure to be disabled",
                broker.quotaManagers().fetch().backpressureEnabled());
    assertFalse("Expected produce backpressure to be disabled",
                broker.quotaManagers().produce().backpressureEnabled());
    assertTrue("Expected request backpressure to be enabled",
               broker.quotaManagers().request().backpressureEnabled());
    assertEquals((double) maxQueueSize,
                 broker.quotaManagers().request().dynamicBackpressureConfig().maxQueueSize(), 0);
    assertEquals(ConfluentConfigs.BACKPRESSURE_REQUEST_MIN_BROKER_LIMIT_DEFAULT.doubleValue(),
            broker.quotaManagers().request().dynamicBackpressureConfig().minBrokerRequestQuota(), 0.0);
    assertEquals(ConfluentConfigs.BACKPRESSURE_REQUEST_QUEUE_SIZE_PERCENTILE_DEFAULT,
            broker.quotaManagers().request().dynamicBackpressureConfig().queueSizePercentile());

    assertEquals(numIoThreads * 100.0, ThreadUsageMetrics.ioThreadsCapacity(broker.metrics()), 1.0);
    assertEquals(numNetworkThreads * 100.0,
                 ThreadUsageMetrics.networkThreadsCapacity(broker.metrics(), JavaConverters.asScalaBuffer(Collections.singletonList("EXTERNAL"))), 1.0);
  }

  @Test
  public void testNonDefaultRequestBackpressureConfig() throws Exception {
    Properties props = brokerPropsWithTenantQuotas();
    props.put(ConfluentConfigs.BACKPRESSURE_TYPES_CONFIG, "request");
    props.put(ConfluentConfigs.BACKPRESSURE_REQUEST_MIN_BROKER_LIMIT_CONFIG, "150");
    props.put(ConfluentConfigs.BACKPRESSURE_REQUEST_QUEUE_SIZE_PERCENTILE_CONFIG, "p99");
    final PhysicalCluster physicalCluster = testHarness.start(props);

    KafkaServer broker = physicalCluster.kafkaCluster().brokers().get(0);
    assertTrue("Expected request backpressure to be enabled",
            broker.quotaManagers().request().backpressureEnabled());
    assertEquals((double) maxQueueSize,
            broker.quotaManagers().request().dynamicBackpressureConfig().maxQueueSize(), 0);
    assertEquals(150.0, broker.quotaManagers().request().dynamicBackpressureConfig().minBrokerRequestQuota(), 0.0);
    assertEquals("p99", broker.quotaManagers().request().dynamicBackpressureConfig().queueSizePercentile());
  }

  @Test
  public void testRequestBackpressureConfigWithInvalidValuesSetsAcceptedValues() throws Exception {
    Properties props = brokerPropsWithTenantQuotas();
    props.put(ConfluentConfigs.BACKPRESSURE_TYPES_CONFIG, "request");
    props.put(ConfluentConfigs.BACKPRESSURE_REQUEST_MIN_BROKER_LIMIT_CONFIG, "0");
    props.put(ConfluentConfigs.BACKPRESSURE_REQUEST_QUEUE_SIZE_PERCENTILE_CONFIG, "100");
    final PhysicalCluster physicalCluster = testHarness.start(props);

    KafkaServer broker = physicalCluster.kafkaCluster().brokers().get(0);
    assertTrue("Expected request backpressure to be enabled",
            broker.quotaManagers().request().backpressureEnabled());
    assertEquals((double) maxQueueSize,
            broker.quotaManagers().request().dynamicBackpressureConfig().maxQueueSize(), 0);
    assertEquals(BrokerBackpressureConfig.MinBrokerRequestQuota(),
            broker.quotaManagers().request().dynamicBackpressureConfig().minBrokerRequestQuota(), 0.0);
    assertEquals(ConfluentConfigs.BACKPRESSURE_REQUEST_QUEUE_SIZE_PERCENTILE_DEFAULT,
            broker.quotaManagers().request().dynamicBackpressureConfig().queueSizePercentile());
  }

  @Test
  public void testDynamicRequestBackpressureConfig() throws Exception {
    Properties props = brokerPropsWithTenantQuotas();
    props.put(ConfluentConfigs.BACKPRESSURE_TYPES_CONFIG, "request");
    final PhysicalCluster physicalCluster = testHarness.start(props);

    KafkaServer broker0 = physicalCluster.kafkaCluster().brokers().get(0);
    assertTrue("Expected request backpressure to be enabled",
            broker0.quotaManagers().request().backpressureEnabled());
    assertEquals(ConfluentConfigs.BACKPRESSURE_REQUEST_MIN_BROKER_LIMIT_DEFAULT,
            broker0.quotaManagers().request().dynamicBackpressureConfig().minBrokerRequestQuota(), 0.0);
    assertEquals(ConfluentConfigs.BACKPRESSURE_REQUEST_QUEUE_SIZE_PERCENTILE_DEFAULT,
            broker0.quotaManagers().request().dynamicBackpressureConfig().queueSizePercentile());

    AdminClient adminClient = physicalCluster.superAdminClient();
    adminClient.incrementalAlterConfigs(
            backpressureConfig(ConfluentConfigs.BACKPRESSURE_REQUEST_MIN_BROKER_LIMIT_CONFIG, "100"), configsOptions).all().get();

    // verify min broker request quota got updated on all brokers
    for (KafkaServer broker : physicalCluster.kafkaCluster().brokers()) {
      TestUtils.waitForCondition(
              () -> broker.quotaManagers().request().dynamicBackpressureConfig().minBrokerRequestQuota() == 100,
              "Expected min broker request limit to be updated to 100 on broker " + broker.config().brokerId());
      // but queue size percentile setting did not change
      assertEquals(ConfluentConfigs.BACKPRESSURE_REQUEST_QUEUE_SIZE_PERCENTILE_DEFAULT,
              broker.quotaManagers().request().dynamicBackpressureConfig().queueSizePercentile());
    }

    adminClient.incrementalAlterConfigs(
            backpressureConfig(ConfluentConfigs.BACKPRESSURE_REQUEST_QUEUE_SIZE_PERCENTILE_CONFIG, "p99"), configsOptions).all().get();

    // verify queue size percentile got updated on all brokers
    for (KafkaServer broker : physicalCluster.kafkaCluster().brokers()) {
      TestUtils.waitForCondition(
              () -> broker.quotaManagers().request().dynamicBackpressureConfig().queueSizePercentile().equals("p99"),
              "Expected queue size percentile to be updated to `p99` on broker " + broker.config().brokerId());
      // and min broker request quota did not change
      assertEquals(100.0, broker.quotaManagers().request().dynamicBackpressureConfig().minBrokerRequestQuota(), 0.0);
    }

    // test that dynamically updating percentile to an invalid value sets it to default `p95`
    adminClient.incrementalAlterConfigs(
            backpressureConfig(ConfluentConfigs.BACKPRESSURE_REQUEST_QUEUE_SIZE_PERCENTILE_CONFIG, "p101"), configsOptions).all().get();
    for (KafkaServer broker : physicalCluster.kafkaCluster().brokers()) {
      TestUtils.waitForCondition(
              () -> broker.quotaManagers().request().dynamicBackpressureConfig().queueSizePercentile().equals(ConfluentConfigs.BACKPRESSURE_REQUEST_QUEUE_SIZE_PERCENTILE_DEFAULT),
              "Expected queue size percentile to be updated to `p95` on broker " + broker.config().brokerId());
    }

    // verify that dynamically setting min broker request limit to too small value sets it to the lowest acceptable value
    adminClient.incrementalAlterConfigs(
            backpressureConfig(ConfluentConfigs.BACKPRESSURE_REQUEST_MIN_BROKER_LIMIT_CONFIG, "-1"), configsOptions).all().get();

    for (KafkaServer broker : physicalCluster.kafkaCluster().brokers()) {
      TestUtils.waitForCondition(
              () -> broker.quotaManagers().request().dynamicBackpressureConfig().minBrokerRequestQuota() == BrokerBackpressureConfig.MinBrokerRequestQuota(),
              "Expected min broker request limit to be updated to 10 on broker " + broker.config().brokerId());
    }
  }

  @Test
  public void testRequestBackpressureConfigWithInvalidTenantListener() throws Exception {
    Properties props = brokerPropsWithInvalidMultitenantListenerName();
    props.put(ConfluentConfigs.BACKPRESSURE_TYPES_CONFIG, "fetch,produce,request");
    final PhysicalCluster physicalCluster = testHarness.start(props);

    KafkaServer broker = physicalCluster.kafkaCluster().brokers().get(0);
    assertFalse("Expected request backpressure to be disabled",
                broker.quotaManagers().request().backpressureEnabled());

    // listener is not required for bandwidth backpressure
    assertTrue("Expected produce backpressure to be enabled",
                broker.quotaManagers().produce().backpressureEnabled());
    assertTrue("Expected consume backpressure to be enabled",
               broker.quotaManagers().fetch().backpressureEnabled());
  }

  @Test
  public void testProduceBackpressureConfig() throws Exception {
    Properties props = brokerPropsWithTenantQuotas();
    props.put(ConfluentConfigs.BACKPRESSURE_TYPES_CONFIG, "produce");
    final PhysicalCluster physicalCluster = testHarness.start(props);

    KafkaServer broker = physicalCluster.kafkaCluster().brokers().get(0);
    assertFalse("Expected consume backpressure to be disabled",
                broker.quotaManagers().fetch().backpressureEnabled());
    assertTrue("Expected produce backpressure to be enabled",
               broker.quotaManagers().produce().backpressureEnabled());
    assertFalse("Expected request backpressure to be disabled",
                broker.quotaManagers().request().backpressureEnabled());
  }

  @Test
  public void testBackpressureDisabledWhenTenantQuotasDisabled() throws Exception {
    Properties props = brokerProps();
    props.put(ConfluentConfigs.BACKPRESSURE_TYPES_CONFIG, "fetch,produce,request");
    final PhysicalCluster physicalCluster = testHarness.start(props);

    KafkaServer broker = physicalCluster.kafkaCluster().brokers().get(0);
    assertFalse("Expected consume backpressure to be disabled",
                broker.quotaManagers().fetch().backpressureEnabled());
    assertFalse("Expected produce backpressure to be disabled",
                broker.quotaManagers().produce().backpressureEnabled());
    assertFalse("Expected request backpressure to be disabled",
                broker.quotaManagers().request().backpressureEnabled());
  }

  @Test
  public void testInvalidBackressureTypesAreIgnored() throws Exception {
    Properties props = brokerPropsWithTenantQuotas();
    props.put(ConfluentConfigs.BACKPRESSURE_TYPES_CONFIG, "randomtype,produce,LeaderReplication");
    final PhysicalCluster physicalCluster = testHarness.start(props);

    KafkaServer broker = physicalCluster.kafkaCluster().brokers().get(0);
    assertFalse("Expected consume backpressure to be disabled",
                broker.quotaManagers().fetch().backpressureEnabled());
    assertTrue("Expected produce backpressure to be enabled",
               broker.quotaManagers().produce().backpressureEnabled());
    assertFalse("Expected request backpressure to be disabled",
                broker.quotaManagers().request().backpressureEnabled());
  }

  @Test
  public void testDynamicBackpressureConfig() throws Exception {
    final PhysicalCluster physicalCluster = testHarness.start(brokerPropsWithTenantQuotas());

    // verify backpressure is disabled on every broker
    for (KafkaServer broker : physicalCluster.kafkaCluster().brokers()) {
      assertFalse("Expected consume backpressure to be disabled on broker " + broker.config().brokerId(),
                  broker.quotaManagers().fetch().backpressureEnabled());
      assertFalse("Expected produce backpressure to be disabled on broker " + broker.config().brokerId(),
                  broker.quotaManagers().produce().backpressureEnabled());
      assertFalse("Expected request backpressure to be disabled on broker " + broker.config().brokerId(),
                  broker.quotaManagers().request().backpressureEnabled());
    }

    AdminClient adminClient = physicalCluster.superAdminClient();
    adminClient.incrementalAlterConfigs(
            backpressureConfig(ConfluentConfigs.BACKPRESSURE_TYPES_CONFIG, "fetch,produce,request"), configsOptions).all().get();

    // verify backpressure is enabled on every broker
    for (KafkaServer broker : physicalCluster.kafkaCluster().brokers()) {
      TestUtils.waitForCondition(
          () -> broker.quotaManagers().fetch().backpressureEnabled(),
          "Expected consume backpressure to be enabled on broker " + broker.config().brokerId());
      TestUtils.waitForCondition(
          () -> broker.quotaManagers().produce().backpressureEnabled(),
          "Expected produce backpressure to be enabled on broker " + broker.config().brokerId());
      TestUtils.waitForCondition(
          () -> broker.quotaManagers().request().backpressureEnabled(),
          "Expected request backpressure to be enabled on broker " + broker.config().brokerId());
      assertEquals((double) maxQueueSize,
                   broker.quotaManagers().request().dynamicBackpressureConfig().maxQueueSize(), 0);
    }

    // disable one backpressure type
    adminClient.incrementalAlterConfigs(backpressureConfig(
            ConfluentConfigs.BACKPRESSURE_TYPES_CONFIG, "fetch,produce"), configsOptions).all().get();
    for (KafkaServer broker : physicalCluster.kafkaCluster().brokers()) {
      assertTrue("Expected consume backpressure to be enabled on broker " + broker.config().brokerId(),
                 broker.quotaManagers().fetch().backpressureEnabled());
      assertTrue("Expected produce backpressure to be enabled on broker " + broker.config().brokerId(),
                 broker.quotaManagers().produce().backpressureEnabled());
      TestUtils.waitForCondition(
          () -> !broker.quotaManagers().request().backpressureEnabled(),
          "Expected request backpressure to be disabled on broker " + broker.config().brokerId());
    }

    // disable all backpressure types
    adminClient.incrementalAlterConfigs(
            backpressureConfig(ConfluentConfigs.BACKPRESSURE_TYPES_CONFIG, ""), configsOptions).all().get();
    for (KafkaServer broker : physicalCluster.kafkaCluster().brokers()) {
      TestUtils.waitForCondition(
          () -> !broker.quotaManagers().fetch().backpressureEnabled(),
          "Expected consume backpressure to be disabled on broker " + broker.config().brokerId());
      TestUtils.waitForCondition(
          () -> !broker.quotaManagers().produce().backpressureEnabled(),
          "Expected produce backpressure to be disabled on broker " + broker.config().brokerId());
      TestUtils.waitForCondition(
          () -> !broker.quotaManagers().request().backpressureEnabled(),
          "Expected request backpressure to be disabled on broker " + broker.config().brokerId());
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testDynamicDiskThrottlingConfig() throws Exception {
    Properties props = brokerPropsWithTenantQuotas();
    final PhysicalCluster physicalCluster = testHarness.start(props);

    KafkaServer broker = physicalCluster.kafkaCluster().brokers().get(0);
    final ClientQuotaManager quotaManager = broker.quotaManagers().produce();

    final List<File> logDirs = JavaConverters.seqAsJavaList(broker.logManager().liveLogDirs());
    final List<String> fileStores = logDirs.stream().map(File::getAbsolutePath).collect(Collectors.toList());
    final DiskUsageBasedThrottlingConfig defaultConfig = DiskUsageBasedThrottlingConfig$.MODULE$.apply(
            ConfluentConfigs.BACKPRESSURE_DISK_THRESHOLD_BYTES_DEFAULT,
            ConfluentConfigs.BACKPRESSURE_PRODUCE_THROUGHPUT_DEFAULT,
            JavaConverters.asScalaBuffer(fileStores).toSeq(),
            ConfluentConfigs.BACKPRESSURE_DISK_ENABLE_DEFAULT,
            DiskUsageBasedThrottlingConfig$.MODULE$.DefaultDiskCheckFrequencyMs(),
            ConfluentConfigs.BACKPRESSURE_DISK_RECOVERY_FACTOR_DEFAULT
    );
    assertEquals(defaultConfig, quotaManager.getCurrentDiskThrottlingConfig());

    final long diskThreshold = 100 * 1024 * 1024 * 1024L;
    final long throughput = 64 * 1024L;
    final double recoveryFactor = 1.5;

    AdminClient adminClient = physicalCluster.superAdminClient();
    adminClient.incrementalAlterConfigs(
            backpressureConfig(ConfluentConfigs.BACKPRESSURE_DISK_ENABLE_CONFIG, "true"), configsOptions).all().get();

    TestUtils.waitForCondition(
            () -> quotaManager.getCurrentDiskThrottlingConfig().enableDiskBasedThrottling(),
            "Expected " + ConfluentConfigs.BACKPRESSURE_DISK_ENABLE_CONFIG + " to be set as true on " + broker.config().brokerId());

    TestUtils.waitForCondition(
            () -> quotaManager.diskThrottlingEnabledInConfig(quotaManager.getCurrentDiskThrottlingConfig()),
            "Expected diskThrottling() to be enabled on " + broker.config().brokerId());

    adminClient.incrementalAlterConfigs(
            backpressureConfig(ConfluentConfigs.BACKPRESSURE_DISK_THRESHOLD_BYTES_CONFIG, String.valueOf(diskThreshold)), configsOptions).all().get();

    TestUtils.waitForCondition(
            () -> quotaManager.getCurrentDiskThrottlingConfig().freeDiskThresholdBytes() == diskThreshold,
            "Expected " + ConfluentConfigs.BACKPRESSURE_DISK_THRESHOLD_BYTES_CONFIG +
                    " to be set as " + diskThreshold + " on " + broker.config().brokerId());

    adminClient.incrementalAlterConfigs(
            backpressureConfig(ConfluentConfigs.BACKPRESSURE_PRODUCE_THROUGHPUT_CONFIG, String.valueOf(throughput)), configsOptions).all().get();

    TestUtils.waitForCondition(
            () -> quotaManager.getCurrentDiskThrottlingConfig().throttledProduceThroughput() == throughput,
            "Expected {} " + ConfluentConfigs.BACKPRESSURE_PRODUCE_THROUGHPUT_CONFIG +
                    " to be set as " + throughput + " on " + broker.config().brokerId());

    adminClient.incrementalAlterConfigs(
            backpressureConfig(ConfluentConfigs.BACKPRESSURE_DISK_RECOVERY_FACTOR_CONFIG, String.valueOf(recoveryFactor)), configsOptions).all().get();

    TestUtils.waitForCondition(
            () -> quotaManager.getCurrentDiskThrottlingConfig().freeDiskThresholdBytesRecoveryFactor() == recoveryFactor,
            "Expected {} " + ConfluentConfigs.BACKPRESSURE_DISK_RECOVERY_FACTOR_CONFIG +
                    " to be set as " + recoveryFactor + " on " + broker.config().brokerId());
  }

  @Test(expected = ExecutionException.class)
  public void testDynamicEnableRequestBackpressureFailsWithoutMultitenantListener() throws Exception {
    final PhysicalCluster physicalCluster = testHarness.start(brokerPropsWithInvalidMultitenantListenerName());
    AdminClient adminClient = physicalCluster.superAdminClient();
    adminClient.incrementalAlterConfigs(backpressureConfig(ConfluentConfigs.BACKPRESSURE_TYPES_CONFIG, "request"), configsOptions).all().get();
  }

  @Test(expected = ExecutionException.class)
  public void testDynamicEnableBackpressureFailsWithoutTenantQuotasEnabled() throws Exception {
    final PhysicalCluster physicalCluster = testHarness.start(brokerProps());
    AdminClient adminClient = physicalCluster.superAdminClient();
    adminClient.incrementalAlterConfigs(backpressureConfig(ConfluentConfigs.BACKPRESSURE_TYPES_CONFIG, "fetch,produce,request"), configsOptions).all().get();
  }

  private Map<ConfigResource, Collection<AlterConfigOp>> backpressureConfig(String configKey, String configValue) {
    ConfigEntry backpressureCfg = new ConfigEntry(configKey, configValue);
    List<AlterConfigOp> brokerConfigs = Collections.singletonList(
            new AlterConfigOp(backpressureCfg, AlterConfigOp.OpType.SET));
    return Collections.singletonMap(defaultBrokerConfigResource, brokerConfigs);
  }

}
