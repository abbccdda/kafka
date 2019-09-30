// (Copyright) [2019 - 2019] Confluent, Inc.
package io.confluent.kafka.multitenant.integration.test;

import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.test.IntegrationTest;

import java.util.Collections;
import java.util.Properties;

import io.confluent.kafka.multitenant.integration.cluster.PhysicalCluster;
import io.confluent.kafka.multitenant.quota.TenantQuotaCallback;
import io.confluent.kafka.server.plugins.policy.AlterConfigPolicy;
import io.confluent.kafka.server.plugins.policy.TopicPolicyConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.server.ThreadUsageMetrics;

import scala.collection.JavaConversions;

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
    props.put(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG, "1");
    props.put(TopicPolicyConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1");
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
                 ThreadUsageMetrics.networkThreadsCapacity(broker.metrics(), JavaConversions.asScalaBuffer(Collections.singletonList("EXTERNAL"))), 1.0);
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
                                                           JavaConversions.asScalaBuffer(Collections.singletonList("EXTERNAL"))),
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

    assertEquals(numIoThreads * 100.0, ThreadUsageMetrics.ioThreadsCapacity(broker.metrics()), 1.0);
    assertEquals(numNetworkThreads * 100.0,
                 ThreadUsageMetrics.networkThreadsCapacity(broker.metrics(), JavaConversions.asScalaBuffer(Collections.singletonList("EXTERNAL"))), 1.0);
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

}
