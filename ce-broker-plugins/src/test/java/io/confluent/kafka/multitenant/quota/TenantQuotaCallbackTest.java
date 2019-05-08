// (Copyright) [2018 - 2018] Confluent, Inc.
package io.confluent.kafka.multitenant.quota;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.TenantMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.quota.ClientQuotaType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;


public class TenantQuotaCallbackTest {

  private static final double EPS = 0.0001;
  private static final Long MIN_BROKER_CONSUME_QUOTA = 20L;
  private static final Long MIN_BROKER_PRODUCE_QUOTA = 10L;
  private static final Long MAX_BROKER_CONSUME_QUOTA = 1200L;
  private static final Long MAX_BROKER_PRODUCE_QUOTA = 600L;

  private final int brokerId = 1;
  private TestCluster testCluster;
  private TenantQuotaCallback quotaCallback;

  @Before
  public void setUp() {
    TenantQuotaCallback.closeAll();
    quotaCallback = new TenantQuotaCallback();
    Map<String, Object> configs = quotaCallbackProps();
    quotaCallback.configure(configs);
    Map<String, QuotaConfig> tenantQuotas = new HashMap<>();
    tenantQuotas.put("tenant1", quotaConfig(1000, 2000, 300));
    tenantQuotas.put("tenant2", quotaConfig(2000, 3000, 400));
    TenantQuotaCallback.updateQuotas(tenantQuotas, QuotaConfig.UNLIMITED_QUOTA);
  }

  @After
  public void tearDown() {
    quotaCallback.close();
  }

  @Test
  public void testTenantQuotaConstructor() {
    TenantQuotaCallback.TenantQuota unlimitedQuota =
        new TenantQuotaCallback().new TenantQuota(QuotaConfig.UNLIMITED_QUOTA);
    assertFalse(unlimitedQuota.hasQuotaLimit(ClientQuotaType.PRODUCE));
    assertFalse(unlimitedQuota.hasQuotaLimit(ClientQuotaType.FETCH));
    assertFalse(unlimitedQuota.hasQuotaLimit(ClientQuotaType.REQUEST));

    QuotaConfig defaultQuota = quotaConfig(102400L, 102400L, 50.0);
    assertTrue(defaultQuota.hasQuotaLimit(ClientQuotaType.PRODUCE));
    assertEquals("Unexpected PRODUCE quota",
                 102400.0, defaultQuota.quota(ClientQuotaType.PRODUCE), EPS);

    assertTrue(defaultQuota.hasQuotaLimit(ClientQuotaType.FETCH));
    assertEquals("Unexpected FETCH quota",
                 102400.0, defaultQuota.quota(ClientQuotaType.FETCH), EPS);

    assertTrue(defaultQuota.hasQuotaLimit(ClientQuotaType.REQUEST));
    assertEquals("Unexpected REQUEST quota",
                 50.0, defaultQuota.quota(ClientQuotaType.REQUEST), EPS);
  }

  @Test
  public void testGetOrCreateTenantQuota() {
    TenantQuotaCallback newQuotaCallback = new TenantQuotaCallback();
    Map<String, Object> configs = new HashMap<>();
    configs.put("broker.id", String.valueOf(brokerId));
    newQuotaCallback.configure(configs);

    TenantQuotaCallback.TenantQuota tenantQuota = newQuotaCallback.getOrCreateTenantQuota(
        "tenantA", quotaConfig(204800L, 204800L, 500.0), false);
    // there are no partition leaders, so expecting minimum produce & consume quotas
    assertTrue(tenantQuota.hasQuotaLimit(ClientQuotaType.PRODUCE));
    assertEquals("Unexpected PRODUCE quota",
                 TenantQuotaCallback.DEFAULT_MIN_BROKER_TENANT_PRODUCER_BYTE_RATE,
                 tenantQuota.quotaLimit(ClientQuotaType.PRODUCE), EPS);

    assertTrue(tenantQuota.hasQuotaLimit(ClientQuotaType.FETCH));
    assertEquals("Unexpected FETCH quota",
                 TenantQuotaCallback.DEFAULT_MIN_BROKER_TENANT_CONSUMER_BYTE_RATE,
                 tenantQuota.quotaLimit(ClientQuotaType.FETCH), EPS);

    assertTrue(tenantQuota.hasQuotaLimit(ClientQuotaType.REQUEST));
    assertEquals("Unexpected REQUEST quota",
                 500.0, tenantQuota.quotaLimit(ClientQuotaType.REQUEST), EPS);

    // this will update quotas based on new partition assignment
    createCluster(5);
    testCluster.setPartitionLeaders("tenantA_topic1", 0, 1, brokerId);
    newQuotaCallback.updateClusterMetadata(testCluster.cluster());
    // without 'forceUpdate' quota is not updated due to new quota config, but update due to new
    // partition assignment already took place when we updated cluster metadata
    TenantQuotaCallback.TenantQuota tenantQuota2 = newQuotaCallback.getOrCreateTenantQuota(
        "tenantA", quotaConfig(404800L, 404800L, 600.0), false);
    assertEquals("Unexpected PRODUCE quota",
                 204800.0, tenantQuota2.quotaLimit(ClientQuotaType.PRODUCE), EPS);
    assertEquals("Unexpected FETCH quota",
                 204800.0, tenantQuota2.quotaLimit(ClientQuotaType.FETCH), EPS);
    assertEquals("Unexpected REQUEST quota",
                 500.0, tenantQuota2.quotaLimit(ClientQuotaType.REQUEST), EPS);

    // quota is updated with 'forceUpdate' based on new quota config
    TenantQuotaCallback.TenantQuota updatedQuota = newQuotaCallback.getOrCreateTenantQuota(
        "tenantA", quotaConfig(404800L, 404800L, 600.0), true);
    assertEquals("Unexpected PRODUCE quota",
                 404800, updatedQuota.quotaLimit(ClientQuotaType.PRODUCE), EPS);
    assertEquals("Unexpected FETCH quota",
                 404800, updatedQuota.quotaLimit(ClientQuotaType.FETCH), EPS);
    assertEquals("Unexpected REQUEST quota",
                 600.0, updatedQuota.quotaLimit(ClientQuotaType.REQUEST), EPS);
  }

  @Test
  public void testTenantQuota() {
    createCluster(5);
    MultiTenantPrincipal principal = new MultiTenantPrincipal("userA",
            new TenantMetadata("tenant1", "tenant1_cluster_id", false));

    // Partitions divided between two brokers
    setPartitionLeaders("tenant1_topic1", 0, 5, brokerId);
    setPartitionLeaders("tenant1_topic2", 0, 5, brokerId + 1);
    verifyQuotas(principal, 500, 1000, 300);

    // Other tenant's partitions shouldn't impact quota
    setPartitionLeaders("tenant2_topic1", 0, 5, brokerId);
    setPartitionLeaders("tenant2_topic2", 0, 5, brokerId + 1);
    verifyQuotas(principal, 500, 1000, 300);

    // Delete topic
    deleteTopic("tenant1_topic2");
    verifyQuotas(principal, MAX_BROKER_PRODUCE_QUOTA, MAX_BROKER_CONSUME_QUOTA, 300);

    // Add another topic
    setPartitionLeaders("tenant1_topic3", 0, 5, brokerId + 2);
    verifyQuotas(principal, 500, 1000, 300);

    // Moving one partition leader from one broker to another (both still have leaders) should
    // not change quota distribution
    setPartitionLeaders("tenant1_topic3", 1, 1, brokerId);
    verifyQuotas(principal, 500, 1000, 300);
  }

  @Test
  public void testTenantEqualQuotaDistribution() {
    createCluster(5);
    MultiTenantPrincipal principal = new MultiTenantPrincipal(
        "userA", new TenantMetadata("tenant1", "tenant1_cluster_id", false));
    MultiTenantPrincipal principal2 = new MultiTenantPrincipal(
        "userB", new TenantMetadata("tenant2", "tenant2_cluster_id", false));

    // One partition on one broker and 1000 partitions on another broker should result in equal
    // quota distribution between the two brokers
    setPartitionLeaders("tenant1_topic1", 0, 1, brokerId);
    setPartitionLeaders("tenant1_topic2", 0, 1000, brokerId + 1);
    verifyQuotas(principal, 500, 1000, 300);

    // Other tenant's partitions shouldn't impact quota
    setPartitionLeaders("tenant2_topic1", 0, 1000, brokerId);
    setPartitionLeaders("tenant2_topic2", 0, 1, brokerId + 1);
    setPartitionLeaders("tenant2_topic3", 0, 1, brokerId + 2);
    setPartitionLeaders("tenant2_topic4", 0, 1, brokerId + 3);
    verifyQuotas(principal, 500, 1000, 300);
    verifyQuotas(principal2, 500, 750, 400);

    // Add partitions to remaining 3 brokers for tenant 1
    setPartitionLeaders("tenant1_topic3", 0, 10, brokerId + 2);
    setPartitionLeaders("tenant1_topic4", 0, 100, brokerId + 3);
    setPartitionLeaders("tenant1_topic5", 0, 500, brokerId + 4);
    // all 5 brokers have tenant leaders
    verifyQuotas(principal, 200, 400, 300);

    // Delete topic
    deleteTopic("tenant1_topic2");
    verifyQuotas(principal, 250, 500, 300);
  }

  @Test
  public void testTenantQuotaUpdateSetsCorrectUpdateFlags() {
    final int numBrokers = 10;
    final String tenantName = "tenant1";
    final String topic = tenantName + "_topic1";

    Map<String, QuotaConfig> tenantQuotas = new HashMap<>();
    tenantQuotas.put(tenantName, quotaConfig(3000, 2000, 300));
    TenantQuotaCallback.updateQuotas(tenantQuotas, QuotaConfig.UNLIMITED_QUOTA);
    // this changed quotas from unlimited to min quota per broker (no partitions yet)
    assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE));
    assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.FETCH));
    assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.REQUEST));

    // if nothing changed, second call to quotaResetRequired returns false
    assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE));
    assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.FETCH));
    assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.REQUEST));

    // setup cluster and topic partitions
    createCluster(numBrokers);
    MultiTenantPrincipal principal = new MultiTenantPrincipal(
        "userA", new TenantMetadata(tenantName, "tenant1_cluster_id", false));

    // each broker has one tenant partition leader, so that quotas distributed equally among brokers
    for (int i = 1; i <= numBrokers; i++) {
      setPartitionLeaders(topic, i - 1, 1, i);
    }
    // quota change comes from metadata update, so quota reset flags should be false
    verifyQuota(ClientQuotaType.PRODUCE, principal, 300.0, tenantName, false);
    verifyQuota(ClientQuotaType.FETCH, principal, 200.0, tenantName, false);
    verifyQuota(ClientQuotaType.REQUEST, principal, 300.0, tenantName, false);

    // update all tenant quotas -- this should set quota reset flags to true for all quota types
    tenantQuotas.put(tenantName, quotaConfig(2000, 4000, 500));
    TenantQuotaCallback.updateQuotas(tenantQuotas, QuotaConfig.UNLIMITED_QUOTA);
    verifyQuota(ClientQuotaType.PRODUCE, principal, 200.0, tenantName, true);
    verifyQuota(ClientQuotaType.FETCH, principal, 400.0, tenantName, true);
    verifyQuota(ClientQuotaType.REQUEST, principal, 500.0, tenantName, true);

    // verify update of produce quota only
    tenantQuotas.put(tenantName, quotaConfig(1000, 4000, 500));
    TenantQuotaCallback.updateQuotas(tenantQuotas, QuotaConfig.UNLIMITED_QUOTA);
    verifyQuota(ClientQuotaType.PRODUCE, principal, 100.0, tenantName, true);
    verifyQuota(ClientQuotaType.FETCH, principal, 400.0, tenantName, false);
    verifyQuota(ClientQuotaType.REQUEST, principal, 500.0, tenantName, false);

    // verify update of consume quota only
    tenantQuotas.put(tenantName, quotaConfig(1000, 2000, 500));
    TenantQuotaCallback.updateQuotas(tenantQuotas, QuotaConfig.UNLIMITED_QUOTA);
    verifyQuota(ClientQuotaType.PRODUCE, principal, 100.0, tenantName, false);
    verifyQuota(ClientQuotaType.FETCH, principal, 200.0, tenantName, true);
    verifyQuota(ClientQuotaType.REQUEST, principal, 500.0, tenantName, false);

    // verify update of request quota only
    tenantQuotas.put(tenantName, quotaConfig(1000, 2000, 200));
    TenantQuotaCallback.updateQuotas(tenantQuotas, QuotaConfig.UNLIMITED_QUOTA);
    verifyQuota(ClientQuotaType.PRODUCE, principal, 100.0, tenantName, false);
    verifyQuota(ClientQuotaType.FETCH, principal, 200.0, tenantName, false);
    verifyQuota(ClientQuotaType.REQUEST, principal, 200.0, tenantName, true);

    // add one more tenant with one partition on this broker
    MultiTenantPrincipal principal2 = new MultiTenantPrincipal(
        "userB", new TenantMetadata("tenant2", "tenant2_cluster_id", false));
    tenantQuotas.put("tenant2", quotaConfig(500, 500, 500));
    TenantQuotaCallback.updateQuotas(tenantQuotas, QuotaConfig.UNLIMITED_QUOTA);
    // this changed quotas from unlimited to min quota per broker (no partitions yet)
    assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE));
    assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.FETCH));
    assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.REQUEST));

    // one partition on one broker
    setPartitionLeaders("tenant2_topic1", 0, 1, brokerId);
    verifyQuotas(principal2, 500, 500, 500);

    // metadata update should not change flags
    assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE));
    assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.FETCH));
    assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.REQUEST));

    // updating one quota of one tenant sets the flag for all tenants for this quota type, but
    // ClientQuotaManager will filter only ones that actually changed
    tenantQuotas.put("tenant2", quotaConfig(400, 500, 500));
    TenantQuotaCallback.updateQuotas(tenantQuotas, QuotaConfig.UNLIMITED_QUOTA);
    verifyQuotas(principal2, 400, 500, 500);
    assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE));
    assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.FETCH));
    assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.REQUEST));
  }

  @Test
  public void testSmallNumberOfPartitions() throws Exception {
    createCluster(5);
    MultiTenantPrincipal principal = new MultiTenantPrincipal("userA",
            new TenantMetadata("tenant1", "tenant1_cluster_id", false));

    // One partition on one broker
    setPartitionLeaders("tenant1_topic1", 0, 1, brokerId);
    verifyQuotas(principal, MAX_BROKER_PRODUCE_QUOTA, MAX_BROKER_CONSUME_QUOTA, 300);

    // Two partitions on one broker
    setPartitionLeaders("tenant1_topic2", 0, 1, brokerId);
    verifyQuotas(principal, MAX_BROKER_PRODUCE_QUOTA, MAX_BROKER_CONSUME_QUOTA, 300);

    // Add two more partitions on another broker
    setPartitionLeaders("tenant1_topic3", 0, 2, brokerId + 1);
    verifyQuotas(principal, 500, 1000, 300);

    // Add six more partitions on another broker
    setPartitionLeaders("tenant1_topic4", 0, 6, brokerId + 2);
    verifyQuotas(principal, 333, 666, 300);
  }

  /**
   * If there are no partitions for a tenant, we assign the minimum tenant quota to each broker.
   * This ensures that if a request was received before metadata was refreshed on the quota
   * callback, the client is not over-throttled. We set the minimum quota because the window
   * where requests can be processed before quota is updated is due to request handling on
   * different threads, so this window is very small.
   */
  @Test
  public void testNoPartitions() throws Exception {
    createCluster(5);
    MultiTenantPrincipal principal = new MultiTenantPrincipal("userA",
            new TenantMetadata("tenant1", "tenant1_cluster_id", false));

    verifyQuotas(principal, MIN_BROKER_PRODUCE_QUOTA, MIN_BROKER_CONSUME_QUOTA, 300);

    setPartitionLeaders("tenant1_topic1", 0, 2, brokerId);
    verifyQuotas(principal, MAX_BROKER_PRODUCE_QUOTA, MAX_BROKER_CONSUME_QUOTA, 300);

    // Delete all partitions
    createCluster(5);
    verifyQuotas(principal, MIN_BROKER_PRODUCE_QUOTA, MIN_BROKER_CONSUME_QUOTA, 300);
  }

  /**
   * If no cluster metadata is available (there could be a small timing window where
   * client requests are processed before metadata is available), minimum tenant quota
   * is allocated to broker to avoid unnecessary throttling.
   */
  @Test
  public void testNoClusterMetadata() {
    MultiTenantPrincipal principal = new MultiTenantPrincipal("userA",
        new TenantMetadata("tenant1", "tenant1_cluster_id", false));
    verifyQuotas(principal, MIN_BROKER_PRODUCE_QUOTA, MIN_BROKER_CONSUME_QUOTA, 300);
  }

  /**
   * Tenants may be created with unlimited quota (e.g. if we dont want to enable request quotas)
   */
  @Test
  public void testUnlimitedTenantQuota() {
    createCluster(5);
    Map<String, QuotaConfig> tenantQuotas = new HashMap<>();
    tenantQuotas.put("tenant1", quotaConfig(Long.MAX_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE));
    TenantQuotaCallback.updateQuotas(tenantQuotas, QuotaConfig.UNLIMITED_QUOTA);
    MultiTenantPrincipal principal = new MultiTenantPrincipal("userA",
        new TenantMetadata("tenant1", "tenant1_cluster_id", false));
    verifyUnlimitedQuotas(principal);

    setPartitionLeaders("tenant1_topic1", 0, 5, brokerId);
    for (ClientQuotaType quotaType : ClientQuotaType.values()) {
      verifyQuota(quotaType, principal, QuotaConfig.UNLIMITED_QUOTA.quota(quotaType), null);
    }
    setPartitionLeaders("tenant1_topic2", 0, 5, brokerId + 1);
    verifyUnlimitedQuotas(principal);

    tenantQuotas.put("tenant1", quotaConfig(1000, 2000, Integer.MAX_VALUE));
    TenantQuotaCallback.updateQuotas(tenantQuotas, QuotaConfig.UNLIMITED_QUOTA);
    verifyQuota(ClientQuotaType.PRODUCE, principal, 500, "tenant1");
    verifyQuota(ClientQuotaType.FETCH, principal, 1000, "tenant1");
    verifyQuota(ClientQuotaType.REQUEST, principal, Integer.MAX_VALUE, null);
  }

  /**
   * Tenant quotas are refreshed periodically with a default 30 second interval.
   * We can apply a configurable default quota for tenants whose quota is not known
   * to avoid tenants overloading the broker during this period.
   */
  @Test
  public void testDefaultTenantQuota() {
    // By default, we don't apply quotas until tenant is known (i.e unlimited quota by default)
    createCluster(5);
    MultiTenantPrincipal principal = new MultiTenantPrincipal("userA",
            new TenantMetadata("tenant100", "tenant100_cluster_id", false));

    setPartitionLeaders("tenant100_topic1", 0, 1, brokerId);
    setPartitionLeaders("tenant100_topic2", 0, 10, brokerId + 1);
    setPartitionLeaders("tenant100_topic3", 0, 1, brokerId + 2);
    setPartitionLeaders("tenant100_topic4", 0, 100, brokerId + 3);
    setPartitionLeaders("tenant100_topic5", 0, 5, brokerId + 4);

    verifyUnlimitedQuotas(principal);

    // We can apply configurable defaults using custom broker configs
    QuotaConfig defaultQuota = quotaConfig(1000L, 2000L, 10.0);
    TenantQuotaCallback.updateQuotas(Collections.emptyMap(), defaultQuota);
    MultiTenantPrincipal principal2 = new MultiTenantPrincipal("userA",
        new TenantMetadata("tenant101", "tenant101_cluster_id", false));
    setPartitionLeaders("tenant101_topic2", 0, 1, brokerId + 1);
    setPartitionLeaders("tenant101_topic3", 0, 1, brokerId + 2);
    setPartitionLeaders("tenant101_topic4", 0, 1, brokerId + 3);
    setPartitionLeaders("tenant101_topic5", 0, 1, brokerId + 4);

    // use-case where this broker does not have any partitions
    verifyQuotas(principal2, MIN_BROKER_PRODUCE_QUOTA, MIN_BROKER_CONSUME_QUOTA, 10.0);

    setPartitionLeaders("tenant101_topic1", 0, 1, brokerId);
    verifyQuotas(principal2, 200, 400, 10.0);

    // Default quota updates should also apply to tenants currently using the default
    verifyQuotas(principal, 200, 400, 10.0);

    // Change default quota and verify again
    QuotaConfig newDefaultQuota = quotaConfig(3000L, 6000L, 30.0);
    TenantQuotaCallback.updateQuotas(Collections.emptyMap(), newDefaultQuota);
    quotaCallback.updateClusterMetadata(testCluster.cluster());
    verifyQuotas(principal2, 600, 1200, 30.0);

    verifyQuotas(principal, 600, 1200, 30.0);
  }

  /**
   * Quotas are not applied to non-tenant principals like broker principal.
   */
  @Test
  public void testNonTenantPrincipal() {
    createCluster(5);
    KafkaPrincipal principal = KafkaPrincipal.ANONYMOUS;
    for (ClientQuotaType quotaType : ClientQuotaType.values()) {
      verifyQuota(quotaType, principal, QuotaConfig.UNLIMITED_QUOTA.quota(quotaType), null);
    }
  }

  @Test
  public void testUnavailableBrokers() {
    createCluster(2);
    MultiTenantPrincipal principal = new MultiTenantPrincipal("userA",
        new TenantMetadata("tenant1", "tenant1_cluster_id", false));

    // Replicas may refer to nodes that are not available. Quotas should
    // still be calculated correctly.
    setPartitionLeaders("tenant1_topic1", 0, 1, brokerId);
    setPartitionLeaders("tenant1_topic2", 0, 1, 30);
    assertNull("Unavailable node created", testCluster.cluster().nodeById(30));
    assertNull("Unavailable node created",
        testCluster.cluster().partitionsForTopic("tenant1_topic2").get(0).leader());
    quotaCallback.updateClusterMetadata(testCluster.cluster());
    verifyQuotas(principal, MAX_BROKER_PRODUCE_QUOTA, MAX_BROKER_CONSUME_QUOTA, 300);
  }

  private Map<String, Object> quotaCallbackProps() {
    Map<String, Object> configs = new HashMap<>();
    configs.put("broker.id", String.valueOf(brokerId));
    configs.put(TenantQuotaCallback.MIN_BROKER_TENANT_PRODUCER_BYTE_RATE_CONFIG,
                MIN_BROKER_PRODUCE_QUOTA.toString());
    configs.put(TenantQuotaCallback.MAX_BROKER_TENANT_PRODUCER_BYTE_RATE_CONFIG,
                MAX_BROKER_PRODUCE_QUOTA.toString());
    configs.put(TenantQuotaCallback.MIN_BROKER_TENANT_CONSUMER_BYTE_RATE_CONFIG,
                MIN_BROKER_CONSUME_QUOTA.toString());
    configs.put(TenantQuotaCallback.MAX_BROKER_TENANT_CONSUMER_BYTE_RATE_CONFIG,
                MAX_BROKER_CONSUME_QUOTA.toString());
    return configs;
  }

  private void createCluster(int numNodes) {
    testCluster = new TestCluster();
    for (int i = 1; i <= numNodes; i++) {
      testCluster.addNode(i, "rack0");
    }
    Cluster cluster = testCluster.cluster();
    quotaCallback.updateClusterMetadata(cluster);
    assertEquals(cluster, quotaCallback.cluster());
  }

  private void verifyQuotas(MultiTenantPrincipal principal, double produceQuota, double fetchQuota, double requestQuota) {
    String tenant = principal.tenantMetadata().tenantName;
    verifyQuota(ClientQuotaType.PRODUCE, principal, produceQuota, tenant);
    verifyQuota(ClientQuotaType.FETCH, principal, fetchQuota, tenant);
    verifyQuota(ClientQuotaType.REQUEST, principal, requestQuota, tenant);
  }

  private void verifyQuota(ClientQuotaType type, KafkaPrincipal principal,
                           double expectedValue, String expectedTenantTag) {
    verifyQuota(type, principal, expectedValue, expectedTenantTag, null);
  }

  private void verifyQuota(ClientQuotaType type, KafkaPrincipal principal,
                           double expectedValue, String expectedTenantTag,
                           Boolean expectQuotaResetRequired) {
    Map<String, String> metricTags = quotaCallback.quotaMetricTags(type, principal, "some-client");
    if (expectedTenantTag != null) {
      assertEquals(Collections.singletonMap("tenant", expectedTenantTag), metricTags);
    } else {
      assertTrue("Unexpected tags " + metricTags, metricTags.isEmpty());
    }
    Double quotaLimit = quotaCallback.quotaLimit(type, metricTags);
    assertEquals("Unexpected quota of type " + type, expectedValue, quotaLimit, EPS);
    if (expectQuotaResetRequired != null) {
      assertEquals(expectQuotaResetRequired, quotaCallback.quotaResetRequired(type));
    }
  }

  private void verifyUnlimitedQuotas(KafkaPrincipal principal) {
    for (ClientQuotaType quotaType : ClientQuotaType.values()) {
      verifyQuota(quotaType, principal, QuotaConfig.UNLIMITED_QUOTA.quota(quotaType), null);
    }
  }

  private void setPartitionLeaders(String topic, int firstPartition, int count,
                                   Integer leaderBrokerId) {
    testCluster.setPartitionLeaders(topic, firstPartition, count, leaderBrokerId);
    quotaCallback.updateClusterMetadata(testCluster.cluster());
  }

  private void deleteTopic(String topic) {
    testCluster.deleteTopic(topic);
    quotaCallback.updateClusterMetadata(testCluster.cluster());
  }

  private QuotaConfig quotaConfig(long producerByteRate, long consumerByteRate, double requestPercentage) {
    return new QuotaConfig(producerByteRate, consumerByteRate, requestPercentage, QuotaConfig.UNLIMITED_QUOTA);
  }
}
