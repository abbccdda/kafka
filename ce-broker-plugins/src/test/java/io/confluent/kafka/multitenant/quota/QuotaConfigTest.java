// (Copyright) [2019 - 2019] Confluent, Inc.
package io.confluent.kafka.multitenant.quota;

import org.apache.kafka.server.quota.ClientQuotaType;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;


public class QuotaConfigTest {

  private static final QuotaConfig TEST_CONFIG =
      new QuotaConfig(102400L, 204800L, 500.0, QuotaConfig.UNLIMITED_QUOTA);

  @Test
  public void testQuotaConfig() {
    assertTrue(TEST_CONFIG.hasQuotaLimit(ClientQuotaType.PRODUCE));
    assertEquals(102400.0, TEST_CONFIG.quota(ClientQuotaType.PRODUCE), 0.0001);

    assertTrue(TEST_CONFIG.hasQuotaLimit(ClientQuotaType.FETCH));
    assertEquals(204800, TEST_CONFIG.quota(ClientQuotaType.FETCH), 0.0001);

    assertTrue(TEST_CONFIG.hasQuotaLimit(ClientQuotaType.REQUEST));
    assertEquals(500, TEST_CONFIG.quota(ClientQuotaType.REQUEST), 0.0001);
  }

  @Test
  public void testUnlimitedQuotaConfig() {
    assertFalse(QuotaConfig.UNLIMITED_QUOTA.hasQuotaLimit(ClientQuotaType.PRODUCE));
    assertFalse(QuotaConfig.UNLIMITED_QUOTA.hasQuotaLimit(ClientQuotaType.FETCH));
    assertFalse(QuotaConfig.UNLIMITED_QUOTA.hasQuotaLimit(ClientQuotaType.REQUEST));
  }

  @Test
  public void testEqualQuotaPerBrokerNoBrokers() {
    final long quotaNoBrokers = 12345;

    assertEquals("Unexpected produce broker quota", quotaNoBrokers,
                 TEST_CONFIG.equalQuotaPerBrokerOrUnlimited(ClientQuotaType.PRODUCE, 0, quotaNoBrokers).longValue());
    assertEquals("Unexpected consume broker quota", quotaNoBrokers,
                 TEST_CONFIG.equalQuotaPerBrokerOrUnlimited(ClientQuotaType.FETCH, 0, quotaNoBrokers).longValue());
    // we are not using `equalQuotaPerBroker` for request quotas right now, but the method works
    // the same as for bandwidth quotas
    assertEquals("Unexpected request broker quota", quotaNoBrokers,
                 TEST_CONFIG.equalQuotaPerBrokerOrUnlimited(ClientQuotaType.REQUEST, 0, quotaNoBrokers).longValue());
  }

  @Test
  public void testEqualQuotaPerBroker() {
    assertEquals("Unexpected produce broker quota", 10240L,
                 TEST_CONFIG.equalQuotaPerBrokerOrUnlimited(ClientQuotaType.PRODUCE, 10, 12345L).longValue());
    assertEquals("Unexpected consume broker quota", 20480L,
                 TEST_CONFIG.equalQuotaPerBrokerOrUnlimited(ClientQuotaType.FETCH, 10, 12345L).longValue());
    // we are not using `equalQuotaPerBroker` for request quotas right now, but the method works
    // the same as for bandwidth quotas
    assertEquals("Unexpected request broker quota", 50L,
                 TEST_CONFIG.equalQuotaPerBrokerOrUnlimited(ClientQuotaType.REQUEST, 10, 12345L).longValue());
  }

  @Test
  public void testEqualQuotaPerBrokerForUnlimitedQuotasReturnsUnlimitedQuota() {
    assertEquals("Unexpected produce broker quota for unlimited quota",
                 QuotaConfig.UNLIMITED_QUOTA.quota(ClientQuotaType.PRODUCE),
                 QuotaConfig.UNLIMITED_QUOTA.equalQuotaPerBrokerOrUnlimited(ClientQuotaType.PRODUCE, 10, 13L).doubleValue(),
                 0.0001);
    assertEquals("Unexpected consume broker quota for unlimited quota",
                 QuotaConfig.UNLIMITED_QUOTA.quota(ClientQuotaType.FETCH),
                 QuotaConfig.UNLIMITED_QUOTA.equalQuotaPerBrokerOrUnlimited(ClientQuotaType.FETCH, 10, 123L).doubleValue(),
                 0.0001);
    assertEquals("Unexpected request broker quota for unlimited quota",
                 QuotaConfig.UNLIMITED_QUOTA.quota(ClientQuotaType.REQUEST),
                 QuotaConfig.UNLIMITED_QUOTA.equalQuotaPerBrokerOrUnlimited(ClientQuotaType.REQUEST, 10, 11L).doubleValue(),
                 0.0001);
  }
}
