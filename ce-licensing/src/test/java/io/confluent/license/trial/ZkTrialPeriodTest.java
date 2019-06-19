/*
 * Copyright [2016  - 2018] Confluent Inc.
 */

package io.confluent.license.trial;

import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.common.utils.MockTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

public class ZkTrialPeriodTest {

  private EmbeddedZookeeper zookeeper;
  private String zkConnect;

  @Before
  public void setUp() {
    zookeeper = new EmbeddedZookeeper();
    zkConnect = "localhost:" + zookeeper.port();
  }

  @After
  public void tearDown() {
    zookeeper.shutdown();
  }

  @Test
  public void testTimeElapsed() {
    MockTime time = new MockTime(0);
    verifyTrialPeriod("/cp/license", time);
  }

  @Test
  public void testTrialExpired() {
    MockTime time = new MockTime(0);
    ZkTrialPeriod zkTrialPeriod = new ZkTrialPeriod(zkConnect);
    long origTimeRemaining = zkTrialPeriod.startOrVerify(time.milliseconds());
    Assert.assertEquals("Expected a 30 day trial.", origTimeRemaining, TimeUnit.DAYS.toMillis(30));
    time.sleep(TimeUnit.DAYS.toMillis(35));
    long newTimeRemaining = zkTrialPeriod.startOrVerify(time.milliseconds());
    assertEquals("Expected the trial to have expired after 35 days", newTimeRemaining, 0);
  }

  @Test
  public void testMultipleTrials() {
    MockTime time = new MockTime(0);
    verifyTrialPeriod("/cp/componentA", time);
    verifyTrialPeriod("/cp/componentB", time);
  }

  private void verifyTrialPeriod(String path, MockTime time) {
    ZkTrialPeriod zkTrialPeriod = new ZkTrialPeriod(zkConnect, path);
    long origTimeRemaining = zkTrialPeriod.startOrVerify(time.milliseconds());
    time.sleep(10000); // Sleep long enough to ensure that current mock time > actual trial start time
    long newTimeRemaining = zkTrialPeriod.startOrVerify(time.milliseconds());
    assertTrue("Expected some time to elapse on the trial.", newTimeRemaining < origTimeRemaining);
  }
}
