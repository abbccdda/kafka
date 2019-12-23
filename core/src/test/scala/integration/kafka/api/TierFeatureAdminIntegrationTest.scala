/*
 Copyright 2018 Confluent Inc.
 */
package kafka.api

import java.util.concurrent.atomic.AtomicBoolean

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.utils.Exit
import org.apache.kafka.common.utils.Exit.Procedure
import org.junit.{After, Before}
import org.junit.Assert

class TierFeatureAdminIntegrationTest extends PlaintextAdminIntegrationTest {
  val exited = new AtomicBoolean(false)
  override def tierFeature = true
  override def logDirCount = 1
  this.serverConfig.setProperty(KafkaConfig.TierFeatureProp, tierFeature.toString)
  this.serverConfig.setProperty(KafkaConfig.TierEnableProp, tierFeature.toString)
  this.serverConfig.setProperty(KafkaConfig.TierBackendProp, "mock")

  @Before
  override def setUp() {
    Exit.setExitProcedure(new Procedure {
      override def execute(statusCode: Int, message: String): Unit = exited.set(true)
    })
    super.setUp()
  }

  @After
  override def tearDown() {
    super.tearDown()
    Assert.assertFalse(exited.get())
  }

  // CPKAFKA-4033: temporarily disabled due to flaky test design triggered by 2b6fbf7d5d522729486e5d0eb213e09f85759b6d
  override def testReplicaCanFetchFromLogStartOffsetAfterDeleteRecords(): Unit = { }

  // Multiple log dirs are not supported in tiered storage yet
  override def testAlterLogDirsAfterDeleteRecords(): Unit = { }
  override def testAlterReplicaLogDirs(): Unit = { }

  // Altering configurations to enable compaction is not supported
  override def testValidIncrementalAlterConfigs(): Unit = { }

  // unclean leader election is not supported in tiered storage yet
  override def testElectUncleanLeadersForOnePartition(): Unit = { }
  override def testElectUncleanLeadersForManyPartitions(): Unit = { }
  override def testElectUncleanLeadersForAllPartitions(): Unit = { }
  override def testElectUncleanLeadersForUnknownPartitions(): Unit = { }
  override def testElectUncleanLeadersWhenNoLiveBrokers(): Unit = { }
  override def testElectUncleanLeadersNoop(): Unit = { }
  override def testElectUncleanLeadersAndNoop(): Unit = { }

  override def testCreateDeleteTopics(): Unit = {
    super.testCreateDeleteTopics()
    // wait until deletion has finished to test full controller tier topic deletion path
    TestUtils.waitUntilTrue(() => zkClient.getTopicDeletions.isEmpty, "timed out waiting for topic deletions to complete")
  }
}
