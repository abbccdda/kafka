/*
 Copyright 2018 Confluent Inc.
 */
package kafka.api

import kafka.server.KafkaConfig

class TierFeatureAdminClientIntegrationTest extends AdminClientIntegrationTest {
  override val tierFeature = true
  override val logDirCount = 1
  this.serverConfig.setProperty(KafkaConfig.TierFeatureProp, tierFeature.toString)
  this.serverConfig.setProperty(KafkaConfig.TierEnableProp, tierFeature.toString)
  this.serverConfig.setProperty(KafkaConfig.TierBackendProp, "mock")

  /*
   Multiple log dirs are not supported in tiered storage yet
   */
  override  def testAlterLogDirsAfterDeleteRecords(): Unit = { }
  override  def testAlterReplicaLogDirs(): Unit = { }
}
