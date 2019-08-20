/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tier

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.api.IntegrationTestHarness
import kafka.log.AbstractLog
import kafka.server.epoch.LeaderEpochFileCache
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfluentTopicConfig, TopicConfig}
import org.apache.kafka.common.utils.Exit
import org.apache.kafka.common.utils.Exit.Procedure
import org.junit.Assert.{assertEquals, assertFalse}
import org.junit.{After, Before, Test}

// TODO: we can remove this test once we have significant system tests.
class TierEpochStateRevolvingReplicationTest extends IntegrationTestHarness with Logging {
  override protected val brokerCount: Int = 3

  val topic = "topic1"
  val msg = new Array[Byte](1000)
  val exited = new AtomicBoolean(false)

  serverConfig.put(KafkaConfig.TierEnableProp, "false")
  serverConfig.put(KafkaConfig.TierFeatureProp, "true")
  serverConfig.put(KafkaConfig.TierBackendProp, "mock")
  serverConfig.put(KafkaConfig.TierS3BucketProp, "mybucket")
  serverConfig.put(KafkaConfig.TierPartitionStateCommitIntervalProp, "5")
  serverConfig.put(KafkaConfig.TierMetadataNumPartitionsProp, "1")
  serverConfig.put(KafkaConfig.LogCleanupIntervalMsProp, "5")

  @Before
  override def setUp() {
    Exit.setExitProcedure(new Procedure {
      override def execute(statusCode: Int, message: String): Unit = exited.set(true)
    })
    super.setUp()
  }

  @After
  override def tearDown() {
    assertFalse(exited.get())
    super.tearDown()
  }

  @Test
  def testTierStateRestoreToReplication(): Unit = {
    val properties = new Properties()
    properties.put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "true")
    properties.put(TopicConfig.SEGMENT_BYTES_CONFIG, "2000")
    properties.put(TopicConfig.RETENTION_BYTES_CONFIG, "-1")
    properties.put(ConfluentTopicConfig.TIER_LOCAL_HOTSET_BYTES_CONFIG, "0")
    properties.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")

    // create partition with 3 replicas
    val tp = new TopicPartition(topic, 0)
    val leaderId = createTopic(topic, numPartitions = 1, replicationFactor = 3, topicConfig = properties).head._2
    val leader = servers.find(_.config.brokerId == leaderId).get
    awaitISR(tp, 3, leader)

    val producer = createProducer()

    // since we use RF = 3 on the tier topic,
    // we must make sure the tier topic is up before we start stopping brokers
    servers.foreach(b => TierTestUtils.awaitTierTopicPartition(b, 0))

    for (_ <- 0 until 3) {
      val followerToShutdown = servers.map(_.config.brokerId).filter(_ != leaderId).head

      // stop the follower before writing anything
      killBroker(followerToShutdown)
      awaitISR(tp, 2, leader)

      producer.send(new ProducerRecord(tp.topic, tp.partition, null, msg)).get

      val logEndPriorToProduce = leader.replicaManager.logManager.getLog(tp).get.localLogEndOffset

      // a follower is now shutdown, write a bunch of messages
      for (_ <- 0 until 4)
        producer.send(new ProducerRecord(tp.topic, tp.partition, null, msg)).get

      TestUtils.waitUntilTrue(() => {
        // wait for log start offset to be greater than the end we saw
        val leaderLog = leader.replicaManager.logManager.getLog(tp)
        leaderLog.get.localLogStartOffset > logEndPriorToProduce
      }, "timed out waiting for segment tiering and deletion",
        60000)

      restartDeadBrokers()
      awaitISR(tp, 3, leader)

      // send one more message so that final epochs match, since leader epoch won't be attached
      // to epoch state in follower unless there is an additional message
      producer.send(new ProducerRecord(topic, 0, null, msg)).get

      for (broker <- servers)
        waitForLogEndOffsetToMatch(leader, broker, 0)

      // check all the broker epoch entries match
      for (broker <- servers)
        assertEquals(epochCache(broker).epochEntries, epochCache(leader).epochEntries)
    }
  }

  private def waitForLogEndOffsetToMatch(b1: KafkaServer, b2: KafkaServer, partition: Int): Unit = {
    TestUtils.waitUntilTrue(() => {
      getLog(b1, partition).logEndOffset == getLog(b2, partition).logEndOffset
    }, s"Logs didn't match ${getLog(b1, partition).logEndOffset} vs ${getLog(b2, partition).logEndOffset}. ${b1.config.brokerId} v ${b2.config.brokerId}",
      60000)
  }

  private def getLog(broker: KafkaServer, partition: Int): AbstractLog = {
    broker.logManager.getLog(new TopicPartition(topic, partition)).orNull
  }

  private def epochCache(broker: KafkaServer): LeaderEpochFileCache = {
    val log = getLog(broker, 0)
    log.leaderEpochCache.get
  }

  private def awaitISR(tp: TopicPartition, numReplicas: Int, leader: KafkaServer): Unit = {
    TestUtils.waitUntilTrue(() => {
      leader.replicaManager.nonOfflinePartition(tp).get.inSyncReplicaIds.size == numReplicas
    }, "Timed out waiting for replicas to join ISR")
  }
}
