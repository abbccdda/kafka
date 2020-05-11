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

import kafka.log.AbstractLog
import kafka.server.KafkaConfig._
import kafka.server.epoch.LeaderEpochFileCache
import kafka.server.{KafkaServer, KafkaConfig}
import kafka.utils.{TestUtils, Logging}
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{TopicConfig, ConfluentTopicConfig}
import org.apache.kafka.common.utils.Exit
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.{Before, After, Test}

import scala.collection.Seq

class TierEpochStateReplicationTest extends ZooKeeperTestHarness with Logging {
  val topic = "topic1"
  val msg = new Array[Byte](1000)
  val msgBigger = new Array[Byte](10000)
  var brokers: Seq[KafkaServer] = null
  var producer: KafkaProducer[Array[Byte], Array[Byte]] = null
  val exited = new AtomicBoolean(false)

  @Before
  override def setUp(): Unit = {
    Exit.setExitProcedure((_, _) => exited.set(true))
    super.setUp()
  }

  @After
  override def tearDown(): Unit = {
    producer.close()
    TestUtils.shutdownServers(brokers)
    super.tearDown()
    assertFalse(exited.get())
  }

  @Test
  def testTierStateRestoreOnLaggingReplica(): Unit = {
    brokers = (100 to 101).map(createBroker(_))

    val properties = new Properties()
    properties.put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "true")
    properties.put(TopicConfig.SEGMENT_BYTES_CONFIG, "10000")
    properties.put(TopicConfig.RETENTION_BYTES_CONFIG, "-1")

    // a single partition topic with 2 replicas
    val tp = new TopicPartition(topic, 0)
    TestUtils.createTopic(zkClient, topic, Map(0 -> Seq(100, 101)), brokers, properties)
    val leader = this.leader
    val follower = this.follower

    // write a message to the partition
    producer = createProducer
    producer.send(new ProducerRecord(topic, 0, null, msg)).get
    val epochBeforeLeaderBounce = epochCache(leader).latestEpoch.get

    // bounce the leader and wait until it is re-elected as the leader
    bounce(leader)
    awaitISR(tp, numReplicas = 2)
    TestUtils.waitUntilTrue(() => this.leader == leader, "Timed out waiting for preferred leader to be elected")
    TestUtils.waitUntilTrue(() => leader.tierTopicManagerOpt.get.isReady, "Timed out waiting for tier topic manager to be ready")

    // Write a message to the partition. This write will be at a higher epoch than the previous message.
    producer.send(new ProducerRecord(topic, 0, null, msg)).get
    val epochAfterLeaderBounce = epochCache(leader).latestEpoch.get
    assertTrue(epochAfterLeaderBounce > epochBeforeLeaderBounce)
    assertEquals(epochCache(leader).epochEntries, epochCache(follower).epochEntries)

    // Ensure that tier topic is created and tier topic manager is ready before stopping the follower
    brokers.foreach { broker =>
      TestUtils.waitUntilTrue(() => broker.tierTopicManagerOpt.get.isReady,
        "Timed out waiting for tier topic manager to be ready")
    }

    // stop the follower
    stop(follower)
    awaitISR(tp, numReplicas = 1)

    // follower is now shutdown, write a bunch of messages
    for (_ <- 0 until 999)
      producer.send(new ProducerRecord(topic, 0, null, msg)).get

    // wait until hotset retention deletes the first local segment
    TestUtils.waitUntilTrue(() => {
      val leaderLog = leader.replicaManager.logManager.getLog(tp).get
      leaderLog.localLogStartOffset > 0
    }, "timed out waiting for segment tiering and hotset retention",
      60000)

    // messages appended now must have an epoch higher than the message appended before follower shutdown
    val epochAfterFollowerShutdown = epochCache(leader).latestEpoch.get
    assertTrue(epochAfterFollowerShutdown > epochAfterLeaderBounce)

    // Startup the follower. The follower will receive an OFFSET_TIERED exception when it begins replication. This
    // will initiate catchup for tier partition state and will restore the leader epoch cache from tiered storage,
    // before it can continue to replicate the leader's local log.
    start(follower)
    awaitISR(tp, 2)

    // now that the follower is in the ISR, the epoch cache must match with that of the leader
    assertEquals(epochCache(leader).epochEntries, epochCache(follower).epochEntries)
  }

  private def getLog(broker: KafkaServer, partition: Int): AbstractLog = {
    broker.logManager.getLog(new TopicPartition(topic, partition)).orNull
  }

  private def stop(server: KafkaServer): Unit = {
    server.shutdown()
    producer.close()
    producer = createProducer
  }

  private def start(server: KafkaServer): Unit = {
    server.startup()
    producer.close()
    producer = createProducer
  }

  private def bounce(server: KafkaServer): Unit = {
    server.shutdown()
    server.startup()
    producer.close()
    producer = createProducer
  }

  private def epochCache(broker: KafkaServer): LeaderEpochFileCache = {
    val log = getLog(broker, 0)
    log.leaderEpochCache.get
  }

  private def awaitISR(tp: TopicPartition, numReplicas: Int): Unit = {
    TestUtils.waitUntilTrue(() => {
      leader.replicaManager.nonOfflinePartition(tp).get.inSyncReplicaIds.size == numReplicas
    }, "Timed out waiting for replicas to join ISR")
  }

  private def createProducer: KafkaProducer[Array[Byte], Array[Byte]] = {
    TestUtils.createProducer(getBrokerListStrFromServers(brokers), acks = -1)
  }

  private def leader: KafkaServer = {
    assertEquals(2, brokers.size)
    val leaderId = zkClient.getLeaderForPartition(new TopicPartition(topic, 0)).get
    brokers.find(_.config.brokerId == leaderId).get
  }

  private def follower: KafkaServer = {
    assertEquals(2, brokers.size)
    val leader = zkClient.getLeaderForPartition(new TopicPartition(topic, 0)).get
    brokers.find(_.config.brokerId != leader).get
  }

  private def createBroker(id: Int, enableUncleanLeaderElection: Boolean = false): KafkaServer = {
    val config = createBrokerConfig(id, zkConnect)
    config.setProperty(KafkaConfig.UncleanLeaderElectionEnableProp, enableUncleanLeaderElection.toString)
    config.setProperty(KafkaConfig.TierFeatureProp, "true")
    config.setProperty(KafkaConfig.TierBackendProp, "mock")
    config.setProperty(KafkaConfig.TierMetadataReplicationFactorProp, "2")
    config.setProperty(KafkaConfig.TierMetadataNumPartitionsProp, "1")
    config.setProperty(KafkaConfig.TierLocalHotsetBytesProp, "0")
    config.setProperty(KafkaConfig.LogCleanupIntervalMsProp, "10")
    config.setProperty(KafkaConfig.LeaderImbalancePerBrokerPercentageProp, "0")
    config.setProperty(KafkaConfig.LeaderImbalanceCheckIntervalSecondsProp, "10")
    createServer(fromProps(config))
  }
}
