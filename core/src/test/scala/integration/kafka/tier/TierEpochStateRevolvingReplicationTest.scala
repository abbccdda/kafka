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
import kafka.tier.TierTestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{TopicConfig, ConfluentTopicConfig}
import org.apache.kafka.common.utils.Exit
import org.apache.kafka.common.utils.Exit.Procedure
import org.junit.Assert.{assertEquals, assertFalse}
import org.junit.{Before, After, Test}

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.util.Random

// TODO: we can remove this test once we have significant system tests.
class TierEpochStateRevolvingReplicationTest extends ZooKeeperTestHarness with Logging {

  val topic = "topic1"
  val msg = new Array[Byte](1000)
  val msgBigger = new Array[Byte](10000)
  var brokers: Seq[KafkaServer] = null
  var producer: KafkaProducer[Array[Byte], Array[Byte]] = null
  val exited = new AtomicBoolean(false)

  @Before
  override def setUp() {
    Exit.setExitProcedure(new Procedure {
      override def execute(statusCode: Int, message: String): Unit = exited.set(true)
    })
    super.setUp()
  }

  @After
  override def tearDown() {
    producer.close()
    TestUtils.shutdownServers(brokers)
    super.tearDown()
    assertFalse(exited.get())
  }

  @Test
  def testTierStateRestoreToReplication(): Unit = {
    //Given 3 brokers
    brokers = (100 to 102).map(createBroker(_))

    val properties = new Properties()
    properties.put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "true")
    properties.put(TopicConfig.SEGMENT_BYTES_CONFIG, "50000")
    properties.put(TopicConfig.RETENTION_BYTES_CONFIG, "-1")
    properties.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")

    //A single partition topic with 3 replicas
    TestUtils.createTopic(zkClient, topic, Map(0 -> Seq(100, 101, 102)), brokers, properties)
    producer = createProducer
    val tp = new TopicPartition(topic, 0)

    // since we use RF = 3 on the tier topic,
    // we must make sure the tier topic is up before we start stopping brokers
    brokers.foreach(b => TierTestUtils.awaitTierTopicPartition(b, 0))

    producer.send(new ProducerRecord(topic, 0, null, msg)).get

    awaitISR(tp, 3)

    for (i <- 0 to 5) {
      val followerToShutdown = randomFollower()

      //Stop the follower before writing anything
      stop(followerToShutdown)
      awaitISR(tp, 2)

      val logEndPriorToProduce = leader.replicaManager.logManager.getLog(tp).get.localLogEndOffset

      // a follower is now shutdown, write a bunch of messages
      for (i <- 0 until 1000) {
        producer.send(new ProducerRecord(topic, 0, null, msg)).get
      }

      TestUtils.waitUntilTrue(() => {
        // delete some segments so that replication will happen
        val leaderLog = leader.replicaManager.logManager.getLog(tp)
        leaderLog.get.deleteOldSegments()
        leaderLog.get.localLogStartOffset > logEndPriorToProduce
      }, "timed out waiting for segment tiering and deletion",
        60000)

      start(followerToShutdown)
      awaitISR(tp, 3)

      // send one more message so that final epochs match, since leader epoch won't be attached
      // to epoch state in follower unless there is an additional message
      producer.send(new ProducerRecord(topic, 0, null, msg)).get

      for (broker <- brokers) {
        waitForLogEndOffsetToMatch(leader, broker, 0)
      }

      // check all the broker epoch entries match
      for (broker <- brokers) {
        assertEquals(epochCache(broker).epochEntries, epochCache(leader).epochEntries)
      }
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
      leader.replicaManager.nonOfflinePartition(tp).get.inSyncReplicas.map(_.brokerId).size == numReplicas
    }, "Timed out waiting for replicas to join ISR")
  }

  private def createProducer: KafkaProducer[Array[Byte], Array[Byte]] = {
    TestUtils.createProducer(getBrokerListStrFromServers(brokers), acks = -1)
  }

  private def leader(): KafkaServer = {
    assertEquals(3, brokers.size)
    val leaderId = zkClient.getLeaderForPartition(new TopicPartition(topic, 0)).get
    brokers.filter(_.config.brokerId == leaderId)(0)
  }

  private def randomFollower(): KafkaServer = {
    assertEquals(3, brokers.size)
    val leader = zkClient.getLeaderForPartition(new TopicPartition(topic, 0)).get
    Random.shuffle(brokers.filter(_.config.brokerId != leader).toList).head
  }

  private def createBroker(id: Int, enableUncleanLeaderElection: Boolean = false): KafkaServer = {
    val config = createBrokerConfig(id, zkConnect)
    config.setProperty(KafkaConfig.UncleanLeaderElectionEnableProp, enableUncleanLeaderElection.toString)
    config.setProperty(KafkaConfig.TierBackendProp, "mock")
    config.setProperty(KafkaConfig.TierFeatureProp, "true")
    config.setProperty(KafkaConfig.TierMetadataReplicationFactorProp, "3")
    config.setProperty(KafkaConfig.TierMetadataNumPartitionsProp, "1")
    config.setProperty(KafkaConfig.TierLocalHotsetBytesProp, "0")
    createServer(fromProps(config))
  }
}
