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
import kafka.server.epoch.{LeaderEpochFileCache, EpochEntry}
import kafka.server.{KafkaServer, KafkaConfig}
import kafka.utils.{TestUtils, Logging}
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{TopicConfig, ConfluentTopicConfig}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.utils.Exit
import org.apache.kafka.common.utils.Exit.Procedure
import org.junit.Assert.{assertEquals, assertFalse}
import org.junit.{Before, After, Test}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer => Buffer}
import scala.collection.Seq

class TierEpochStateReplicationTest extends ZooKeeperTestHarness with Logging {

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
    //Given 2 brokers
    brokers = (100 to 101).map(createBroker(_))

    val properties = new Properties()
    properties.put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "true")
    properties.put(TopicConfig.SEGMENT_BYTES_CONFIG, "10000")
    properties.put(TopicConfig.RETENTION_BYTES_CONFIG, "-1")

    // since we use RF = 2 on the tier topic,
    // we must make sure the tier topic is up before we start stopping brokers
    // otherwise no tiering will take place
    // Tier topic partition 0 will lie on all brokers due to RF.
    brokers.foreach(b => TierTestUtils.awaitTierTopicPartition(b, 0))

    //A single partition topic with 2 replicas
    TestUtils.createTopic(zkClient, topic, Map(0 -> Seq(100, 101)), brokers, properties)
    producer = createProducer
    val tp = new TopicPartition(topic, 0)

    producer.send(new ProducerRecord(topic, 0, null, msg)).get

    // check epoch entries
    assertEquals(Buffer(EpochEntry(0, 0)), epochCache(leader).epochEntries)
    assertEquals(Buffer(EpochEntry(0, 0)), epochCache(leader).epochEntries)

    // bounce leader to bump initial epoch
    bounce(leader)

    awaitISR(tp, 2)

    producer.send(new ProducerRecord(topic, 0, null, msg)).get

    assertEquals(Buffer(EpochEntry(0, 0), EpochEntry(1, 1)), epochCache(leader).epochEntries)
    assertEquals(Buffer(EpochEntry(0, 0), EpochEntry(1, 1)), epochCache(follower).epochEntries)

    val followerToShutdown = follower()
    //Stop the follower before writing anything
    stop(followerToShutdown)
    awaitISR(tp, 1)

    val logEndPriorToProduce = leader.replicaManager.logManager.getLog(tp).get.localLogEndOffset

    // follower is now shutdown, write a bunch of messages
    for (i <- 0 until 999) {
      producer.send(new ProducerRecord(topic, 0, null, msg)).get
    }

    TestUtils.waitUntilTrue(() => {
      // delete some segments so that replication will happen
      val leaderLog = leader.replicaManager.logManager.getLog(tp)
      leaderLog.get.deleteOldSegments()
      leaderLog.get.localLogStartOffset > logEndPriorToProduce
    }, "timed out waiting for segment tiering and deletion",
      60000)

    assertEquals(1001, latestRecord(leader).nextOffset())

    //The message should have epoch 2 stamped onto it after we shutdown the follower
    assertEquals(2, latestRecord(leader).partitionLeaderEpoch())

    // leader should have recorded Epoch 2 at Offset 2, and new entries at epoch 1
    assertEquals(Buffer(EpochEntry(0, 0), EpochEntry(1, 1), EpochEntry(2, 2)), epochCache(leader).epochEntries)

    start(followerToShutdown)
    awaitISR(tp, 2)

    // Since we've started back up the leader and we've replicated, follower and leader entries should be the same
    // even though we only replicated a portion from the leader
    assertEquals(Buffer(EpochEntry(0, 0), EpochEntry(1, 1), EpochEntry(2, 2)), epochCache(leader).epochEntries)
    assertEquals(Buffer(EpochEntry(0, 0), EpochEntry(1, 1), EpochEntry(2, 2)), epochCache(follower).epochEntries)
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

  private def latestRecord(leader: KafkaServer, offset: Int = -1, partition: Int = 0): RecordBatch = {
    val segment = getLog(leader, partition).activeSegment
    val records = segment.read(0, None, Integer.MAX_VALUE).records
    records.batches().asScala.toSeq.last
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
    assertEquals(2, brokers.size)
    val leaderId = zkClient.getLeaderForPartition(new TopicPartition(topic, 0)).get
    brokers.filter(_.config.brokerId == leaderId)(0)
  }

  private def follower(): KafkaServer = {
    assertEquals(2, brokers.size)
    val leader = zkClient.getLeaderForPartition(new TopicPartition(topic, 0)).get
    brokers.filter(_.config.brokerId != leader)(0)
  }

  private def createBroker(id: Int, enableUncleanLeaderElection: Boolean = false): KafkaServer = {
    val config = createBrokerConfig(id, zkConnect)
    config.setProperty(KafkaConfig.UncleanLeaderElectionEnableProp, enableUncleanLeaderElection.toString)
    config.setProperty(KafkaConfig.TierFeatureProp, "true")
    config.setProperty(KafkaConfig.TierBackendProp, "mock")
    config.setProperty(KafkaConfig.TierMetadataReplicationFactorProp, "2")
    config.setProperty(KafkaConfig.TierMetadataNumPartitionsProp, "1")
    config.setProperty(KafkaConfig.TierLocalHotsetBytesProp, "0")
    createServer(fromProps(config))
  }
}
