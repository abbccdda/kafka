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

package kafka.server

import java.util.Properties

import kafka.log.LogConfig._
import kafka.server.KafkaConfig.fromProps
import kafka.server.QuotaType._
import kafka.utils.TestUtils._
import kafka.utils.CoreUtils._
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.{After, Test}

import scala.collection.JavaConverters._

/**
  * This is the main test which ensures Replication Quotas work correctly.
  *
  * The test will fail if the quota is < 1MB/s as 1MB is the default for replica.fetch.max.bytes.
  * So with a throttle of 100KB/s, 1 fetch of 1 partition would fill 10s of quota. In turn causing
  * the throttled broker to pause for > 10s
  *
  * Anything over 100MB/s tends to fail as this is the non-throttled replication rate
  */
class ReplicationQuotasTest extends ZooKeeperTestHarness {
  def percentError(percent: Int, value: Long): Long = Math.round(value * percent / 100)

  val msg100KB = new Array[Byte](100000)
  var brokers: Seq[KafkaServer] = Seq()
  val topic = "topic1"
  var producer: KafkaProducer[Array[Byte], Array[Byte]] = null

  @After
  override def tearDown(): Unit = {
    producer.close()
    shutdownServers(brokers)
    super.tearDown()
  }

  @Test
  def shouldBootstrapTwoBrokersWithLeaderThrottle(): Unit = {
    shouldMatchQuota(replicateWithDynamicThrottledReplicasThroughAnAsymmetricTopology, true)
  }

  @Test
  def shouldBootstrapTwoBrokersWithFollowerThrottle(): Unit = {
    shouldMatchQuota(replicateWithDynamicThrottledReplicasThroughAnAsymmetricTopology, false)
  }

  @Test
  def shouldBootstrapTwoBrokersWithBrokerReplicasFollowerThrottle(): Unit = {
    shouldMatchQuota(replicateWithStaticThrottledReplicas, false)
  }

  @Test
  def shouldBootstrapTwoBrokersWithBrokerReplicasLeaderThrottle(): Unit = {
    shouldMatchQuota(replicateWithStaticThrottledReplicas, true)
  }

  /**
   * shouldMatchQuota runs a replication test with a given throttle
   * and asserts that the time it took to replicate is as expected
   */
  def shouldMatchQuota(replicationTest: ReplicationTestSettings => ReplicationResults,
                       testLeaderThrottle: Boolean): Unit = {
    val msg = msg100KB
    val msgCount = 100
    val expectedDuration = 10 //Keep the test to N seconds
    var throttle: Long = msgCount * msg.length / expectedDuration
    // Follower throttle needs to replicate 3x as fast to get the same duration as there are three replicas to replicate for each of the two follower brokers
    if (!testLeaderThrottle) throttle = throttle * 3

    val replicationResults = replicationTest(ReplicationTestSettings(throttle, msgCount, msg, testLeaderThrottle))

    // Check the times for throttled/unthrottled are each side of what we expect
    val throttledLowerBound = expectedDuration * 1000 * 0.9
    val throttledUpperBound = expectedDuration * 1000 * 3
    assertTrue(s"Expected ${replicationResults.unthrottledTime} < $throttledLowerBound",
      replicationResults.unthrottledTime < throttledLowerBound)
    assertTrue(s"Expected ${replicationResults.throttledTime} > $throttledLowerBound", replicationResults.throttledTime > throttledLowerBound)
    assertTrue(s"Expected ${replicationResults.throttledTime} < $throttledUpperBound", replicationResults.throttledTime < throttledUpperBound)

    // Check the rate metric matches what we expect.
    // In a short test the brokers can be read unfairly, so assert against the average
    val rateUpperBound = throttle * 1.1
    val rateLowerBound = throttle * 0.5
    val rate = if (testLeaderThrottle) avRate(LeaderReplication, 100 to 105) else avRate(FollowerReplication, 106 to 107)
    assertTrue(s"Expected $rate < $rateUpperBound", rate < rateUpperBound)
    assertTrue(s"Expected $rate > $rateLowerBound", rate > rateLowerBound)
  }

  def replicateWithStaticThrottledReplicas(settings: ReplicationTestSettings): ReplicationResults = {
    val initialBrokers = 100 to 105
    createBrokers(initialBrokers, settings.throttleBytes,
      throttleAllBrokerLeaderReplicas = settings.throttleLeader,
      throttleAllBrokerFollowerReplicas = !settings.throttleLeader)
    createBrokers(Seq(108), settings.throttleBytes) // unthrottled broker

    // additionally test that dynamically set throttles get reverted to the static values
    setAndUnsetDynamicThrottles(initialBrokers, settings.throttleBytes, 100)

    // Given six partitions, led on nodes 0,1,2,3,4,5 but with followers on node 6,7 (not started yet)
    // And one extra partition 6, on new unthrottled nodes (8, 9) which we don't intend on throttling.
    val assignment = Map(
      0 -> Seq(100, 106), //Throttled
      1 -> Seq(101, 106), //Throttled
      2 -> Seq(102, 106), //Throttled
      3 -> Seq(103, 107), //Throttled
      4 -> Seq(104, 107), //Throttled
      5 -> Seq(105, 107), //Throttled
      6 -> Seq(108, 109), //Not Throttled
    )
    TestUtils.createTopic(zkClient, topic, assignment, brokers)

    //Add data equally to each partition
    producer = createProducer(getBrokerListStrFromServers(brokers), acks = 1)
    (0 until settings.msgCount).foreach { _ =>
      (0 to 6).foreach { partition =>
        producer.send(new ProducerRecord(topic, partition, null, settings.msg))
      }
    }

    //Ensure data is fully written: broker 1 has partition 1, broker 2 has partition 2 etc
    (0 to 5).foreach { id => waitForOffsetsToMatch(settings.msgCount, id, 100 + id) }
    //Check the non-throttled partitions too
    waitForOffsetsToMatch(settings.msgCount, 6, 108)

    val start = System.currentTimeMillis()

    // 2 new, empty throttled brokers
    createBrokers(106 to 107, settings.throttleBytes,
      throttleAllBrokerLeaderReplicas = settings.throttleLeader,
      throttleAllBrokerFollowerReplicas = !settings.throttleLeader)
    createBrokers(109 to 109, settings.throttleBytes) // 1 new, empty unthrottled broker

    // assert all brokers are throttled as expected
    (100 to 107).foreach { brokerId =>
      val broker = brokerFor(brokerId)
      val (quotaManager, quotaName) = if (settings.throttleLeader)
        (broker.quotaManagers.leader, "leader")
      else
        (broker.quotaManagers.follower, "follower")

      assertTrue(s"Expected all $quotaName replicas on broker $brokerId", quotaManager.isThrottled(tp(0)))
    }

    //Wait for non-throttled partitions to replicate first
    waitForOffsetsToMatch(settings.msgCount, 6, 109)
    val unthrottledTime = System.currentTimeMillis() - start

    // Wait for replicas 0,1,2,3,4,5 to become fully replicated to broker 106,107
    (0 to 2).foreach { id => waitForOffsetsToMatch(settings.msgCount, id, 106) }
    (3 to 5).foreach { id => waitForOffsetsToMatch(settings.msgCount, id, 107) }

    val throttledTime = System.currentTimeMillis() - start
    ReplicationResults(unthrottledTime = unthrottledTime, throttledTime = throttledTime)
  }

  /**
   * In short we have 8 brokers, 2 are not-started. We assign replicas for the two non-started
   * brokers, so when we start them we can monitor replication from the 6 to the 2.
   *
   * We also have two non-throttled partitions on two of the 6 brokers, just to make sure
   * regular replication works as expected.
   */
  def replicateWithDynamicThrottledReplicasThroughAnAsymmetricTopology(settings: ReplicationTestSettings): ReplicationResults = {
    createBrokers(100 to 105, settings.throttleBytes)

    //Given six partitions, led on nodes 0,1,2,3,4,5 but with followers on node 6,7 (not started yet)
    //And two extra partitions 6,7, which we don't intend on throttling.
    val assignment = Map(
      0 -> Seq(100, 106), //Throttled
      1 -> Seq(101, 106), //Throttled
      2 -> Seq(102, 106), //Throttled
      3 -> Seq(103, 107), //Throttled
      4 -> Seq(104, 107), //Throttled
      5 -> Seq(105, 107), //Throttled
      6 -> Seq(100, 106), //Not Throttled
      7 -> Seq(101, 107) //Not Throttled
    )
    TestUtils.createTopic(zkClient, topic, assignment, brokers)

    //Either throttle the six leaders or the two followers
    if (settings.throttleLeader)
      adminZkClient.changeTopicConfig(topic, propsWith(LeaderReplicationThrottledReplicasProp, "0:100,1:101,2:102,3:103,4:104,5:105" ))
    else
      adminZkClient.changeTopicConfig(topic, propsWith(FollowerReplicationThrottledReplicasProp, "0:106,1:106,2:106,3:107,4:107,5:107"))

    //Add data equally to each partition
    producer = createProducer(getBrokerListStrFromServers(brokers), acks = 1)
    (0 until settings.msgCount).foreach { _ =>
      (0 to 7).foreach { partition =>
        producer.send(new ProducerRecord(topic, partition, null, settings.msg))
      }
    }

    //Ensure data is fully written: broker 1 has partition 1, broker 2 has partition 2 etc
    (0 to 5).foreach { id => waitForOffsetsToMatch(settings.msgCount, id, 100 + id) }
    //Check the non-throttled partitions too
    waitForOffsetsToMatch(settings.msgCount, 6, 100)
    waitForOffsetsToMatch(settings.msgCount, 7, 101)

    val start = System.currentTimeMillis()

    //When we create the 2 new, empty brokers
    createBrokers(106 to 107, settings.throttleBytes)

    //Check that throttled config is present in the new brokers
    (106 to 107).foreach { brokerId =>
      assertEquals(settings.throttleBytes, brokerFor(brokerId).quotaManagers.follower.upperBound())
    }
    if (!settings.throttleLeader) {
      (0 to 2).foreach { partition => assertTrue(brokerFor(106).quotaManagers.follower.isThrottled(tp(partition))) }
      (3 to 5).foreach { partition => assertTrue(brokerFor(107).quotaManagers.follower.isThrottled(tp(partition))) }
    }

    //Wait for non-throttled partitions to replicate first
    (6 to 7).foreach { id => waitForOffsetsToMatch(settings.msgCount, id, 100 + id) }
    val unthrottledTook = System.currentTimeMillis() - start

    //Wait for replicas 0,1,2,3,4,5 to fully replicated to broker 106,107
    (0 to 2).foreach { id => waitForOffsetsToMatch(settings.msgCount, id, 106) }
    (3 to 5).foreach { id => waitForOffsetsToMatch(settings.msgCount, id, 107) }
    val throttledTook = System.currentTimeMillis() - start

    ReplicationResults(unthrottledTime = unthrottledTook, throttledTime = throttledTook)
  }

  @Test
  def shouldThrottleOldSegments(): Unit = {
    /**
      * Simple test which ensures throttled replication works when the dataset spans many segments
      */

    //2 brokers with 1MB Segment Size & 1 partition
    val config: Properties = createBrokerConfig(100, zkConnect)
    config.put("log.segment.bytes", (1024 * 1024).toString)
    brokers = Seq(createServer(fromProps(config)))
    TestUtils.createTopic(zkClient, topic, Map(0 -> Seq(100, 101)), brokers)

    //Write 20MBs and throttle at 5MB/s
    val msg = msg100KB
    val msgCount: Int = 200
    val expectedDuration = 4
    val throttle: Long = msg.length * msgCount / expectedDuration

    //Set the throttle to only limit leader
    adminZkClient.changeBrokerConfig(Seq(100), propsWith(KafkaConfig.LeaderReplicationThrottledRateProp, throttle.toString))
    adminZkClient.changeTopicConfig(topic, propsWith(LeaderReplicationThrottledReplicasProp, "0:100"))

    //Add data
    addData(msgCount, msg)

    //Start the new broker (and hence start replicating)
    debug("Starting new broker")
    brokers = brokers :+ createServer(fromProps(createBrokerConfig(101, zkConnect)))
    val start = System.currentTimeMillis()

    waitForOffsetsToMatch(msgCount, 0, 101)

    val throttledTook = System.currentTimeMillis() - start

    assertTrue(s"Throttled replication of ${throttledTook}ms should be > ${expectedDuration * 1000 * 0.9}ms",
      throttledTook > expectedDuration * 1000 * 0.9)
    assertTrue(s"Throttled replication of ${throttledTook}ms should be < ${expectedDuration * 1500}ms",
      throttledTook < expectedDuration * 1000 * 1.5)
  }

  def addData(msgCount: Int, msg: Array[Byte]): Unit = {
    producer = createProducer(getBrokerListStrFromServers(brokers), acks = 0)
    (0 until msgCount).map(_ => producer.send(new ProducerRecord(topic, msg))).foreach(_.get)
    waitForOffsetsToMatch(msgCount, 0, 100)
  }

  private def waitForOffsetsToMatch(offset: Int, partitionId: Int, brokerId: Int): Unit = {
    waitUntilTrue(() => {
      offset == brokerFor(brokerId).getLogManager.getLog(new TopicPartition(topic, partitionId))
        .map(_.logEndOffset).getOrElse(0)
    }, s"Offsets did not match for partition $partitionId on broker $brokerId", 60000)
  }

  private def brokerFor(id: Int): KafkaServer = brokers.filter(_.config.brokerId == id).head

  /**
   * @param unthrottledTime - the time it took to fully replicate a non-throttled replica
   * @param throttledTime - the time it took to fully replicate a throttled replica
   */
  case class ReplicationResults(unthrottledTime: Long, throttledTime: Long)

  /**
   * @param throttleBytes - the bytes/seconds this test is throttling at
   * @param msgCount - the number of messages produced to each partition
   * @param msg - the message we're producing to the topic
   * @param throttleLeader - whether we should throttle the leader replicas or the followers
   */
  case class ReplicationTestSettings(throttleBytes: Long, msgCount: Int, msg: Array[Byte], throttleLeader: Boolean)

  def tp(partition: Int): TopicPartition = new TopicPartition(topic, partition)

  def createBrokers(brokerIds: Seq[Int], throttle: Long, throttleAllBrokerLeaderReplicas: Boolean = false,
                    throttleAllBrokerFollowerReplicas: Boolean = false): Unit = {
    brokerIds.foreach { id =>
      // We are setting the throttle limit on all 8 brokers
      // but we will only assign throttled replicas to the six leaders, or two followers
      val props = createBrokerConfig(id, zkConnect)
      props.setProperty(KafkaConfig.LeaderReplicationThrottledRateProp, throttle.toString)
      props.setProperty(KafkaConfig.FollowerReplicationThrottledRateProp, throttle.toString)
      props.setProperty(KafkaConfig.LeaderReplicationThrottledReplicasProp, throttledReplicasConfig(throttleAllBrokerLeaderReplicas))
      props.setProperty(KafkaConfig.FollowerReplicationThrottledReplicasProp, throttledReplicasConfig(throttleAllBrokerFollowerReplicas))
      brokers = brokers :+ createServer(fromProps(props))
      assertEquals(throttle, brokerFor(id).quotaManagers.leader.upperBound())
      assertEquals(throttle, brokerFor(id).quotaManagers.follower.upperBound())
    }
  }

  /**
   * Returns the appropriate config value for
   * `KafkaConfig.LeaderReplicationThrottledReplicasProp`/`KafkaConfig.FollowerReplicationThrottledReplicasProp`
   * depending on whether we want to throttle all replicas on the broker or not
   */
  def throttledReplicasConfig(shouldThrottleAllReplicas: Boolean): String =
    if (shouldThrottleAllReplicas)
      ReplicationQuotaManagerConfig.AllThrottledReplicasValue
    else
      ReplicationQuotaManagerConfig.NoThrottledReplicasValue

  private def avRate(replicationType: QuotaType, brokers: Seq[Int]): Double = {
    brokers.map(brokerFor).map(measuredRate(_, replicationType)).sum / brokers.length
  }

  private def measuredRate(broker: KafkaServer, repType: QuotaType): Double = {
    val metricName = broker.metrics.metricName("byte-rate", repType.toString)
    broker.metrics.metrics.asScala(metricName).metricValue.asInstanceOf[Double]
  }

  def setAndUnsetDynamicThrottles(brokers: Seq[Int], staticThrottleBytes: Long, dynamicThrottleBytes: Long): Unit = {
    brokers.foreach { brokerId =>
      adminZkClient.changeBrokerConfig(Seq(brokerId),
        propsWith(
          (KafkaConfig.LeaderReplicationThrottledRateProp, dynamicThrottleBytes.toString),
          (KafkaConfig.FollowerReplicationThrottledRateProp, dynamicThrottleBytes.toString)
        ))
    }
    TestUtils.waitUntilTrue(() => {
      brokers.forall { brokerId =>
        dynamicThrottleBytes == brokerFor(brokerId).quotaManagers.leader.upperBound() && dynamicThrottleBytes == brokerFor(brokerId).quotaManagers.follower.upperBound()
      }
    }, "Dynamically set throttles weren't as expected")
    brokers.foreach { brokerId =>
      adminZkClient.changeBrokerConfig(Seq(brokerId), propsWith())
    }
    TestUtils.waitUntilTrue(() => {
      brokers.forall { brokerId =>
        staticThrottleBytes == brokerFor(brokerId).quotaManagers.leader.upperBound() && staticThrottleBytes == brokerFor(brokerId).quotaManagers.follower.upperBound()
      }
    }, "Throttles weren't reverted to the statically set ones as expected")
  }
}
