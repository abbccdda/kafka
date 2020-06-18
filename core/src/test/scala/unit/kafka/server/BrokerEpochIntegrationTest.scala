/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.api.LeaderAndIsr
import kafka.cluster.Broker
import kafka.controller.{ControlMetadataBatch, LeaderAndIsrBatch, StopReplicaBatch, UpdateMetadataBatch, ControllerChannelManager, ControllerContext, StateChangeLogger}
import kafka.utils.TestUtils
import kafka.utils.TestUtils.createTopic
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Time
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.jdk.CollectionConverters._

class BrokerEpochIntegrationTest extends ZooKeeperTestHarness {
  val brokerId1 = 0
  val brokerId2 = 1

  var servers: Seq[KafkaServer] = Seq.empty[KafkaServer]

  @Before
  override def setUp(): Unit = {
    super.setUp()
    val configs = Seq(
      TestUtils.createBrokerConfig(brokerId1, zkConnect),
      TestUtils.createBrokerConfig(brokerId2, zkConnect))

    configs.foreach { config =>
        config.setProperty(KafkaConfig.AutoLeaderRebalanceEnableProp, false.toString)}

    // start both servers
    servers = configs.map(config => TestUtils.createServer(KafkaConfig.fromProps(config)))
  }

  @After
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testReplicaManagerBrokerEpochMatchesWithZk(): Unit = {
    val brokerAndEpochs = zkClient.getAllBrokerAndEpochsInCluster
    assertEquals(brokerAndEpochs.size, servers.size)
    brokerAndEpochs.foreach {
      case (broker, epoch) =>
        val brokerServer = servers.find(e => e.config.brokerId == broker.id)
        assertTrue(brokerServer.isDefined)
        assertEquals(epoch, brokerServer.get.kafkaController.brokerEpoch)
    }
  }

  @Test
  def testControllerBrokerEpochCacheMatchesWithZk(): Unit = {
    val controller = getController
    val otherBroker = servers.find(e => e.config.brokerId != controller.config.brokerId).get

    // Broker epochs cache matches with zk in steady state
    checkControllerBrokerEpochsCacheMatchesWithZk(controller.kafkaController.controllerContext)

    // Shutdown a broker and make sure broker epochs cache still matches with zk state
    otherBroker.shutdown()
    checkControllerBrokerEpochsCacheMatchesWithZk(controller.kafkaController.controllerContext)

    // Restart a broker and make sure broker epochs cache still matches with zk state
    otherBroker.startup()
    checkControllerBrokerEpochsCacheMatchesWithZk(controller.kafkaController.controllerContext)
  }

  @Test
  def testControlRequestWithCorrectBrokerEpoch(): Unit = {
    testControlRequestWithBrokerEpoch(0)
  }

  @Test
  def testControlRequestWithStaleBrokerEpoch(): Unit = {
    testControlRequestWithBrokerEpoch(-1)
  }

  @Test
  def testControlRequestWithNewerBrokerEpoch(): Unit = {
    testControlRequestWithBrokerEpoch(1)
  }

  private def testControlRequestWithBrokerEpoch(epochInRequestDiffFromCurrentEpoch: Long): Unit = {
    val tp = new TopicPartition("new-topic", 0)

    // create topic with 1 partition, 2 replicas, one on each broker
    createTopic(zkClient, tp.topic(), partitionReplicaAssignment = Map(0 -> Seq(brokerId1, brokerId2)), servers = servers)

    val controllerId = 2
    val controllerEpoch = zkClient.getControllerEpoch.get._1

    val controllerConfig = KafkaConfig.fromProps(TestUtils.createBrokerConfig(controllerId, zkConnect))
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val brokerAndEpochs = servers.map(s =>
      (new Broker(s.config.brokerId, "localhost", TestUtils.boundPort(s), listenerName, securityProtocol),
        s.kafkaController.brokerEpoch)).toMap
    val nodes = brokerAndEpochs.keys.map(_.node(listenerName))

    val controllerContext = new ControllerContext
    controllerContext.setLiveBrokers(brokerAndEpochs)
    val metrics = new Metrics
    val controllerChannelManager = new ControllerChannelManager(controllerContext, controllerConfig, Time.SYSTEM,
      metrics, new StateChangeLogger(controllerId, inControllerContext = true, None))
    controllerChannelManager.startup()

    val broker2 = servers(brokerId2)
    val epochInRequest = broker2.kafkaController.brokerEpoch + epochInRequestDiffFromCurrentEpoch

    try {
      // Send LeaderAndIsr request with correct broker epoch
      {
        val batch = new LeaderAndIsrBatch(brokerId2)
          .setControllerId(controllerId)
          .setControllerEpoch(controllerEpoch)
          .setBrokerEpoch(epochInRequest)
          .addPartitionState(tp, new LeaderAndIsrPartitionState()
            .setTopicName(tp.topic)
            .setPartitionIndex(tp.partition)
            .setControllerEpoch(controllerEpoch)
            .setLeader(brokerId2)
            .setLeaderEpoch(LeaderAndIsr.initialLeaderEpoch + 1)
            .setIsr(Seq(brokerId1, brokerId2).map(Integer.valueOf).asJava)
            .setZkVersion(LeaderAndIsr.initialZKVersion)
            .setReplicas(Seq(0, 1).map(Integer.valueOf).asJava)
            .setIsNew(false))
          .addLiveLeaders(nodes.toSet)

        if (epochInRequestDiffFromCurrentEpoch < 0) {
          // stale broker epoch in LEADER_AND_ISR
          sendAndVerifyStaleBrokerEpochInResponse(controllerChannelManager, batch)
        }
        else {
          // broker epoch in LEADER_AND_ISR >= current broker epoch
          sendAndVerifySuccessfulResponse(controllerChannelManager, batch)
          TestUtils.waitUntilLeaderIsKnown(Seq(broker2), tp, 10000)
        }
      }

      // Send UpdateMetadata request with correct broker epoch
      {
        val batch = new UpdateMetadataBatch(brokerId2)
          .setControllerId(controllerId)
          .setControllerEpoch(controllerEpoch)
          .setBrokerEpoch(epochInRequest)
          .addPartitionState(tp, new UpdateMetadataPartitionState()
            .setTopicName(tp.topic)
            .setPartitionIndex(tp.partition)
            .setControllerEpoch(controllerEpoch)
            .setLeader(brokerId2)
            .setLeaderEpoch(LeaderAndIsr.initialLeaderEpoch + 1)
            .setIsr(Seq(brokerId1, brokerId2).map(Integer.valueOf).asJava)
            .setZkVersion(LeaderAndIsr.initialZKVersion)
            .setReplicas(Seq(0, 1).map(Integer.valueOf).asJava))
          .addLiveBrokers(brokerAndEpochs.keys.toSet)

        if (epochInRequestDiffFromCurrentEpoch < 0) {
          // stale broker epoch in UPDATE_METADATA
          sendAndVerifyStaleBrokerEpochInResponse(controllerChannelManager, batch)
        }
        else {
          // broker epoch in UPDATE_METADATA >= current broker epoch
          sendAndVerifySuccessfulResponse(controllerChannelManager, batch)
          TestUtils.waitUntilMetadataIsPropagated(Seq(broker2), tp.topic, tp.partition, 10000)
          assertEquals(brokerId2,
            broker2.metadataCache.getPartitionInfo(tp.topic, tp.partition).get.leader)
        }
      }

      // Send StopReplica request with correct broker epoch
      {
        val batch = new StopReplicaBatch(brokerId2)
          .setControllerId(controllerId)
          .setControllerEpoch(controllerEpoch)
          .setBrokerEpoch(epochInRequest)
          .addPartitionState(tp, new StopReplicaPartitionState()
            .setPartitionIndex(tp.partition())
            .setLeaderEpoch(LeaderAndIsr.initialLeaderEpoch + 2)
            .setDeletePartition(true))

        if (epochInRequestDiffFromCurrentEpoch < 0) {
          // stale broker epoch in STOP_REPLICA
          sendAndVerifyStaleBrokerEpochInResponse(controllerChannelManager, batch)
        } else {
          // broker epoch in STOP_REPLICA >= current broker epoch
          sendAndVerifySuccessfulResponse(controllerChannelManager, batch)
          assertEquals(HostedPartition.None, broker2.replicaManager.getPartition(tp))
        }
      }
    } finally {
      controllerChannelManager.shutdown()
      metrics.close()
    }
  }

  private def getController: KafkaServer = {
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    servers.filter(s => s.config.brokerId == controllerId).head
  }

  private def checkControllerBrokerEpochsCacheMatchesWithZk(controllerContext: ControllerContext): Unit = {
    val brokerAndEpochs = zkClient.getAllBrokerAndEpochsInCluster
    TestUtils.waitUntilTrue(() => {
      val brokerEpochsInControllerContext = controllerContext.liveBrokerIdAndEpochs
      if (brokerAndEpochs.size != brokerEpochsInControllerContext.size) false
      else {
        brokerAndEpochs.forall {
          case (broker, epoch) => brokerEpochsInControllerContext.get(broker.id).contains(epoch)
        }
      }
    }, "Broker epoch mismatches")
  }

  private def sendAndVerifyStaleBrokerEpochInResponse(controllerChannelManager: ControllerChannelManager,
                                                      builder: ControlMetadataBatch): Unit = {
    var staleBrokerEpochDetected = false
    controllerChannelManager.sendControlMetadataBatch(brokerId2, builder, (_, result) => {
      staleBrokerEpochDetected = result.error == Errors.STALE_BROKER_EPOCH
    })
    TestUtils.waitUntilTrue(() => staleBrokerEpochDetected, "Broker epoch should be stale")
    assertTrue("Stale broker epoch not detected by the broker", staleBrokerEpochDetected)
  }

  private def sendAndVerifySuccessfulResponse(controllerChannelManager: ControllerChannelManager,
                                              builder: ControlMetadataBatch): Unit = {
    @volatile var succeed = false
    controllerChannelManager.sendControlMetadataBatch(brokerId2, builder, (_, result) => {
      succeed = result.error == Errors.NONE
    })
    TestUtils.waitUntilTrue(() => succeed, "Should receive response with no errors")
  }
}
