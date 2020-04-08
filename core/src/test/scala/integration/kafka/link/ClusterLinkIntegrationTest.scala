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
package integration.kafka.link

import java.util.concurrent.TimeUnit
import java.util.{Collections, Properties}

import kafka.api.{IntegrationTestHarness, KafkaSasl, SaslSetup}
import kafka.controller.ReplicaAssignment
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.Implicits._
import kafka.utils.{JaasTestUtils, Logging, TestUtils}
import kafka.zk.ConfigEntityChangeNotificationZNode
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.NewPartitions
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.scram.ScramCredential
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._
import scala.collection.{Map, Seq, mutable}

class ClusterLinkIntegrationTest extends Logging {

  val sourceCluster = new ClusterLinkTestHarness
  val destCluster = new ClusterLinkTestHarness
  val topic = "linkedTopic"
  var numPartitions = 4
  val linkName = "testLink"
  val producedRecords = mutable.Buffer[ProducerRecord[Array[Byte], Array[Byte]]]()
  var nextProduceIndex: Int = 0

  @Before
  def setUp(): Unit = {
    sourceCluster.setUp()
    destCluster.setUp()
  }

  @After
  def tearDown(): Unit = {
    destCluster.tearDown()
    sourceCluster.tearDown()
  }

  /**
    * Verifies topic mirroring when mirroring is set up on a source topic that is empty.
    */
  @Test
  def testMirrorNewRecords(): Unit = {
    val numRecords = 20
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)
    destCluster.createClusterLink(linkName, sourceCluster)
    destCluster.linkTopic(topic, numPartitions, linkName)

    produceToSourceCluster(numRecords)
    consume(sourceCluster, topic)

    verifyMirror(topic)
  }

  /**
    * Verifies topic mirroring when mirroring is set up on a source topic that contains records.
    */
  @Test
  def testMirrorExistingRecords(): Unit = {
    val numRecords = 20
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)
    produceToSourceCluster(numRecords)

    destCluster.createClusterLink(linkName, sourceCluster)
    destCluster.linkTopic(topic, numPartitions, linkName)
    verifyMirror(topic)
  }

  /**
    * Verifies topic mirroring with source leader changes. Verifies that truncation is performed
    * for unclean leader election.
    */
  @Test
  def testSourceLeaderChanges(): Unit = {
    numPartitions = 1
    val tp = partitions.head
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)

    // Produce before and after source epoch change
    produceToSourceCluster(2)
    sourceCluster.bounceLeader(tp)
    produceToSourceCluster(2)

    // Create a topic mirror when source epoch > 0 and verify that records with different epochs are mirrored
    destCluster.createClusterLink(linkName, sourceCluster)
    destCluster.linkTopic(topic, numPartitions, linkName)
    var destLeaderEpoch = destCluster.waitForLeaderEpochChange(tp, 0, sourceCluster.leaderEpoch(tp))
    waitForMirror(topic)
    produceToSourceCluster(2)
    waitForMirror(topic)

    // Shutdown leader and verify clean leader election. No truncation is expected.
    val (leader1, _) = sourceCluster.shutdownLeader(tp)
    produceToSourceCluster(2)
    destLeaderEpoch = destCluster.waitForLeaderEpochChange(tp, destLeaderEpoch, sourceCluster.leaderEpoch(tp))
    waitForMirror(topic)

    // Trigger unclean leader election in the source cluster and ensure truncation is performed
    // on the leader as well as follower in the destination cluster
    /* TODO: Enable after additional testing to ensure this is not flaky
    val (leader2, _) = sourceCluster.shutdownLeader(tp)
    sourceCluster.servers(leader1).startup()
    sourceCluster.updateBootstrapServers()
    truncate(producedRecords, 2)
    produceToSourceCluster(4)
    assertEquals(producedRecords.size, logEndOffset(sourceCluster.servers(leader1), tp).get)
    consume(sourceCluster, topic)
    */
    verifyMirror(topic)
  }

  /**
    * Verifies topic mirroring with destination leader changes. Verifies that truncation is not
    * performed for clean or unclean leader elections.
    */
  @Test
  def testDestLeaderChanges(): Unit = {
    numPartitions = 1
    val tp = partitions.head
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)

    // Create a mirror and produce some records.
    destCluster.createClusterLink(linkName, sourceCluster)
    destCluster.linkTopic(topic, numPartitions, linkName)
    produceToSourceCluster(2)
    waitForMirror(topic)

    // Shutdown destination leader and verify clean leader election. No truncation is expected.
    val (leader1, _) = destCluster.shutdownLeader(tp)
    produceToSourceCluster(2)
    waitForMirror(topic, servers = destCluster.servers - destCluster.servers(leader1))

    // Trigger unclean leader election in the destination cluster. No truncation is expected.
    // Produce records and ensure that all records are replicated in destination leader and follower
    val (leader2, _) = destCluster.shutdownLeader(tp)
    destCluster.servers(leader1).startup()
    destCluster.addClusterLink(destCluster.servers(leader1), linkName)
    destCluster.updateBootstrapServers()
    produceToSourceCluster(2)
    waitForMirror(topic, servers = destCluster.servers - destCluster.servers(leader2))
    destCluster.servers(leader2).startup()
    produceToSourceCluster(2)
    verifyMirror(topic)
  }

  @Test
  def testAddPartitions(): Unit = {
    numPartitions = 1
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)

    // Create a mirror and produce some records.
    destCluster.createClusterLink(linkName, sourceCluster, metadataMaxAgeMs = 1000L)
    destCluster.linkTopic(topic, numPartitions, linkName)
    produceToSourceCluster(4)
    waitForMirror(topic)

    numPartitions = 4
    sourceCluster.createAdminClient()
      .createPartitions(Collections.singletonMap(topic, NewPartitions.increaseTo(numPartitions)))
      .all().get(20, TimeUnit.SECONDS)
    produceToSourceCluster(8)

    val (numDestPartitions, _) = TestUtils.computeUntilTrue(destCluster.adminZkClient.numPartitions(Set(topic))(topic)) {
      _ == numPartitions
    }
    assertEquals(numPartitions, numDestPartitions)

    produceToSourceCluster(8)
    verifyMirror(topic)
  }

  private def verifyMirror(topic: String): Unit = {
    waitForMirror(topic)
    destCluster.unlinkTopic(topic, linkName)
    consume(destCluster, topic)
  }

  private def waitForMirror(topic: String, servers: Seq[KafkaServer] = destCluster.servers): Unit = {
    val offsetsByPartition = (0 until numPartitions).map { i =>
      i -> producedRecords.count(_.partition == i).toLong
    }.toMap
    partitions.foreach { tp =>
      val expectedOffset = offsetsByPartition.getOrElse(tp.partition, 0L)
      val leader = TestUtils.waitUntilLeaderIsKnown(servers, tp)
      servers.foreach { server =>
        server.replicaManager.nonOfflinePartition(tp).foreach { _ =>
          val (offset, _) = TestUtils.computeUntilTrue(logEndOffset(server, tp).get)(_ == expectedOffset)
          assertEquals(s"Unexpected offset on broker ${server.config.brokerId} leader $leader", expectedOffset, offset)
        }
      }
    }
  }

  private def logEndOffset(server: KafkaServer, tp: TopicPartition): Option[Long] = {
    server.replicaManager.getLog(tp).map(_.localLogEndOffset)
  }

  private def partitions: Seq[TopicPartition] = (0 until numPartitions).map { i => new TopicPartition(topic, i) }

  private def produceToSourceCluster(numRecords: Int): Unit = {
    val producer = sourceCluster.createProducer()
    produceRecords(producer, topic, numRecords)
    producer.close()
  }

  def produceRecords(producer: KafkaProducer[Array[Byte], Array[Byte]], topic: String, numRecords: Int): Unit = {
    val futures = (0 until numRecords).map { _ =>
      val index = nextProduceIndex
      nextProduceIndex += 1
      val record = new ProducerRecord(topic, index % numPartitions, index.toLong, s"key $index".getBytes, s"value $index".getBytes)
      producedRecords += record
      producer.send(record)
    }
    futures.foreach(_.get(15, TimeUnit.SECONDS))
  }

  def consume(cluster: ClusterLinkTestHarness, topic: String): Unit = {
    val consumer = cluster.createConsumer()
    consumer.assign(partitions.asJava)
    consumeRecords(consumer)
    consumer.close()
  }

  def consumeRecords(consumer: KafkaConsumer[Array[Byte], Array[Byte]]): Unit = {
    val consumedRecords = TestUtils.consumeRecords(consumer, producedRecords.size, waitTimeMs = 20000)
    val producedByPartition = producedRecords.groupBy(_.partition)
    val consumedByPartition = consumedRecords.groupBy(_.partition)
    producedByPartition.foreach { case (partition, produced) =>
      val consumed = consumedByPartition(partition)
      assertEquals(produced.size, consumed.size)
      produced.zipWithIndex.foreach { case (producedRecord, i) =>
        val consumedRecord = consumed(i)
        assertEquals(i, consumedRecord.offset)
        assertEquals(topic, consumedRecord.topic)
        assertEquals(new String(producedRecord.key), new String(consumedRecord.key))
        assertEquals(new String(producedRecord.value), new String(consumedRecord.value))
      }
    }
  }

  def truncate(records: mutable.Buffer[ProducerRecord[Array[Byte], Array[Byte]]], numRecords: Int): Unit = {
    records.remove(records.size - numRecords, numRecords)
  }
}

class ClusterLinkTestHarness extends IntegrationTestHarness with SaslSetup {

  override val brokerCount = 3
  private val kafkaClientSaslMechanism = "SCRAM-SHA-256"
  private val kafkaServerSaslMechanisms = Collections.singletonList("SCRAM-SHA-256").asScala

  override protected def securityProtocol = SecurityProtocol.SASL_PLAINTEXT

  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))

  private val clusterLinks = mutable.Map[String, Properties]()

  serverConfig.put(KafkaConfig.OffsetsTopicReplicationFactorProp, brokerCount.toString)
  serverConfig.put(KafkaConfig.UncleanLeaderElectionEnableProp, "true")
  consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

  override def configureSecurityBeforeServersStart(): Unit = {
    super.configureSecurityBeforeServersStart()
    zkClient.makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)
    createScramCredentials(zkConnect, JaasTestUtils.KafkaScramAdmin, JaasTestUtils.KafkaScramAdminPassword)
    createScramCredentials(zkConnect, JaasTestUtils.KafkaScramUser, JaasTestUtils.KafkaScramPassword)
    createScramCredentials(zkConnect, JaasTestUtils.KafkaScramUser2, JaasTestUtils.KafkaScramPassword2)

    startSasl(jaasSections(kafkaServerSaslMechanisms, Option(kafkaClientSaslMechanism), KafkaSasl))
  }

  def updateBootstrapServers(): Unit = {
    brokerList = TestUtils.bootstrapServers(servers, listenerName)
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
  }

  def createClusterLink(linkName: String, sourceCluster: ClusterLinkTestHarness, metadataMaxAgeMs: Long = 60000L): Unit = {
    val userName = s"user-$linkName"
    val password = s"secret-$linkName"
    val linkJaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";"
      .format(userName, password)

    sourceCluster.createLinkCredentials(userName, password)
    val props = new Properties
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, sourceCluster.brokerList)
    props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, metadataMaxAgeMs.toString)
    props ++= sourceCluster.clientSecurityProps(linkName)
    props.put(SaslConfigs.SASL_JAAS_CONFIG, linkJaasConfig)
    clusterLinks += linkName -> props

    servers.foreach { server =>
     addClusterLink(server, linkName)
    }
  }

  def deleteClusterLink(linkName: String): Unit = {
    servers.foreach { server =>
      server.replicaManager.clusterLinkManager.removeClusterLink(linkName)
    }
    clusterLinks.remove(linkName)
  }

  def addClusterLink(server: KafkaServer, linkName: String): Unit = {
    server.replicaManager.clusterLinkManager.addClusterLink(linkName, clusterLinks(linkName))
  }

  def linkTopic(topic: String, numPartitions: Int, linkName: String): Unit = {
    val assignment = (0 until numPartitions).map { i =>
      i -> ReplicaAssignment(Seq(i % 2, (i + 1) % 2), Seq.empty)
    }.toMap
    adminZkClient.createTopicWithAssignment(topic,
      config = new Properties(),
      assignment,
      clusterLink = Some(linkName))

    assertEquals(Map(topic -> linkName), zkClient.getClusterLinkForTopics(Set(topic)))
  }

  def unlinkTopic(topic: String, linkName: String): Unit = {
    val assignment = zkClient.getFullReplicaAssignmentForTopics(Set(topic)).map { case (tp, tpAssignment) =>
      (tp.partition, tpAssignment)
    }
    adminZkClient.writeTopicPartitionAssignment(topic, assignment, isUpdate = true, clusterLink = None)

    servers.foreach { server =>
      TestUtils.waitUntilTrue(() =>
        server.replicaManager.clusterLinkManager.fetcherManager(linkName).forall(_.isEmpty),
        s"Linked fetchers not stopped for $linkName on broker ${server.config.brokerId}")
    }
  }

  def createLinkCredentials(userName: String, password: String): Unit = {
    createScramCredentials(zkConnect, userName, password)
    servers.foreach { server =>
      val cache = server.credentialProvider.credentialCache.cache(kafkaClientSaslMechanism, classOf[ScramCredential])
      TestUtils.waitUntilTrue(() => cache.get(userName) != null, "SCRAM credentials not created")
    }
  }

  def bounceLeader(tp: TopicPartition): Unit = {
    val (oldLeaderId, oldLeaderEpoch) = shutdownLeader(tp)
    waitForLeaderChange(tp, oldLeaderId, oldLeaderEpoch)
    servers(oldLeaderId).startup()
    updateBootstrapServers()
  }

  def shutdownLeader(tp: TopicPartition): (Int, Int) = {
    val leaderId = TestUtils.waitUntilLeaderIsKnown(servers, tp)
    val epoch = leaderEpoch(tp)
    val leader = servers(leaderId)
    leader.shutdown()
    leader.awaitShutdown()
    updateBootstrapServers()
    (leaderId, epoch)
  }

  def leaderEpoch(partition: TopicPartition): Int = {
    val leader = TestUtils.waitUntilLeaderIsKnown(servers, partition)
    TestUtils.findLeaderEpoch(leader, partition, servers)
  }

  def waitForLeaderEpoch(partition: TopicPartition, expectedEpoch: Int): Unit = {
    val (epoch, _) = TestUtils.computeUntilTrue(leaderEpoch(partition), 5000)(_ == expectedEpoch)
    assertEquals(expectedEpoch, epoch)
  }

  def waitForLeaderChange(tp: TopicPartition, oldLeaderId: Int, oldLeaderEpoch: Int): (Int, Int) = {
    val newLeaderId = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, tp.topic, tp.partition, oldLeaderOpt = Some(oldLeaderId))
    val newLeaderEpoch = leaderEpoch(tp)
    assertTrue(s"Unexpected leader epoch old=$oldLeaderEpoch new=$newLeaderEpoch", newLeaderEpoch > oldLeaderEpoch)
    (newLeaderId, newLeaderEpoch)
  }

  def waitForLeaderEpochChange(tp: TopicPartition, currentEpoch: Int, sourceEpoch: Int): Int = {
    val expectedMinEpoch = Math.max(currentEpoch + 1, sourceEpoch)
    val (epoch, _) = TestUtils.computeUntilTrue(leaderEpoch(tp))(_ >= expectedMinEpoch)
    assertTrue(s"Leader epoch not updated epoch=$epoch expected>=$expectedMinEpoch", epoch >= expectedMinEpoch)
    epoch
  }
}



