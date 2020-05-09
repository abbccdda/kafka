/*
 * Copyright 2020 Confluent Inc.
 */
package integration.kafka.link

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.time.Duration
import java.util
import java.util.{Collections, Properties}
import java.util.concurrent.{ExecutionException, TimeUnit}

import kafka.api.{IntegrationTestHarness, KafkaSasl, SaslSetup}
import kafka.controller.ReplicaAssignment
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.server.link.{ClusterLinkConfig, ClusterLinkTopicState}
import kafka.utils.Implicits._
import kafka.utils.{JaasTestUtils, Logging, TestUtils}
import kafka.zk.ConfigEntityChangeNotificationZNode
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.NewPartitions
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfigDef, ConfigResource, SaslConfigs, SslConfigs, TopicConfig}
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.requests.NewClusterLink
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.scram.ScramCredential
import org.junit.Assert.{assertEquals, assertNotEquals, assertTrue}
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._
import scala.collection.{Map, Seq, mutable}

class ClusterLinkIntegrationTest extends Logging {

  val sourceCluster = new ClusterLinkTestHarness(SecurityProtocol.SASL_SSL)
  val destCluster = new ClusterLinkTestHarness(SecurityProtocol.SASL_PLAINTEXT)
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

    destCluster.deleteClusterLink(linkName)
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
    val (leader2, _) = sourceCluster.shutdownLeader(tp)
    sourceCluster.startBroker(leader1)
    truncate(producedRecords, 2)
    produceToSourceCluster(4)
    val (endOffset, _) = TestUtils.computeUntilTrue(logEndOffset(sourceCluster.servers(leader1), tp).get)(_ >= producedRecords.size)
    assertEquals(producedRecords.size, endOffset)
    consume(sourceCluster, topic)
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
    waitForMirror(topic, servers = destCluster.servers.filter(_ != destCluster.servers(leader1)))

    // Trigger unclean leader election in the destination cluster. No truncation is expected.
    // Produce records and ensure that all records are replicated in destination leader and follower
    val (leader2, _) = destCluster.shutdownLeader(tp)
    destCluster.startBroker(leader1)
    produceToSourceCluster(2)
    waitForMirror(topic, servers = destCluster.servers.filter(_ != destCluster.servers(leader2)))
    destCluster.servers(leader2).startup()
    produceToSourceCluster(2)
    verifyMirror(topic)
  }

  /**
    * Scenario:
    *   destBroker1 shutdown
    *   Source cluster: (epoch=0, 0-99) (epoch=1, 100-199) (epoch=2, 200-299) (epoch=3, 300-399)
    *   sourceBroker1 shutdown
    *   sourceBroker2 produces: (epoch=4, 400-499) with only one replica
    *   destBroker2 mirrors: (epoch=0, 0-99) (epoch=1, 100-199) (epoch=2, 200-299) (epoch=3, 300-399) (epoch=4, 400-499)
    *   destBroker2 shutdown
    *   sourceBroker2 shutdown
    *   sourceBroker1 starts up becomes leader, (epoch=4, 400-499) needs truncation in followers
    *   destBroker1 starts up and becomes leader with no records yet and hence no truncation
    *   destBroker2 starts up, becomes follower, gets offsets from destBroker1 and performs truncation
    *   Source cluster: (epoch=0, 0-99) (epoch=1, 100-199) (epoch=2, 200-299) (epoch=3, 300-399) (epoch=6 400-499)
    *   Wait for mirror and verify records in destBroker1 and destBroker2
    */
  @Test
  def testDestFollowerAheadOfLeader(): Unit = {
    numPartitions = 1
    val tp = partitions.head
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)
    destCluster.createClusterLink(linkName, sourceCluster)
    destCluster.linkTopic(topic, numPartitions, linkName)

    val (destBroker1, _) = destCluster.shutdownLeader(tp)
    val destBroker2 = TestUtils.waitUntilLeaderIsElectedOrChanged(destCluster.zkClient, topic, 0, oldLeaderOpt = Some(destBroker1))

    // Produce records with multiple source epochs and wait for destBroker2 to mirror them. The last
    // batch is produced with a single leader and will be truncated due to unclean leader election later.
    produceToSourceCluster(100)
    (0 until 3).foreach { i =>
      sourceCluster.bounceLeader(tp)
      produceToSourceCluster(100)
    }
    val (sourceBroker1, _) = sourceCluster.shutdownLeader(tp)
    produceToSourceCluster(100)
    waitForMirror(topic, Seq(destCluster.servers(destBroker2)))

    // Shutdown destination destBroker2 and trigger unclean leader election in the source cluster
    destCluster.shutdownLeader(tp)
    val (sourceBroker2, _) = sourceCluster.shutdownLeader(tp)
    truncate(producedRecords, 100)
    sourceCluster.startBroker(sourceBroker1)

    // Startup destBroker1 so that destBroker1 with no records becomes the new leader
    destCluster.startBroker(destBroker1)
    val newLeader = TestUtils.waitUntilLeaderIsElectedOrChanged(destCluster.zkClient, topic, 0, oldLeaderOpt = Some(destBroker2))
    assertEquals(destBroker1, newLeader)

    // Restart destBroker2 which was ahead of destBroker1 and verify the mirrored records on leader and follower
    destCluster.startBroker(destBroker2)
    produceToSourceCluster(100)
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

  @Test
  def testAlterClusterLinkConfigs(): Unit = {
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)

    // Create a mirror and produce some records.
    destCluster.createClusterLink(linkName, sourceCluster, metadataMaxAgeMs = 10000L)
    destCluster.linkTopic(topic, numPartitions, linkName)
    produceToSourceCluster(8)
    waitForMirror(topic)

    // Update non-critical non-dynamic config metadata.max.age.ms
    destCluster.alterClusterLink(linkName, Map(CommonClientConfigs.METADATA_MAX_AGE_CONFIG -> "60000"))
    produceToSourceCluster(8)
    waitForMirror(topic)

    // Update critical non-dynamic config bootstrap.servers. Restart source brokers to ensure
    // new bootstrap servers are required for the test to pass.
    sourceCluster.servers.foreach(_.shutdown())
    sourceCluster.servers.foreach(_.startup())
    sourceCluster.updateBootstrapServers()
    destCluster.alterClusterLink(linkName, Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> sourceCluster.brokerList))
    produceToSourceCluster(8)
    waitForMirror(topic)

    // Update critical dynamic truststore path config
    val oldFile = new File(destCluster.clusterLinks(linkName).getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG))
    val newFile = File.createTempFile("truststore", ".jks")
    Files.copy(oldFile.toPath, newFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    destCluster.alterClusterLink(linkName, Map(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG -> newFile.getAbsolutePath))
    produceToSourceCluster(8)
    waitForMirror(topic)
    verifyMirror(topic)
  }

  @Test
  def testSourceTopicDelete(): Unit = {
    val numRecords = 10
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)
    destCluster.createClusterLink(linkName, sourceCluster, retryTimeoutMs = 10000)
    destCluster.linkTopic(topic, numPartitions, linkName)

    produceToSourceCluster(numRecords)
    waitForMirror(topic)
    assertTrue(destCluster.topicLinkState(topic).shouldSync)
    sourceCluster.createAdminClient().deleteTopics(Collections.singleton(topic)).all()
      .get(20, TimeUnit.SECONDS)
    TestUtils.waitUntilTrue(() => !destCluster.topicLinkState(topic).shouldSync,
      "Source topic deletion not propagated", waitTimeMs = 20000)

    val topicProps = new Properties
    topicProps.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "100000")
    sourceCluster.createTopic(topic, 1, replicationFactor = 2, topicProps)
    produceToSourceCluster(numRecords)
    truncate(producedRecords, numRecords)

    // Verify that partitions and configs of new source topic are not sync'ed
    val destAdmin = destCluster.createAdminClient()
    val topicDescription = destAdmin.describeTopics(Collections.singleton(topic))
      .values().get(topic)
      .get(20, TimeUnit.SECONDS)
    assertEquals(numPartitions, topicDescription.partitions().size())

    val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
    val topicConfigs = destAdmin.describeConfigs(Collections.singleton(topicResource))
      .values().get(topicResource)
      .get(20, TimeUnit.SECONDS)
    val maxMessageSize = topicConfigs.entries().asScala.find(entry => entry.name == TopicConfig.MAX_MESSAGE_BYTES_CONFIG)
    assertTrue(maxMessageSize.nonEmpty)
    assertNotEquals("100000", maxMessageSize.get.value)
    verifyMirror(topic)
  }

  @Test
  def testDestReadOnly(): Unit = {
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)

    // Create a mirror and produce some records.
    destCluster.createClusterLink(linkName, sourceCluster, metadataMaxAgeMs = 10000L)
    destCluster.linkTopic(topic, numPartitions, linkName)
    produceToSourceCluster(4)
    waitForMirror(topic)

    // Attempt to produce to the mirror.
    val producer = destCluster.createProducer()
    try {
      producer.send(new ProducerRecord(topic, 0, 0, "key".getBytes, "value".getBytes)).get(15, TimeUnit.SECONDS)
    } catch {
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[InvalidRequestException])
    } finally {
      producer.close()
    }

    // Produce more records to the source and verify we see no additional records.
    produceToSourceCluster(4)
    waitForMirror(topic)
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
    val numPartitions = producer.partitionsFor(topic).size()
    assertTrue(s"Invalid partition count $numPartitions", numPartitions > 0)
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

class ClusterLinkTestHarness(kafkaSecurityProtocol: SecurityProtocol) extends IntegrationTestHarness with SaslSetup {

  override val brokerCount = 3
  private val kafkaClientSaslMechanism = "SCRAM-SHA-256"
  private val kafkaServerSaslMechanisms = Collections.singletonList("SCRAM-SHA-256").asScala

  override protected def securityProtocol = kafkaSecurityProtocol

  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))
  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))

  val clusterLinks = mutable.Map[String, Properties]()
  private var producer: KafkaProducer[Array[Byte], Array[Byte]] = _

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
    maybeShutdownProducer()
  }

  def createClusterLink(linkName: String,
                        sourceCluster: ClusterLinkTestHarness,
                        metadataMaxAgeMs: Long = 60000L,
                        retryTimeoutMs: Long = 30000L): Unit = {
    val userName = s"user-$linkName"
    val password = s"secret-$linkName"
    val linkJaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";"
      .format(userName, password)

    sourceCluster.createLinkCredentials(userName, password)
    val props = new Properties
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, sourceCluster.brokerList)
    props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, metadataMaxAgeMs.toString)
    props.put(ClusterLinkConfig.RetryTimeoutMsProp, retryTimeoutMs.toString)
    props ++= sourceCluster.clientSecurityProps(linkName)
    props.put(SaslConfigs.SASL_JAAS_CONFIG, linkJaasConfig)
    val linkConfigs = ConfigDef.convertToStringMapWithPasswordValues(props.asInstanceOf[util.Map[String, _]])

    val newLink = new NewClusterLink(linkName, null, linkConfigs)
    servers.head.clusterLinkManager.admin.createClusterLink(
      newLink,
      validateLink = false,
      validateOnly = false,
      timeoutMs = 10000
    ).get(15, TimeUnit.SECONDS)

    servers.foreach { server =>
      TestUtils.waitUntilTrue(() =>
        server.clusterLinkManager.fetcherManager(linkName).nonEmpty,
        s"Linked fetcher not created for $linkName on broker ${server.config.brokerId}")
    }
    val linkProps = new Properties
    linkProps ++= linkConfigs.asScala
    clusterLinks += linkName -> linkProps
  }

  def deleteClusterLink(linkName: String): Unit = {
    servers.head.clusterLinkManager.admin.deleteClusterLink(linkName, validateOnly = false, force = false)
    servers.foreach { server =>
      TestUtils.waitUntilTrue(() =>
        server.clusterLinkManager.fetcherManager(linkName).isEmpty,
        s"Linked fetcher not deleted for $linkName on broker ${server.config.brokerId}")
    }
    clusterLinks.remove(linkName)
  }

  def alterClusterLink(linkName: String, updatedConfigs: Map[String, String]): Unit = {
    val newProps = new Properties()
    newProps ++= clusterLinks(linkName)
    newProps ++= updatedConfigs
    adminZkClient.changeClusterLinkConfig(linkName, newProps)
    clusterLinks.put(linkName, newProps)
    servers.foreach { server =>
      TestUtils.waitUntilTrue(() => {
        val config = server.clusterLinkManager.fetcherManager(linkName).get.currentConfig
        updatedConfigs.forall { case (name, value) => config.originals.get(name) == value }
      }, s"Linked fetcher configs not updated for $linkName on broker ${server.config.brokerId}")
    }
  }

  def linkTopic(topic: String, numPartitions: Int, linkName: String): Unit = {
    val assignment = (0 until numPartitions).map { i =>
      i -> ReplicaAssignment(Seq(i % 2, (i + 1) % 2), Seq.empty)
    }.toMap

    val mirror = ClusterLinkTopicState.Mirror(linkName)
    adminZkClient.createTopicWithAssignment(topic,
      config = new Properties(),
      assignment,
      clusterLink = Some(mirror))

    assertEquals(Map(topic -> mirror), zkClient.getClusterLinkForTopics(Set(topic)))
  }

  def unlinkTopic(topic: String, linkName: String): Unit = {
    val assignment = zkClient.getFullReplicaAssignmentForTopics(Set(topic)).map { case (tp, tpAssignment) =>
      (tp.partition, tpAssignment)
    }
    adminZkClient.writeTopicPartitionAssignment(topic, assignment, isUpdate = true, clusterLink = None)

    servers.foreach { server =>
      TestUtils.waitUntilTrue(() =>
        server.clusterLinkManager.fetcherManager(linkName).forall(_.isEmpty),
        s"Linked fetchers not stopped for $linkName on broker ${server.config.brokerId}")
    }
  }

  def topicLinkState(topic: String): ClusterLinkTopicState = {
    val topicLinkOpt = zkClient.getReplicaAssignmentAndTopicIdForTopics(Set(topic)).head.clusterLink
    assertTrue("Cluster link not found", topicLinkOpt.nonEmpty)
    topicLinkOpt.get
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
    startBroker(oldLeaderId)
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

  def getOrCreateProducer(): KafkaProducer[Array[Byte], Array[Byte]] = {
    if (producer != null) producer else createProducer()
  }

  def maybeShutdownProducer(): Unit = {
    val oldProducer = this.producer
    this.producer = null
    if (oldProducer != null)
      oldProducer.close(Duration.ZERO)
  }

  def startBroker(brokerId: Int): Unit = {
    servers(brokerId).startup()
    updateBootstrapServers()
  }
}
