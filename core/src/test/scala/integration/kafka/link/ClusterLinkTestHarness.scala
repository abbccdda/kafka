/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.link

import java.io.File
import java.time.Duration
import java.util
import java.util.concurrent.{ExecutionException, TimeUnit}
import java.util.{Collections, Optional, Properties, UUID}

import kafka.api.{IntegrationTestHarness, KafkaSasl, SaslSetup}
import kafka.server._
import kafka.server.link.{ClusterLinkConfig, ClusterLinkTopicState}
import kafka.utils.Implicits._
import kafka.utils.{JaasTestUtils, TestUtils}
import kafka.zk.ConfigEntityChangeNotificationZNode
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{Config => _, _}
import org.apache.kafka.common.requests.{AlterMirrorsRequest, ClusterLinkListing, NewClusterLink}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.scram.ScramCredential
import org.junit.Assert._

import scala.collection.Map
import scala.jdk.CollectionConverters._

class ClusterLinkTestHarness(kafkaSecurityProtocol: SecurityProtocol) extends IntegrationTestHarness with SaslSetup {

  override val brokerCount = 3
  private val kafkaClientSaslMechanism = "SCRAM-SHA-256"
  private val kafkaServerSaslMechanisms = Collections.singletonList("SCRAM-SHA-256").asScala
  private val adminTimeoutMs = 10 * 1000
  private val waitTimeMs = 15 * 1000

  override protected def securityProtocol = kafkaSecurityProtocol

  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))
  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))

  private var producer: KafkaProducer[Array[Byte], Array[Byte]] = _

  serverConfig.put(KafkaConfig.OffsetsTopicReplicationFactorProp, brokerCount.toString)
  serverConfig.put(KafkaConfig.UncleanLeaderElectionEnableProp, "true")
  serverConfig.put(KafkaConfig.PasswordEncoderSecretProp, "password-encoder-secret")
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

  def linkUserName(linkName: String) = s"user-$linkName"

  def createClusterLink(linkName: String,
                        sourceCluster: ClusterLinkTestHarness,
                        metadataMaxAgeMs: Long = 60000L,
                        retryTimeoutMs: Long = 30000L,
                        fetchMaxBytes: Long = Defaults.ReplicaFetchMaxBytes,
                        configOverrides: Properties = new Properties): UUID = {
    val userName = linkUserName(linkName)
    val password = s"secret-$linkName"
    val linkJaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";"
      .format(userName, password)

    sourceCluster.createLinkCredentials(userName, password)
    val props = new Properties
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, sourceCluster.brokerList)
    props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, metadataMaxAgeMs.toString)
    props.put(ClusterLinkConfig.RetryTimeoutMsProp, retryTimeoutMs.toString)
    props.put(KafkaConfig.ReplicaFetchMaxBytesProp, fetchMaxBytes.toString)
    props ++= sourceCluster.clientSecurityProps(linkName)
    props.put(SaslConfigs.SASL_JAAS_CONFIG, linkJaasConfig)
    props ++= configOverrides
    val linkConfigs = ConfigDef.convertToStringMapWithPasswordValues(props.asInstanceOf[util.Map[String, _]])

    val newLink = new NewClusterLink(linkName, null, linkConfigs)
    withAdmin((admin: ConfluentAdmin) => {
      val options = new CreateClusterLinksOptions().timeoutMs(adminTimeoutMs)
      admin.createClusterLinks(Collections.singleton(newLink), options).all.get(waitTimeMs, TimeUnit.MILLISECONDS)
    })

    val linkId = withAdmin((admin: ConfluentAdmin) => {
      val options = new ListClusterLinksOptions().timeoutMs(adminTimeoutMs)
      admin.listClusterLinks(options).result.get(waitTimeMs, TimeUnit.MILLISECONDS)
        .asScala.filter(_.linkName == linkName).head.linkId
    })

    servers.foreach { server =>
      TestUtils.waitUntilTrue(() =>
        server.clusterLinkManager.fetcherManager(linkId).nonEmpty,
        s"Linked fetcher not created for $linkName on broker ${server.config.brokerId}")
    }

    linkId
  }

  def listClusterLinks(includeTopics: Boolean = false): Seq[ClusterLinkListing] = {
    withAdmin((admin: ConfluentAdmin) => {
      val options = new ListClusterLinksOptions().includeTopics(includeTopics).timeoutMs(adminTimeoutMs)
      admin.listClusterLinks(options).result.get(waitTimeMs, TimeUnit.MILLISECONDS).asScala.toSeq
    })
  }

  def deleteClusterLink(linkName: String, force: Boolean = false): Unit = {
    val linkId = servers.head.clusterLinkManager.resolveLinkIdOrThrow(linkName)

    withAdmin((admin: ConfluentAdmin) => {
      val options = new DeleteClusterLinksOptions().force(force).timeoutMs(adminTimeoutMs)
      admin.deleteClusterLinks(Collections.singleton(linkName), options)
        .all.get(waitTimeMs, TimeUnit.MILLISECONDS)
    })

    servers.foreach { server =>
      TestUtils.waitUntilTrue(() =>
        server.clusterLinkManager.fetcherManager(linkId).isEmpty,
        s"Linked fetcher not deleted for $linkName on broker ${server.config.brokerId}")
    }
  }

  def alterClusterLink(linkName: String, updatedConfigs: Map[String, String]): Unit = {
    val resource = new ConfigResource(ConfigResource.Type.CLUSTER_LINK, linkName)
    val ops = updatedConfigs.map { case (k, v) =>
      new AlterConfigOp(new ConfigEntry(k, v), AlterConfigOp.OpType.SET)
    }

    withAdmin((admin: ConfluentAdmin) => {
      val options = new AlterConfigsOptions().timeoutMs(adminTimeoutMs)
      admin.incrementalAlterConfigs(Map(resource -> ops.asJavaCollection).asJava, options)
        .all.get(waitTimeMs, TimeUnit.MILLISECONDS)
    })

    servers.foreach { server =>
      val linkId = server.clusterLinkManager.resolveLinkIdOrThrow(linkName)
      TestUtils.waitUntilTrue(() => {
        val config = server.clusterLinkManager.fetcherManager(linkId).get.currentConfig
        updatedConfigs.forall { case (name, value) => config.originals.get(name) == value }
      }, s"Linked fetcher configs not updated for $linkName on broker ${server.config.brokerId}")
    }
  }

  def describeClusterLink(linkName: String): Config = {
    val resource = new ConfigResource(ConfigResource.Type.CLUSTER_LINK, linkName)
    withAdmin((admin: ConfluentAdmin) => {
      val options = new DescribeConfigsOptions().timeoutMs(adminTimeoutMs)
      admin.describeConfigs(Collections.singleton(resource), options)
        .all.get(waitTimeMs, TimeUnit.MILLISECONDS).get(resource)
    })
  }

  def linkTopic(topic: String,
                replicationFactor: Short,
                linkName: String,
                configs: Map[String, String] = Map.empty): CreateTopicsResult = {
    val newTopic = new NewTopic(topic, Optional.empty[Integer], Optional.of(Short.box(replicationFactor)))
    if (configs.nonEmpty)
      newTopic.configs(configs.asJava)
    newTopic.mirror(Optional.of(new NewTopicMirror(linkName, topic)))
    withAdmin((admin: ConfluentAdmin) => {
      val options = new CreateTopicsOptions().timeoutMs(adminTimeoutMs)
      val result = admin.createTopics(Collections.singleton(newTopic), options)
      result.all.get(waitTimeMs, TimeUnit.MILLISECONDS)
      result
    })
  }

  def unlinkTopic(topic: String, linkName: String, verifyShutdown: Boolean = true): Unit = {
    withAdmin((admin: ConfluentAdmin) => {
      val op = new AlterMirrorsRequest.StopTopicMirrorOp(topic)
      val options = new AlterMirrorsOptions().timeoutMs(adminTimeoutMs)
      admin.alterMirrors(Collections.singletonList(op), options)
        .all.get(waitTimeMs, TimeUnit.MILLISECONDS)
    })

    if (verifyShutdown) {
      servers.foreach { server =>
        val linkId = server.clusterLinkManager.resolveLinkIdOrThrow(linkName)
        TestUtils.waitUntilTrue(() =>
          server.clusterLinkManager.fetcherManager(linkId).forall(_.isEmpty),
          s"Linked fetchers not stopped for $linkName on broker ${server.config.brokerId}")
      }
    }
  }

  def describeTopic(topic: String): TopicDescription = {
    withAdmin((admin: ConfluentAdmin) => {
      val options = new DescribeTopicsOptions().timeoutMs(adminTimeoutMs)
      admin.describeTopics(Collections.singleton(topic), options)
        .all.get(waitTimeMs, TimeUnit.MILLISECONDS).get(topic)
    })
  }

  def describeTopicConfig(topic: String): Config = {
    val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
    withAdmin((admin: ConfluentAdmin) => {
      val options = new DescribeConfigsOptions().timeoutMs(adminTimeoutMs)
      admin.describeConfigs(Collections.singleton(resource), options)
        .all.get(waitTimeMs, TimeUnit.MILLISECONDS).get(resource)
    })
  }

  def alterTopic(topic: String, updatedConfigs: Map[String, String]): Unit = {
    val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
    val ops = updatedConfigs.map { case (k, v) =>
      new AlterConfigOp(new ConfigEntry(k, v), AlterConfigOp.OpType.SET)
    }

    withAdmin((admin: ConfluentAdmin) => {
      val options = new AlterConfigsOptions().timeoutMs(adminTimeoutMs)
      admin.incrementalAlterConfigs(Map(resource -> ops.asJavaCollection).asJava, options)
        .all.get(waitTimeMs, TimeUnit.MILLISECONDS)
    })
  }

  def deleteTopic(topic: String): Unit = {
    withAdmin((admin: ConfluentAdmin) => {
      val options = new DeleteTopicsOptions().timeoutMs(adminTimeoutMs)
      admin.deleteTopics(Collections.singleton(topic), options)
        .all.get(waitTimeMs, TimeUnit.MILLISECONDS)
    })
  }

  def createPartitions(topic: String, numPartitions: Int): Unit = {
    withAdmin((admin: ConfluentAdmin) => {
      val options = new CreatePartitionsOptions().timeoutMs(adminTimeoutMs)
      admin.createPartitions(Collections.singletonMap(topic, NewPartitions.increaseTo(numPartitions)), options)
        .all.get(waitTimeMs, TimeUnit.MILLISECONDS)
    })
  }

  def topicLinkState(topic: String): ClusterLinkTopicState = {
    val topicLinkOpt = zkClient.getReplicaAssignmentAndTopicIdForTopics(Set(topic)).head.clusterLink
    assertTrue("Cluster link not found", topicLinkOpt.nonEmpty)
    topicLinkOpt.get
  }

  def getOffset(topic: String, partition: Int, consumerGroup: String): Long = {
    withAdmin((admin: ConfluentAdmin) => {
      admin.listConsumerGroupOffsets(consumerGroup).partitionsToOffsetAndMetadata.get(waitTimeMs, TimeUnit.MILLISECONDS)
        .getOrDefault(new TopicPartition(topic, partition), new OffsetAndMetadata(0, "")).offset
    })
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
    val epoch = leaderEpoch(tp)
    val leader = partitionLeader(tp)
    leader.shutdown()
    leader.awaitShutdown()
    updateBootstrapServers()
    (leader.config.brokerId, epoch)
  }

  def partitionLeader(tp: TopicPartition): KafkaServer = {
    val leaderId = TestUtils.waitUntilLeaderIsKnown(servers, tp)
    servers(leaderId)
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

  /**
    * Runs the callable with an admin client that communicates with the cluster, closing it on completion.
    * All thrown ExecutionExceptions are unwrapped for convenience.
    *
    * @param callable the callable to invoke with an admin client
    * @return the result of the callable
    */
  def withAdmin[T](callable: ConfluentAdmin => T): T = {
    val admin = createAdminClient().asInstanceOf[ConfluentAdmin]
    try {
      callable(admin)
    } catch {
      case e: ExecutionException => throw e.getCause
    } finally {
      admin.close()
    }
  }
}
