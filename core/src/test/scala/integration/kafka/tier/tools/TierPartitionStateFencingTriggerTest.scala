/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tools

import java.io.{File, PrintWriter}
import java.util.Collections
import java.util.HashMap
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier

import kafka.api.IntegrationTestHarness
import kafka.log.Log
import kafka.server.{Defaults, KafkaConfig, LogDirFailureChannel}
import kafka.tier.TierTopicManagerCommitter
import kafka.tier.client.TierTopicConsumerSupplier
import kafka.tier.domain.AbstractTierMetadata
import kafka.tier.domain.TierRecordType
import kafka.tier.domain.TierPartitionFence
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.state.{FileTierPartitionState, OffsetAndEpoch, TierPartitionStatus}
import kafka.tier.TopicIdPartition
import kafka.tier.tools.common.FenceEventInfo
import kafka.tier.topic.TierTopicManagerConfig
import kafka.tier.topic.TierTopic
import kafka.tier.topic.TierTopicAdmin
import kafka.tier.topic.TierTopicConsumer
import kafka.tier.topic.TierTopicManager
import kafka.utils.{CoreUtils, TestUtils}
import kafka.zk.AdminZkClient
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.utils.Utils
import org.junit.{After, Before, Test}
import org.junit.Assert.{assertEquals, assertTrue}
import org.mockito.Mockito._
import org.scalatest.Assertions.assertThrows

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

import net.sourceforge.argparse4j.inf.ArgumentParserException

class TierPartitionStateFencingTriggerTest extends IntegrationTestHarness {
  override protected def brokerCount: Int = 3

  private val logDirFailureChannel = new LogDirFailureChannel(10)
  private val logDir = TestUtils.tempDir().getAbsolutePath
  private var tierPartitionStateFiles: Array[FileTierPartitionState] = Array()
  private val tpidsToBeFenced: Array[TopicIdPartition] = Array(
    new TopicIdPartition("the_dark_knight", UUID.randomUUID(),123),
    new TopicIdPartition("mummy_returns", UUID.randomUUID(), 456),
    new TopicIdPartition("mission_impossible", UUID.randomUUID(), 789)
  )
  private var topicIdPartitionsFile: File = _
  private var propertiesConfFile: File = _

  private def addReplica(topicIdPartition: TopicIdPartition, tierTopicConsumer: TierTopicConsumer): Unit = {
    val dir = new File(logDir + "/" + Log.logDirName(topicIdPartition.topicPartition))
    dir.mkdir()

    val tierPartitionState = new FileTierPartitionState(
      dir, logDirFailureChannel, topicIdPartition.topicPartition, true)
    tierPartitionState.setTopicId(topicIdPartition.topicId)
    tierPartitionStateFiles :+= tierPartitionState

    tierTopicConsumer.register(topicIdPartition, new TierTopicConsumer.ClientCtx {
      override def process(metadata: AbstractTierMetadata, offsetAndEpoch: OffsetAndEpoch): AppendResult = tierPartitionState.append(metadata, offsetAndEpoch)
      override def status(): TierPartitionStatus = tierPartitionState.status
      override def beginCatchup(): Unit = tierPartitionState.beginCatchup()
      override def completeCatchup(): Unit = tierPartitionState.onCatchUpComplete()
    })
  }

  @Before
  override def setUp(): Unit = {
    super.setUp()
    topicIdPartitionsFile = TestUtils.tempFile
    val pw = new PrintWriter(topicIdPartitionsFile)
    tpidsToBeFenced.foreach(tpid => {
      pw.write("%s,%s,%d".format(tpid.topicIdAsBase64(), tpid.topic(), tpid.partition()))
      pw.println()
    })
    pw.close

    propertiesConfFile = TestUtils.tempFile
  }

  @After
  override def tearDown(): Unit = {
    tierPartitionStateFiles.foreach { tierPartitionState =>
      tierPartitionState.close()
      tierPartitionState.delete()
    }

    super.tearDown()
  }

  @Test
  def testPartitionFenceEventInjectionAndFencing(): Unit = {
    // 1. Setup
    val tierTopicNamespace = ""
    val numTierTopicPartitions: Short = 19
    val tierTopicReplicationFactor = 3

    // 2. Create the TierTopic.
    createTopic(
      TierTopic.topicName(tierTopicNamespace),
      numTierTopicPartitions,
      tierTopicReplicationFactor,
      TierTopicAdmin.topicConfig)
    val adminZkClient = mock(classOf[AdminZkClient])
    val adminClientSupplier = new Supplier[AdminZkClient] {
      override def get(): AdminZkClient = adminZkClient
    }
    val tierTopic = new TierTopic(tierTopicNamespace, adminClientSupplier)
    when(adminZkClient.createTopic(tierTopic.topicName(),
        numTierTopicPartitions.asInstanceOf[Int],
        Defaults.TierMetadataReplicationFactor,
        TierTopicAdmin.topicConfig))
      .thenThrow(new TopicExistsException("topic exists"))
    when(adminZkClient.numPartitions(Set(tierTopic.topicName())))
      .thenReturn(Map(tierTopic.topicName() -> numTierTopicPartitions.asInstanceOf[Int]))
    tierTopic.ensureTopic(numTierTopicPartitions, Defaults.TierMetadataReplicationFactor)

    // 3. Trigger fencing on user partitions, by injecting TierPartitionFence events into
    // the corresponding TierTopic partitions.
    Utils.mkProperties(
      new HashMap[String, String] {
        put(KafkaConfig.TierMetadataBootstrapServersProp, brokerList)
        put(KafkaConfig.TierMetadataNamespaceProp, tierTopicNamespace)
      }
    ).store(new PrintWriter(propertiesConfFile), "")

    val fenceEvents: List[FenceEventInfo] =
      kafka.tier.tools.TierPartitionStateFencingTrigger.runMain(Array(
        kafka.tier.tools.RecoveryUtils.makeArgument(
          kafka.tier.tools.RecoveryUtils.TIER_PROPERTIES_CONF_FILE_CONFIG),
        propertiesConfFile.getPath,
        kafka.tier.tools.RecoveryUtils.makeArgument(
          kafka.tier.tools.TierPartitionStateFencingTrigger.FILE_FENCE_TARGET_PARTITIONS_CONFIG),
        topicIdPartitionsFile.getPath)
      ).asScala.toList
    assertEquals(tpidsToBeFenced.size, fenceEvents.size)

    val partitionToFenceEventInfoMap = Map[Int, FenceEventInfo]()
    (fenceEvents zip tpidsToBeFenced).map {
      case (output, input) => {
        val outputTpid = new TopicIdPartition(
          output.topic, CoreUtils.uuidFromBase64(output.topicIdBase64), output.partition)
        assertEquals(outputTpid, input)  // Ensure the TopicIdPartition is valid and same as input
        assertTrue(output.recordOffset >= 0)  // Ensure the record has a valid TierTopic offset >= 0
        CoreUtils.uuidFromBase64(output.recordMessageIdBase64)  // Ensure the record has a valid message UUID
        partitionToFenceEventInfoMap += (output.partition -> output)
      }
    }

    // 4. Using a dummy consumer, verify that exactly 1 TierPartitionFence event was written
    // to TierTopic.
    val config = new TierTopicManagerConfig(
      () => Collections.singletonMap(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList),
      tierTopicNamespace,
      numTierTopicPartitions,
      Defaults.TierMetadataReplicationFactor,
      -1,
      "unknown",
      Defaults.TierMetadataMaxPollMs,
      Defaults.TierMetadataRequestTimeoutMs,
      Defaults.TierPartitionStateCommitInterval,
      Collections.singletonList(logDir))
    val primaryConsumerSupplier = new TierTopicConsumerSupplier(config, "primary");
    val verificationConsumer = primaryConsumerSupplier.get()
    val tierTopicPartitions = TierTopicManager.partitions(
      tierTopic.topicName(), tierTopic.numPartitions().getAsInt());
    verificationConsumer.assign(tierTopicPartitions);
    tierTopicPartitions.forEach(
      tp => verificationConsumer.seekToBeginning(Collections.singletonList(tp)))
    val records = new ListBuffer[ConsumerRecord[Array[Byte], Array[Byte]]]()
    TestUtils.waitUntilTrue(() => {
      val batch = verificationConsumer.poll(config.pollDuration)
      val batchIterator = batch.iterator()
      while (batchIterator.hasNext) {
        records += batchIterator.next
      }
      records.size == tpidsToBeFenced.size
    }, "Timed out trying to fetch TierTopic records")

    val allFencedTpids = collection.mutable.Set[TopicIdPartition]() ++ tpidsToBeFenced
    records.foreach(record => {
      // Ensure each event is a TierPartitionFence event.
      val eventOpt: Optional[AbstractTierMetadata]
        = AbstractTierMetadata.deserialize(record.key(), record.value());
      assertTrue(eventOpt.isPresent)

      assertEquals(TierRecordType.PartitionFence, eventOpt.get.`type`())
      val fenceEvent = eventOpt.get.asInstanceOf[TierPartitionFence]
      assertTrue(allFencedTpids.contains(fenceEvent.topicIdPartition))
      allFencedTpids.remove(fenceEvent.topicIdPartition)

      // Check if the FenceEvent UUID matches with the output produced by the tool.
      val userPartition = fenceEvent.topicIdPartition.partition
      assertTrue(partitionToFenceEventInfoMap.contains(userPartition))
      val fenceEventInfo = partitionToFenceEventInfoMap(userPartition)
      assertEquals(fenceEvent.messageId, CoreUtils.uuidFromBase64(fenceEventInfo.recordMessageIdBase64))
      assertEquals(record.offset(), fenceEventInfo.recordOffset)
      partitionToFenceEventInfoMap -= userPartition
    })
    assertTrue(allFencedTpids.isEmpty)
    verificationConsumer.close

    // 5. Using a TierTopicConsumer, consume the injected TierPartitionFence and verify
    // that the materializer gets fenced.
    val tierTopicConsumer = new TierTopicConsumer(
      config,
      primaryConsumerSupplier,
      new TierTopicConsumerSupplier(config, "catchup"),
      new TierTopicManagerCommitter(config, logDirFailureChannel),
      Optional.empty())
    tpidsToBeFenced.foreach(tpid => addReplica(tpid, tierTopicConsumer))
    tierTopicConsumer.startConsume(true, tierTopic)
    TestUtils.waitUntilTrue(() => {
      tierPartitionStateFiles.forall(state => state.status == TierPartitionStatus.ERROR)
    }, "Timed out waiting for fencing")
    tierTopicConsumer.shutdown()
  }

  @Test
  def testFencingWithBadTopicIdPartitionFile(): Unit = {
    Utils.mkProperties(
      new HashMap[String, String] {
        put(KafkaConfig.TierMetadataBootstrapServersProp, brokerList)
        put(KafkaConfig.TierMetadataNamespaceProp, "")
      }
    ).store(new PrintWriter(propertiesConfFile), "")

    // 1. Empty TopicIdPartition file should cause ArgumentParserException to be raised.
    val emptyTopicIdPartitionsFile = TestUtils.tempFile
    assertThrows[ArgumentParserException] {
      kafka.tier.tools.TierPartitionStateFencingTrigger.runMain(Array(
        kafka.tier.tools.RecoveryUtils.makeArgument(
          kafka.tier.tools.RecoveryUtils.TIER_PROPERTIES_CONF_FILE_CONFIG),
        propertiesConfFile.getPath,
        kafka.tier.tools.RecoveryUtils.makeArgument(
          kafka.tier.tools.TierPartitionStateFencingTrigger.FILE_FENCE_TARGET_PARTITIONS_CONFIG),
        emptyTopicIdPartitionsFile.getPath))
    }

    // 2. Badly formatted TopicIdPartition file should cause ArgumentParserException to be raised.
    val badTopicIdPartitionsFile = TestUtils.tempFile
    val pw = new PrintWriter(badTopicIdPartitionsFile)
    // Third field is intentionally missing in the printed line
    pw.write("%s,%s".format("abc", "def"))
    pw.println()
    pw.close
    assertThrows[ArgumentParserException] {
      kafka.tier.tools.TierPartitionStateFencingTrigger.runMain(Array(
        kafka.tier.tools.RecoveryUtils.makeArgument(
          kafka.tier.tools.RecoveryUtils.TIER_PROPERTIES_CONF_FILE_CONFIG),
        propertiesConfFile.getPath,
        kafka.tier.tools.RecoveryUtils.makeArgument(
          kafka.tier.tools.TierPartitionStateFencingTrigger.FILE_FENCE_TARGET_PARTITIONS_CONFIG),
        badTopicIdPartitionsFile.getPath))
    }
  }

  @Test
  def testFencingWithBadPropertiesFile(): Unit = {
    // 1. Bad properties file path should cause ArgumentParserException to be raised.
    assertThrows[ArgumentParserException] {
      kafka.tier.tools.TierPartitionStateFencingTrigger.runMain(Array(
        kafka.tier.tools.RecoveryUtils.makeArgument(
          kafka.tier.tools.RecoveryUtils.TIER_PROPERTIES_CONF_FILE_CONFIG),
        "non-existing-file",
        kafka.tier.tools.RecoveryUtils.makeArgument(
          kafka.tier.tools.TierPartitionStateFencingTrigger.FILE_FENCE_TARGET_PARTITIONS_CONFIG),
        topicIdPartitionsFile.getPath))
    }

    // 2. Empty properties file (without required properties) should cause ArgumentParserException
    // to be raised.
    assertThrows[ArgumentParserException] {
      kafka.tier.tools.TierPartitionStateFencingTrigger.runMain(Array(
        kafka.tier.tools.RecoveryUtils.makeArgument(
          kafka.tier.tools.RecoveryUtils.TIER_PROPERTIES_CONF_FILE_CONFIG),
        propertiesConfFile.getPath,
        kafka.tier.tools.RecoveryUtils.makeArgument(
          kafka.tier.tools.TierPartitionStateFencingTrigger.FILE_FENCE_TARGET_PARTITIONS_CONFIG),
        topicIdPartitionsFile.getPath))
    }
  }
}
