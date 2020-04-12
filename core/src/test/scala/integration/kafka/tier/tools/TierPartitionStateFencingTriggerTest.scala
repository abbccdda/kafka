/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tools

import kafka.api.IntegrationTestHarness
import kafka.log.Log
import kafka.server.Defaults
import kafka.server.LogDirFailureChannel
import kafka.tier.TierTopicManagerCommitter
import kafka.tier.client.TierTopicConsumerSupplier
import kafka.tier.domain.AbstractTierMetadata
import kafka.tier.domain.TierRecordType
import kafka.tier.domain.TierSegmentDeleteInitiate
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.state.{FileTierPartitionState, OffsetAndEpoch, TierPartitionStatus}
import kafka.tier.topic.TierTopicManagerConfig
import kafka.tier.topic.TierTopic
import kafka.tier.topic.TierTopicAdmin
import kafka.tier.topic.TierTopicConsumer
import kafka.tier.topic.TierTopicManager
import kafka.utils.TestUtils
import kafka.zk.AdminZkClient
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.utils.Utils
import org.junit.{After, Test}
import org.junit.Assert._
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import java.io.File
import java.util.Collections
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier

import kafka.tier.TopicIdPartition

class TierPartitionStateFencingTriggerTest extends IntegrationTestHarness {
  override protected def brokerCount: Int = 3

  private val logDirFailureChannel = new LogDirFailureChannel(10)
  private val logDir = TestUtils.tempDir().getAbsolutePath
  private var tierPartitionStateFiles: Array[FileTierPartitionState] = Array()

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

  @After
  def teardown(): Unit = {
    tierPartitionStateFiles.foreach { tierPartitionState =>
      tierPartitionState.close()
      tierPartitionState.delete()
    }

    Utils.delete(new File(logDir))
  }

  @Test
  def testDeleteInitiateEventInjectionAndFencing(): Unit = {
    // 1. Setup
    val tierTopicNamespace = ""
    val numTierTopicPartitions: Short = 19
    val tierTopicReplicationFactor = 3
    val tieredTopicIdPartition = new TopicIdPartition(
      "dummy",
      UUID.fromString("021516db-7a5f-40ef-adda-b6e2b21a3e83"),
      123
    )
    val deleteInitiateEventEpoch = Int.MaxValue

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

    // 3. Trigger fencing on a user partition, by injecting a delete initiate event into
    // the corresponding TierTopic partition.
    kafka.tier.tools.TierPartitionStateFencingTrigger.main(Array(
      "--bootstrap-servers",
      brokerList,
      "--tiered-partition-name",
      tieredTopicIdPartition.partition().toString(),
      "--tiered-partition-topic-name",
      tieredTopicIdPartition.topic(),
      "--tiered-partition-topic-id",
      tieredTopicIdPartition.topicId().toString(),
      "--tier-topic-namespace",
      tierTopicNamespace,
      "dangerous-fence-via-delete-event"
    ));

    // 4. Using a dummy consumer, verify that exactly 1 TierSegmentDeleteInitiate event was written
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
    val tierTopicPartitions = TierTopicManager.partitions(tierTopic.topicName(), tierTopic.numPartitions().getAsInt());
    verificationConsumer.assign(tierTopicPartitions);
    tierTopicPartitions.forEach(tp => verificationConsumer.seekToBeginning(Collections.singletonList(tp)))
    val records = verificationConsumer.poll(config.pollDuration)
    assertEquals(1, records.count)
    assertTrue(records.iterator().hasNext)
    val record = records.iterator().next()
    val eventOpt: Optional[AbstractTierMetadata]
      = AbstractTierMetadata.deserialize(record.key(), record.value());
    assertTrue(eventOpt.isPresent)
    assertEquals(TierRecordType.SegmentDeleteInitiate, eventOpt.get.`type`())
    val deleteEvent = eventOpt.get().asInstanceOf[TierSegmentDeleteInitiate]
    assertEquals(tieredTopicIdPartition, deleteEvent.topicIdPartition())
    assertEquals(deleteInitiateEventEpoch, deleteEvent.tierEpoch())
    verificationConsumer.close

    // 5. Using a TierTopicConsumer, consume the injected TierSegmentDeleteInitiate and verify
    // that the materializer gets fenced.
    val tierTopicConsumer = new TierTopicConsumer(
      config,
      primaryConsumerSupplier,
      new TierTopicConsumerSupplier(config, "catchup"),
      new TierTopicManagerCommitter(config, logDirFailureChannel),
      Optional.empty())
    addReplica(tieredTopicIdPartition, tierTopicConsumer)
    tierTopicConsumer.startConsume(true, tierTopic)
    TestUtils.waitUntilTrue(() => {
      tierPartitionStateFiles(0).status == TierPartitionStatus.ERROR
    }, "Timed out waiting for fencing")
    tierTopicConsumer.shutdown()
  }
}
 