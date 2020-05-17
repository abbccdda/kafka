package kafka.server

import java.util.Properties

import kafka.log.{LogSegment, TierLogSegment}
import kafka.log.AbstractLog
import kafka.server.epoch.EpochEntry
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfluentTopicConfig, TopicConfig}
import org.junit.Assert.assertEquals
import org.junit.{Before, Test}
import scala.jdk.CollectionConverters._

import scala.collection.mutable

class TieredPartitionRecoveryTest extends BaseRequestTest {
  override def brokerCount: Int = 5 // Need 5 servers because at one time we shut down 2 of these
  val topicName: String = "tiered-topic"
  val topicPartition = new TopicPartition(topicName, 0)
  val props = new Properties

  serverConfig.put(KafkaConfig.TierEnableProp, "true")
  serverConfig.put(KafkaConfig.TierFeatureProp, "true")
  serverConfig.put(KafkaConfig.TierBackendProp, "mock")
  serverConfig.put(KafkaConfig.TierS3BucketProp, "mybucket")
  serverConfig.put(KafkaConfig.TierLocalHotsetBytesProp, "0")
  serverConfig.put(KafkaConfig.TierMetadataNumPartitionsProp, "1")
  serverConfig.put(KafkaConfig.TierMetadataReplicationFactorProp, "3")
  serverConfig.setProperty(KafkaConfig.LogFlushSchedulerIntervalMsProp, "10")
  serverConfig.setProperty(KafkaConfig.TierPartitionStateCommitIntervalProp, "10")
  serverConfig.setProperty(KafkaConfig.LogCleanupIntervalMsProp, "10")

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    serverConfig.stringPropertyNames().forEach(key => properties.put(key, serverConfig.get(key)))
  }

  @Before
  def prepareForTest(): Unit = {
    props.clear()
    props.put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "true")
    props.put(TopicConfig.SEGMENT_BYTES_CONFIG, "16384") // segment size: 16 KB
    props.put(ConfluentTopicConfig.TIER_LOCAL_HOTSET_BYTES_CONFIG, "16384") // hot set size: 16KB
    props.put(ConfluentTopicConfig.TIER_LOCAL_HOTSET_MS_CONFIG, "-1")
    props.put(TopicConfig.RETENTION_BYTES_CONFIG, "-1")
    props.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
  }

  @Test
  def testRecoveryAtUncleanLeaderWithNoDataAtLocalLog(): Unit = {
    // Recover tiered partition at an unclean leader that had no data at local log to begin with
    TestUtils.waitUntilControllerElected(zkClient)
    val (replica1Id, replica1, _, replica2) = createTestTopic(props)
    // shutdown follower and wait for the ISR to shrink
    replica2.shutdown()
    waitForIsrToChangeTo(leader = replica1, Set(replica1Id))
    TestUtils.waitUntilTrue(() =>
      !replica1.replicaManager.getPartitionOrException(topicPartition, expectLeader = true).getIsUncleanLeader,
      "Waiting for log to be clean", 120000)
    // produce records and wait for segments to tier
    val numMessages = 26000
    TestUtils.generateAndProduceMessages(servers.toSeq, topicName, numMessages)
    val log = replica1.replicaManager.getLog(topicPartition).get
    TestUtils.waitUntilTrue(() =>
      log.logEndOffset == numMessages &&
        tierLogSegments(log).size >= log.numberOfSegments - 1,
      "Timeout waiting for all messages to be written", 120000)
    val expectedLogStartOffset = log.logStartOffset
    val lastTieredOffset = tierLogSegments(log).last.endOffset
    // shutdown leader, and then start the follower which will become an unclean leader.
    replica1.shutdown()
    replica2.startup()
    // wait for recovery to complete and verify the log start and end offsets
    waitForReplicaToBeLeader(topicPartition, newLeader = replica2)
    val log_2 = replica2.replicaManager.getLog(topicPartition).get
    assertEquals("Post recovery, LogStartOffset does not match first tiered offset", expectedLogStartOffset, log_2.logStartOffset)
    assertEquals("Post recovery, LogEndOffset does not match last tiered offset", lastTieredOffset + 1, log_2.logEndOffset)
    assertEquals("Post recovery, LocalLogStartOffset does not match last tiered offset", lastTieredOffset + 1, log_2.localLogStartOffset)
    assertEquals("Post recovery, LocalLogEndOffset does not match last tiered offset", lastTieredOffset + 1, log_2.localLogEndOffset)
  }

  @Test
  def testRecoveryAtUncleanLeaderWithLocalLEOLessThanLastTieredOffset(): Unit = {
    // Recover tiered partition at an unclean leader that has some data at local log but not the last
    // tiered segment. There is no divergence. The local log must get discarded.
    // Essentially, localLogEndOffset at unclean leader < lastTieredOffset
    val (replica1Id, replica1, replica2Id, replica2) = createTestTopic(props)
    // produce a first batch of records. wait for both replicas to sync and all but active segments to get tiered
    val numMessages = 1000
    appendMessagesAndVerifySync(numMessages, leaderId = replica1Id, followerId = replica2Id)
    // shutdown follower and wait for the ISR to shrink
    replica2.shutdown()
    waitForIsrToChangeTo(leader = replica1, Set(replica1Id))
    // produce more records and wait for some more segments to get tiered
    TestUtils.generateAndProduceMessages(servers.toSeq, topicName, numMessages)
    val log = replica1.replicaManager.getLog(topicPartition).get
    TestUtils.waitUntilTrue(() =>
      (log.logEndOffset == 2 * numMessages) &&
        tierLogSegments(log).size >= log.numberOfSegments - 1,
      "Timeout waiting for some segments to tier", 120000)
    val firstTieredOffset = tierLogSegments(log).head.startOffset
    val lastTieredOffset = tierLogSegments(log).last.endOffset
    // shutdown leader, and then start the follower which will become an unclean leader.
    replica1.shutdown()
    replica2.startup()
    // wait for recovery to complete, and then verify log start and end offsets
    waitForReplicaToBeLeader(topicPartition, newLeader = replica2)
    val log_2 = replica2.replicaManager.getLog(topicPartition).get
    assertEquals("Post recovery, LogStartOffset does not match first tiered offset", firstTieredOffset, log_2.logStartOffset)
    assertEquals("Post recovery, LogEndOffset does not match last tiered segment", lastTieredOffset + 1, log_2.logEndOffset)
    assertEquals("Post recovery, LocalLogStartOffset does not match last tiered segment", lastTieredOffset + 1, log_2.localLogStartOffset)
    assertEquals("Post recovery, LocalLogEndOffset does not match last tiered segment", lastTieredOffset + 1, log_2.localLogEndOffset)
  }

  @Test
  def testRecoveryAtUncleanLeaderWithAllDataAtLocalLog(): Unit = {
    // Recover tiered partition at an unclean leader that has all the data at local log. This simulates a case when
    // followers may be kicked out of ISR because of a slow disk at the leader. No data loss in this scenario.
    // Note: set hot set equal to retention for this test. This ensures local log at follower does not increment in a
    // non-deterministic manner owing to local hot set config.
    props.put(ConfluentTopicConfig.TIER_LOCAL_HOTSET_BYTES_CONFIG, "-1")
    props.put(ConfluentTopicConfig.TIER_LOCAL_HOTSET_MS_CONFIG, "-1")
    props.put(TopicConfig.RETENTION_BYTES_CONFIG, "-1")
    TestUtils.waitUntilControllerElected(zkClient)
    val (replica1Id, replica1, replica2Id, replica2) = createTestTopic(props)
    // produce enough records and wait for some of the segments to get tiered
    val numMessages = 10000
    appendMessagesAndVerifySync(numMessages, leaderId = replica1Id, followerId = replica2Id)
    val log = replica1.replicaManager.getLog(topicPartition).get
    val expectedLogStartOffset = log.logStartOffset
    val expectedLogEndOffset = log.logEndOffset
    val expectedLocalLogStartOffset = replica2.replicaManager.getLog(topicPartition).get.localLogStartOffset
    // shutdown follower and wait for the ISR to shrink
    replica2.shutdown()
    waitForIsrToChangeTo(leader = replica1, Set(replica1Id))
    // shutdown leader, and then start the follower which will become an unclean leader.
    replica1.shutdown()
    replica2.startup()
    // wait for recovery to complete and verify the log start and end offsets
    waitForReplicaToBeLeader(topicPartition, newLeader = replica2)
    val log_2 = replica2.replicaManager.getLog(topicPartition).get
    assertEquals("Unexpected LogStartOffset after recovery", expectedLogStartOffset, log_2.logStartOffset)
    assertEquals("Unexpected LogEndOffset after recovery", expectedLogEndOffset, log_2.logEndOffset)
    assertEquals("Unexpected LocalLogStartOffset after recovery", expectedLocalLogStartOffset, log_2.localLogStartOffset)
    assertEquals("Unexpected LocalLogEndOffset after recovery", expectedLogEndOffset, log_2.localLogEndOffset)
  }

  @Test
  def testRecoveryAtUncleanLeaderWithLSOOlderThanFirstTieredOffset(): Unit = {
    // Recover tiered partition after an unclean leader election. Local log at the unclean leader is in following state:
    // 1. LocalLogStartOffset < first tiered offset
    // 2. LocalLogEndOffset >= last tiered offset
    // 2. No divergence between local log epoch cache and the tiered leader epoch state
    // Expected Result:
    // 1. Local log should not be discarded.
    // 2. Local log start offset must be incremented to the first tiered offset
    props.put(TopicConfig.SEGMENT_BYTES_CONFIG, "4096") // segment size: 4 KB
    props.put(ConfluentTopicConfig.TIER_LOCAL_HOTSET_BYTES_CONFIG, "40960") // hotset = retention to keep all data local
    props.put(TopicConfig.RETENTION_BYTES_CONFIG, "40960")
    props.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, "100")
    val (replica1Id, replica1, replica2Id, replica2) = createTestTopic(props)
    // produce enough records and wait for some of the segments to get tiered and replicas to sync
    val log = replica1.replicaManager.getLog(topicPartition).get
    val numMessages = 50
    while (log.size < 40960) {
      appendMessagesAndVerifySync(numMessages, leaderId = replica1Id, followerId = replica2Id)
    }
    val oldLocalLogStartOffsetFollower = replica2.replicaManager.getLog(topicPartition).get.localLogStartOffset
    // shutdown follower and wait for ISR to shrink
    replica2.shutdown()
    waitForIsrToChangeTo(leader = replica1, Set(replica1Id))
    // reduce log retention size (bytes), so that some of the tiered segments get deleted
    props.put(ConfluentTopicConfig.TIER_LOCAL_HOTSET_BYTES_CONFIG, "20480")
    props.put(TopicConfig.RETENTION_BYTES_CONFIG, "20480")
    adminZkClient.changeConfigs(ConfigType.Topic, topicName, props)
    // wait for retention to complete
    var sizeInBytes = 0
    val offsetAndSize = mutable.SortedSet[(Long, Int)]()
    log.localLogSegments.iterator.foreach((segment: LogSegment) => { offsetAndSize.add((segment.baseOffset, segment.size)) })
    tierLogSegments(log).iterator.foreach((segment: TierLogSegment) => { offsetAndSize.add((segment.baseOffset, segment.size)) })
    var newStartOffset = log.logEndOffset
    val it = offsetAndSize.toList.reverseIterator
    while (sizeInBytes < 20480 && it.hasNext) {
      val (baseOffset, segmentSize) = it.next
      sizeInBytes += segmentSize
      newStartOffset = baseOffset
    }
    TestUtils.waitUntilTrue(() =>
      log.logStartOffset == newStartOffset &&
      tierLogSegments(log).head.baseOffset == newStartOffset,
      s"Timed out waiting for retention to complete", 120000)
    val firstTieredOffset = tierLogSegments(log).head.baseOffset
    val expectedLogEndOffset = log.logEndOffset
    val lastTieredOffset = tierLogSegments(log).last.endOffset
    // shutdown leader and then startup follower which will be elected as an unclean leader
    replica1.shutdown()
    replica2.startup()
    // wait for the unclean leader to complete recovery
    waitForReplicaToBeLeader(topicPartition, newLeader = replica2)
    // verify the log start and end offsets
    val log_2 = replica2.replicaManager.getLog(topicPartition).get
    assertEquals("Post recovery, LogStartOffset does not match first tiered offset", firstTieredOffset, log_2.logStartOffset)
    assert(log_2.localLogStartOffset > oldLocalLogStartOffsetFollower, "LocalLogStartOffset has not incremented to match first tiered offset")
    assertEquals("Unexpected LogEndOffset after recovery", expectedLogEndOffset, log_2.logEndOffset)
    assertEquals("Unexpected LocalLogStartOffset after recovery", firstTieredOffset, log_2.localLogStartOffset)
    assert(log_2.localLogEndOffset >= lastTieredOffset,
      s"LocalLogEndOffset ${log_2.localLogEndOffset} is lesser than the last tiered offset ${lastTieredOffset} after recovery")
  }

  @Test
  def testRecoveryAtUncleanLeaderWithDivergenceAtLocalLog(): Unit = {
    // Recover tiered partition at an unclean leader that has divergence at local log. Local log must be discarded.
    TestUtils.waitUntilControllerElected(zkClient)
    val (replica1Id, replica1, replica2Id, replica2) = createTestTopic(props)
    val numMessages = 5000
    // produce records from first leader
    appendMessagesAndVerifySync(numMessages, leaderId = replica1Id, followerId = replica2Id)
    // elect a new leader; append more records from second leader to build more than one entry at epoch cache
    replica1.shutdown()
    waitForReplicaToBeLeader(topicPartition, newLeader = replica2)
    replica1.startup()
    waitForIsrToChangeTo(leader = replica2, Set(replica1Id, replica2Id))
    // produce records from second leader
    appendMessagesAndVerifySync(numMessages, leaderId = replica2Id, followerId = replica1Id)
    // edit leader epoch cache at the follower
    val followerLog = replica1.replicaManager.getLog(topicPartition).get
    val editedEpochEntries = followerLog.leaderEpochCache.get.epochEntries.collect {
      case entry => EpochEntry(entry.epoch, entry.startOffset + 1)
    }
    followerLog.leaderEpochCache.get.clearAndFlush()
    editedEpochEntries.foreach(entry => followerLog.leaderEpochCache.get.assign(entry.epoch, entry.startOffset))
    val leaderLog = replica2.replicaManager.getLog(topicPartition).get
    val expectedLogStartOffset = leaderLog.logStartOffset
    val lastTieredOffset = tierLogSegments(leaderLog).last.endOffset
    // shutdown follower and wait for the ISR to shrink
    replica1.shutdown()
    waitForIsrToChangeTo(leader = replica2, Set(replica2Id))
    // shutdown leader, and then start the follower which will become an unclean leader.
    replica2.shutdown()
    replica1.startup()
    waitForReplicaToBeLeader(topicPartition, newLeader = replica1)
    val log = replica1.replicaManager.getLog(topicPartition).get
    assertEquals("Unexpected LogStartOffset after recovery", expectedLogStartOffset, log.logStartOffset)
    assertEquals("Unexpected LogEndOffset after recovery", lastTieredOffset + 1, log.logEndOffset)
    assertEquals("Unexpected LocalLogStartOffset after recovery", lastTieredOffset + 1, log.localLogStartOffset)
    assertEquals("Unexpected LocalLogEndOffset after recovery", lastTieredOffset + 1, log.localLogEndOffset)
  }

  @Test
  def testRecoveryAtUncleanLeaderWhenNoSegmentHasBeenTiered(): Unit = {
    // Recover local log after unclean leader election when no data has been tiered yet
    TestUtils.waitUntilControllerElected(zkClient)
    val (replica1Id, replica1, _, replica2) = createTestTopic(props)
    // shutdown follower and wait for the ISR to shrink
    replica2.shutdown()
    waitForIsrToChangeTo(leader = replica1, Set(replica1Id))
    val log = replica1.replicaManager.getLog(topicPartition).get
    assert(log.logStartOffset == 0 && log.logEndOffset == 0 && log.tierPartitionState.numSegments() == 0)
    // shutdown leader, and then start the follower which will become an unclean leader.
    replica1.shutdown()
    replica2.startup()
    // wait for recovery to complete and verify the log start and end offsets
    waitForReplicaToBeLeader(topicPartition, newLeader = replica2)
    val log_2 = replica2.replicaManager.getLog(topicPartition).get
    assertEquals("Post recovery, LogStartOffset does not match first tiered offset", 0L, log_2.logStartOffset)
    assertEquals("Post recovery, LogEndOffset does not match last tiered offset", 0L, log_2.logEndOffset)
    assertEquals("Post recovery, LocalLogStartOffset does not match last tiered offset", 0L, log_2.localLogStartOffset)
    assertEquals("Post recovery, LocalLogEndOffset does not match last tiered offset", 0L, log_2.localLogEndOffset)
  }

  @Test
  def testRecoveryAtUncleanLeaderWithTieringDisabled(): Unit = {
    // Recover non-tiered partition after an unclean leader election. New leader's local log must become the accepted
    // state of the log.
    props.put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "false")
    val (replica1Id, replica1, replica2Id, replica2) = createTestTopic(props)
    // produce a first batch of records. wait for both replicas to sync
    val numMessages = 1000
    val log = serverForId(replica1Id).get.replicaManager.getLog(topicPartition).get
    val followerLog = serverForId(replica2Id).get.replicaManager.getLog(topicPartition).get
    TestUtils.generateAndProduceMessages(servers.toSeq, topicName, numMessages)
    TestUtils.waitUntilTrue(() =>
      log.logEndOffset == numMessages && followerLog.logEndOffset == numMessages,
      "Timeout waiting for all messages to be written and synced", 120000)
    // shutdown follower and wait for the ISR to shrink
    val expectedLogStartOffset = followerLog.logStartOffset
    val expectedLogEndOffset = followerLog.logEndOffset
    replica2.shutdown()
    waitForIsrToChangeTo(leader = replica1, Set(replica1Id))
    // produce more records
    TestUtils.generateAndProduceMessages(servers.toSeq, topicName, numMessages)
    TestUtils.waitUntilTrue(() => log.logEndOffset == 2 * numMessages, "Timeout waiting for all messages to be written", 120000)
    // shutdown leader, and then start the follower which will become an unclean leader.
    replica1.shutdown()
    replica2.startup()
    // wait for recovery to complete, and then verify log start and end offsets
    waitForReplicaToBeLeader(topicPartition, newLeader = replica2)
    val log_2 = replica2.replicaManager.getLog(topicPartition).get
    assertEquals("Post recovery, LogStartOffset does not match first tiered offset", expectedLogStartOffset, log_2.logStartOffset)
    assertEquals("Post recovery, LogEndOffset does not match last tiered segment", expectedLogEndOffset, log_2.logEndOffset)
    assertEquals("Post recovery, LocalLogStartOffset does not match last tiered segment", expectedLogStartOffset, log_2.localLogStartOffset)
    assertEquals("Post recovery, LocalLogEndOffset does not match last tiered segment", expectedLogEndOffset, log_2.localLogEndOffset)
  }

  private def tierLogSegments(log: AbstractLog): List[TierLogSegment] = {
    val iterator = log.tieredLogSegments
    try {
      iterator.asScala.toList
    } finally {
      iterator.close()
    }
  }

  private def createTestTopic(props: Properties): (Int, KafkaServer, Int, KafkaServer) = {
    val partitionToLeaderMap = createTopic(topicName, 1, 2, props)
    val leaderId = partitionToLeaderMap(topicPartition.partition())
    val leader = serverForId(leaderId).get
    val followerId = TestUtils.findFollowerId(topicPartition, servers)
    val follower = serverForId(followerId).get
    (leaderId, leader, followerId, follower)
  }

  private def appendMessagesAndVerifySync(numMessages: Int, leaderId: Int, followerId: Int): Unit = {
    val leaderLog = serverForId(leaderId).get.replicaManager.getLog(topicPartition).get
    val followerLog = serverForId(followerId).get.replicaManager.getLog(topicPartition).get
    val currentEndOffset = leaderLog.logEndOffset
    TestUtils.generateAndProduceMessages(servers.toSeq, topicName, numMessages)
    TestUtils.waitUntilTrue(() =>
      leaderLog.logEndOffset == currentEndOffset + numMessages &&
        leaderLog.logEndOffset == followerLog.logEndOffset &&
        tierLogSegments(leaderLog).size >= leaderLog.numberOfSegments - 1,
      "Timeout waiting for all messages to be written, synced and tiered", 120000)
  }

  private def waitForIsrToChangeTo(leader: KafkaServer, expectedIsr: Set[Int]): Unit = {
    val partition = leader.replicaManager.getPartitionOrException(topicPartition, expectLeader = true)
    TestUtils.waitUntilTrue(() =>
      partition.inSyncReplicaIds.equals(expectedIsr),
      s"Timeout waiting for ISR to change to ${expectedIsr}", 120000)
  }

  private def waitForReplicaToBeLeader(topicPartition: TopicPartition, newLeader: KafkaServer): Unit = {
    TestUtils.waitUntilTrue(() =>
      newLeader.replicaManager.allPartitions.contains(topicPartition),
      s"Timed out waiting for partition object at new leader", 120000)
    val partition = newLeader.replicaManager.getPartitionOrException(topicPartition, expectLeader = true)
    TestUtils.waitUntilTrue(() =>
      partition.isLeader &&
        !partition.getIsUncleanLeader,
      "Timed out waiting for leader to change and log to be recovered, if needed", 120000)
  }
}