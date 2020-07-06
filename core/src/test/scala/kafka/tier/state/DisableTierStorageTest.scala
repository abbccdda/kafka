/*
 Copyright 2020 Confluent Inc.
 */
package kafka.tier.state

import java.util
import java.util.concurrent.TimeUnit
import java.util.{Collections, Optional, Properties}

import kafka.admin.ReassignPartitionsCommand._
import kafka.log.{AbstractLog, TierLogSegment}
import kafka.server.{BaseRequestTest, KafkaConfig}
import kafka.tier.store.TierObjectStore
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfigResource, ConfluentTopicConfig, TopicConfig}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse}
import org.junit.Assert.assertEquals
import org.junit.{Assert, Before, Test}

import scala.collection.Map
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class DisableTierStorageTest extends BaseRequestTest {
  override def brokerCount: Int = 5 // Need 5 servers because at one time we shut down 2 of these
  val topicName: String = "foo"
  val topicPartition = new TopicPartition(topicName, 0)
  val props = new Properties

  serverConfig.put(KafkaConfig.TierFeatureProp, "true")
  serverConfig.put(KafkaConfig.TierBackendProp, "mock")
  serverConfig.put(KafkaConfig.TierS3BucketProp, "mybucket")
  serverConfig.put(KafkaConfig.TierLocalHotsetBytesProp, "0")
  serverConfig.put(KafkaConfig.TierMetadataNumPartitionsProp, "1")
  serverConfig.put(KafkaConfig.LogFlushSchedulerIntervalMsProp, "10")
  serverConfig.put(KafkaConfig.TierPartitionStateCommitIntervalProp, "10")
  serverConfig.put(KafkaConfig.LogCleanupIntervalMsProp, "10")
  serverConfig.put(KafkaConfig.TierTopicDeleteCheckIntervalMsProp, "10")
  serverConfig.put(KafkaConfig.TierMetadataMaxPollMsProp, "10")
  serverConfig.put(KafkaConfig.AutoLeaderRebalanceEnableProp, "false")

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    serverConfig.stringPropertyNames().forEach(key => properties.put(key, serverConfig.get(key)))
  }

  @Before
  def prepareForTest(): Unit = {
    props.clear()
    props.put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "true")
    props.put(TopicConfig.SEGMENT_BYTES_CONFIG, "2048") // segment size: 4 KB
    props.put(ConfluentTopicConfig.TIER_LOCAL_HOTSET_BYTES_CONFIG, "2048")
    props.put(ConfluentTopicConfig.TIER_LOCAL_HOTSET_MS_CONFIG, "10") // test default: local hot set retention = 10 ms
    props.put(TopicConfig.RETENTION_BYTES_CONFIG, "20480")
    props.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.1")
    props.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, "10")
  }

  @Test
  def testFetchRequestWithTieredStorageDisabled(): Unit = {
    // Disable tiered storage when some data has been tiered and make sure that fetch requests to tiered section of log
    // are honored by the current leader and upon subsequent leader election.
    val (replica1, replica2) = createTopicAndGetReplicas()
    // Append messages and fetch before setting tier.enable to false
    appendMessagesAndWaitUntilTieredAndSynced(leaderId = replica1, followerId = replica2)
    waitForHotSetRetentionToKickIn(List(replica1, replica2))
    debug(s"Fetch with tier enabled")
    fetchTieredAndLocalDataAndValidate(leaderId = replica1, followerIdOpt = Some(replica2))
    // Disable tiered storage and fetch (Case: Leader can fetch tiered data after tier.enable is set to false)
    changeTopicConfig(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "false")
    val leaderLog = serverForId(replica1).get.replicaManager.getLog(topicPartition).get
    TestUtils.waitUntilTrue(() => !leaderLog.tierPartitionState.isTieringEnabled, "Timed out waiting for tiered storage to be turned off", 30000)
    debug(s"Fetch with tier disabled")
    fetchTieredAndLocalDataAndValidate(leaderId = replica1, followerIdOpt = Some(replica2))
    // Elect a new leader and fetch (Case: Replica becomes leader and can fetch tiered data)
    switchLeader(currentLeaderId = replica1, currentFollowerId = replica2)
    debug(s"Fetch with tier disabled and new leader elected")
    fetchTieredAndLocalDataAndValidate(leaderId = replica2, followerIdOpt = Some(replica1))
    // Switch back to original leader and fetch (Case: Replica becomes leader after roll and can fetch tiered data)
    switchLeader(currentLeaderId = replica2, currentFollowerId = replica1)
    debug(s"Fetch with tier disabled and new leader elected (that has since been rolled)")
    fetchTieredAndLocalDataAndValidate(leaderId = replica1, followerIdOpt = Some(replica2))
  }

  @Test
  def testAddNewReplicaWithTieredStorageDisabled(): Unit = {
    // As a new replica is added to a partition it needs to open the TierPartitionState channel and materialize till the
    // leader's local log start offset. If tier.enable is set to true, the channel is opened at the time of MergedLog creation;
    // else channel has to be opened in the replication path while handling OFFSET_TIERED exception from the leader.
    val (replica1, _) = createTopicAndGetReplicas()
    appendMessagesToLeaderAndWaitUntilTiered(leaderId = replica1)
    // Make sure that all data is not local, otherwise replicas being added will not get OFFSET_TIERED exception.
    val log = serverForId(replica1).get.replicaManager.getLog(topicPartition).get
    TestUtils.waitUntilTrue(() => log.logStartOffset < log.localLogStartOffset,
      s"Timed out waiting for hot set retention to kick in logStartOffset: ${log.logStartOffset} localLogStartOffset ${log.localLogStartOffset}", 60000)
    // Add a replica to assignment. This replica will open TierPartitionState channel and begin materialization while
    // creating MergedLog as config `tier.enable = true` for the partition.
    val replica3 = addReplicaToAssignment(leaderId = replica1)
    waitForReplicaToGetInSync(leaderId = replica1, followerId = replica3)
    // Disable tiered storage for the partition under test
    changeTopicConfig(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "false")
    TestUtils.waitUntilTrue(() => !log.tierPartitionState.isTieringEnabled, "Timed out waiting for tiered storage to be turned off", 30000)
    // Add a new replica to the assignment. It will need to open TierPartitionState channel as it gets OFFSET_TIERED exception
    val replica4 = addReplicaToAssignment(leaderId = replica1)
    waitForReplicaToGetInSync(leaderId = replica1, followerId = replica4)
  }

  @Test
  def testReplicationWithTieredStorageDisabled(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)
    val partitionToLeaderMap = createTopic(topicName, 1, 3, props)
    val replica1 = partitionToLeaderMap(topicPartition.partition())
    val leaderPart = serverForId(replica1).get.replicaManager.getPartitionOrException(topicPartition, expectLeader = true)
    val followers = leaderPart.assignmentState.replicas.filter(_ != replica1)
    followers.foreach(follower => serverForId(follower).get.shutdown())
    appendMessagesToLeaderAndWaitUntilTiered(leaderId = replica1)
    // Make sure that all data is not local, otherwise replicas will not get OFFSET_TIERED exception.
    val log = serverForId(replica1).get.replicaManager.getLog(topicPartition).get
    TestUtils.waitUntilTrue(() => log.logStartOffset < log.localLogStartOffset,
      s"Timed out waiting for hot set retention to kick in logStartOffset: ${log.logStartOffset} localLogStartOffset ${log.localLogStartOffset}", 60000)
    // start a follower before disabling tiered storage
    serverForId(followers.head).get.startup()
    waitForReplicaToGetInSync(leaderId = replica1, followerId = followers.head)
    // Disable tiered storage for the partition under test
    changeTopicConfig(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "false")
    TestUtils.waitUntilTrue(() => !log.tierPartitionState.isTieringEnabled, "Timed out waiting for tiered storage to be turned off", 30000)
    // start a follower after disabling tiered storage
    serverForId(followers(1)).get.startup()
    waitForReplicaToGetInSync(leaderId = replica1, followerId = followers(1))
  }

  @Test
  def testDeleteTopicWithTieredStorageDisabled(): Unit = {
    TestUtils.waitUntilControllerElected(zkClient)
    val topic1 = "foo"
    val topic2 = "bar"
    // create test topics
    val topic1LeaderId = createTopic(topic1, 1, 1, props)(0)
    val topic1Log = serverForId(topic1LeaderId).get.replicaManager.getLog(new TopicPartition(topic1, 0)).get
    val topic2LeaderId = createTopic(topic2, 1, 1, props)(0)
    val topic2Log = serverForId(topic2LeaderId).get.replicaManager.getLog(new TopicPartition(topic2, 0)).get
    // append messages to each topic
    val numMessages = 100
    var totalMessages1 = 0
    while (topic1Log.numberOfSegments <= 3) {
      TestUtils.generateAndProduceMessages(servers.toSeq, topic1, numMessages)
      totalMessages1 += numMessages
    }
    var totalMessages2 = 0
    while (topic2Log.numberOfSegments <= 3) {
      TestUtils.generateAndProduceMessages(servers.toSeq, topic2, numMessages)
      totalMessages2 += numMessages
    }
    TestUtils.waitUntilTrue(() =>
      topic1Log.logEndOffset == totalMessages1 && tierLogSegments(topic1Log).size >= topic1Log.numberOfSegments - 1,
      "Timeout waiting for all messages to be written and tiered", 60000)
    TestUtils.waitUntilTrue(() =>
      topic2Log.logEndOffset == totalMessages2 && tierLogSegments(topic2Log).size >= topic2Log.numberOfSegments - 1,
      "Timeout waiting for all messages to be written and tiered", 60000)
    val tieredSegments1 = tierLogSegments(topic1Log)
    val tieredSegments2 = tierLogSegments(topic2Log)
    // delete topic1 (tier enabled)
    val adminClient = createAdminClient()
    val future1 = adminClient.deleteTopics(List(topic1).asJavaCollection)
    // delete topic2 (after disabling tiered storage for this topic)
    changeTopicConfig(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "false", topic2)
    TestUtils.waitUntilTrue(() => !topic2Log.tierPartitionState.isTieringEnabled, "Timed out waiting for tiered storage to be turned off", 30000, 200)
    val future2 = adminClient.deleteTopics(List(topic2).asJavaCollection)
    future1.all().get(30000, TimeUnit.MILLISECONDS)
    future2.all().get(30000, TimeUnit.MILLISECONDS)
    val objStore = serverForId(topic1LeaderId).get.tierObjectStoreOpt.get
    // verify that all tiered segments are deleted for both the topics
    verifyTieredSegmentsDeleted(topic1, objStore, tieredSegments1)
    verifyTieredSegmentsDeleted(topic2, objStore, tieredSegments2)
  }

  @Test
  def testLogRetentionWithTieredStorageDisabled(): Unit = {
    // Retention logic for the log follows a different path when `tierEnable == true`. With `tierEnable = true`, two kinds
    // of retention comes into picture, A: normal time/size based retention that works on the complete length of MergedLog.
    // B: time/size based retention for the local segments(hot set) of MergedLog. Note: There is a separate retention component
    // called DeletionTask (a type of TierTask state machine) that is responsible for deleting tiered segments of MergedLog.
    // With `tierEnable = false`, only the (A) from above comes into play.
    val numMessages = 100
    var topic: String = ""
    for ((configVal, index) <- List("false","true").zipWithIndex) {
      debug(s"Testing topic created with config.TierEnable = ${configVal}")
      props.put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, configVal)
      topic = topicName + "-" + index
      val leaderId = createTopic(topic, 1, 1, props)(0)
      val partition = serverForId(leaderId).get.replicaManager.getPartitionOrException(new TopicPartition(topic, 0), expectLeader = true)
      while (partition.log.get.numberOfSegments < 4)
        TestUtils.generateAndProduceMessages(servers.toSeq, topic, numMessages)
      // disable tiered storage after writing data, if it was enabled
      var expectedLogStartOffset = 0L
      var retentionSizeBytes = 0L
      if (configVal == "true") {
        TestUtils.waitUntilTrue(() => partition.log.get.tierPartitionState.numSegments() >= partition.log.get.numberOfSegments - 1 &&
          partition.log.get.localLogStartOffset > partition.log.get.tierPartitionState.startOffset().get(),
          "Timed out waiting for segments to be tiered and deleted from local storage", 30000)
        changeTopicConfig(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "false", topic)
        TestUtils.waitUntilTrue(() => !partition.log.get.tierPartitionState.isTieringEnabled, "Timed out waiting for tiering to disable", 30000)
        val tieredSegments = tierLogSegments(partition.log.get)
        retentionSizeBytes = partition.log.get.size - (tieredSegments.head.size + 1)
        expectedLogStartOffset = tieredSegments(1).baseOffset
      } else {
        retentionSizeBytes = partition.log.get.size - (partition.log.get.localLogSegments.head.size + 1)
        expectedLogStartOffset = partition.log.get.localLogSegments.toList(1).baseOffset
      }
      // change size based retention config such that the first segment gets deleted
      changeTopicConfig(TopicConfig.RETENTION_BYTES_CONFIG, retentionSizeBytes.toString, topic)
      TestUtils.waitUntilTrue(() => expectedLogStartOffset == partition.logStartOffset,
        s"Unexpected log start offset: ${partition.logStartOffset} Expected value: ${expectedLogStartOffset}", 60000, 10)
    }
  }

  @Test
  def testLogRetentionAfterTieredDataIsDeletedAndTieredStorageDisabled(): Unit = {
    // This test exercises MergedLog#deleteOldSegments path after tier storage has been disabled for a topic, and all
    // of the tiered data has been deleted. Above mentioned method is responsible for incrementing the log start offset and
    // deleting local log segments past retention.
    val numMessages = 100
    val leaderId = createTopic(topicName, 1, 1, props)(0)
    val log = serverForId(leaderId).get.getLogManager().getLog(new TopicPartition(topicName, 0)).get
    // append some data to the topic, and make sure some segments get tiered
    while(log.numberOfSegments < 4)
      TestUtils.generateAndProduceMessages(servers.toSeq, topicName, numMessages)
    TestUtils.waitUntilTrue(() => log.tierPartitionState.numSegments() >= log.numberOfSegments - 1 &&
      log.localLogStartOffset > log.tierPartitionState.startOffset().get(),
      "Timed out waiting for segments to be tiered and deleted from local storage", 30000)
    // disable tiered storage
    changeTopicConfig(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "false", topicName)
    TestUtils.waitUntilTrue(() => !log.tierPartitionState.isTieringEnabled, "Timed out waiting for tiering to disable", 30000)
    // keep track of log bounds, last tiered offset
    val currentNumMessages = log.logEndOffset
    val lastTieredOffset = log.tierPartitionState.endOffset()
    val desiredRetentionSizeBytes = log.size.toString
    while(log.logEndOffset < currentNumMessages * 3)
      TestUtils.generateAndProduceMessages(servers.toSeq, topicName, numMessages)
    val segmentsNotTiered = log.localLogSegments(lastTieredOffset + 1, log.logEndOffset).toList
    // change log retention bytes such that all tiered segments and at least one local segment gets deleted
    changeTopicConfig(TopicConfig.RETENTION_BYTES_CONFIG, desiredRetentionSizeBytes, topicName)
    TestUtils.waitUntilTrue(() => log.logStartOffset >= segmentsNotTiered(1).baseOffset,
      "Timed out waiting for retention to delete some local segments", 30000)
    TestUtils.waitUntilTrue(() => log.localLogStartOffset == log.logStartOffset,
      s"LogStartOffset ${log.logStartOffset} != localLogStartOffset ${log.localLogStartOffset}", 30000)
    TestUtils.waitUntilTrue(() => !log.tierPartitionState.startOffset().isPresent,
      "FirstTieredOffset is defined after all tiered segments are deleted", 60000)
  }

  @Test
  def testDeletionTaskStateMachineWhenTierStorageDisabled(): Unit = {
    // Verify that the tiered data gets deleted by the DeletionTask state machine as per log retention rules when tier
    // storage has been disabled.
    props.put(TopicConfig.RETENTION_BYTES_CONFIG, "-1")
    props.put(TopicConfig.RETENTION_MS_CONFIG, "-1")
    TestUtils.waitUntilControllerElected(zkClient)
    val leaderId = createTopic(topicPartition.topic(), 1, 1, props)(0)
    val log = serverForId(leaderId).get.replicaManager.getLog(topicPartition).get
    val objStore = serverForId(leaderId).get.tierObjectStoreOpt.get
    appendMessagesToLeaderAndWaitUntilTiered(leaderId)
    changeTopicConfig(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "false")
    TestUtils.waitUntilTrue(() => !log.tierPartitionState.isTieringEnabled, "Timed out waiting for tiered storage to be turned off", 10000)
    val tieredSegments = tierLogSegments(log)
    changeTopicConfig(TopicConfig.RETENTION_MS_CONFIG, "100")
    verifyTieredSegmentsDeleted(topicPartition.topic(), objStore, tieredSegments)
    TestUtils.waitUntilTrue(() => log.tierPartitionState.numSegments() == 0, "Timed out waiting for all tiered segments to delete", 10000)
  }

  private def tierLogSegments(log: AbstractLog): List[TierLogSegment] = {
    val iterator = log.tieredLogSegments
    try {
      iterator.asScala.toList
    } finally {
      iterator.close()
    }
  }

  private def waitForHotSetRetentionToKickIn(replicas: List[Int]): Unit = {
    replicas.foreach { replica =>
      val log = serverForId(replica).get.replicaManager.getLog(topicPartition).get
      TestUtils.waitUntilTrue(() => log.localLogStartOffset > log.logStartOffset, "Timed out waiting for hot set retention to kick in", 60000, 200)
    }
  }

  // verify that all files related to the list of tiered segments have been deleted at tiered storage
  private def verifyTieredSegmentsDeleted(topicName: String, objStore: TierObjectStore, tieredSegments: List[TierLogSegment]): Unit = {
    tieredSegments.foreach(segment => {
      var deleted: Boolean = false
      var retries = 0
      for (fileType <- TierObjectStore.FileType.values()) {
        deleted = false
        while (!deleted) {
          Try(objStore.getObject(segment.metadata, fileType)) match {
            case Success(_) =>
              debug(s"[${topicName}] ${segment.metadata.toString} ${fileType.toString} not deleted yet. Back off and retry")
              retries += 1
              Assert.assertTrue(s"[${topicName}] ${segment.metadata.toString} ${fileType.toString} not deleted in max retries", retries < 10)
              Thread.sleep(2000)
            case Failure(exception) =>
              Assert.assertEquals(s"[${topicName}] Unexpected exception while checking for ${segment.metadata.toString} ${fileType.toString}", classOf[java.io.IOException], exception.getClass)
              deleted = true
          }
        }
      }
    })
  }

  private def waitForReplicaToGetInSync(leaderId: Int, followerId: Int): Unit = {
    val leaderPart = serverForId(leaderId).get.replicaManager.getPartitionOrException(topicPartition, expectLeader = true)
    var loaded = false
    var numAttempts = 0
    while (!loaded && numAttempts < 25) {
      Try(serverForId(followerId).get.replicaManager.getPartitionOrException(topicPartition, expectLeader = false)) match {
        case Success(_) =>
          loaded = true
        case Failure(ex) =>
          debug(s"Exception returned: $ex while getting partition at follower. numAttempts: $numAttempts")
          Thread.sleep(200L)
          numAttempts += 1
      }
    }
    val followerPart = serverForId(followerId).get.replicaManager.getPartitionOrException(topicPartition, expectLeader = false)
    TestUtils.waitUntilTrue(() => leaderPart.inSyncReplicaIds.contains(followerId), s"Timed out waiting replica to get in sync", 30000)
    assertEquals("Log start offset mismatch", leaderPart.log.get.logStartOffset, followerPart.log.get.logStartOffset)
    assertEquals("First tiered offset mismatch", leaderPart.log.get.tierPartitionState.startOffset().get(), followerPart.log.get.tierPartitionState.startOffset().get())
    assertEquals("Last tiered offset mismatch", leaderPart.log.get.tierPartitionState.endOffset(), followerPart.log.get.tierPartitionState.endOffset())
    assertEquals("Log end offset mismatch", leaderPart.log.get.logEndOffset, followerPart.log.get.logEndOffset)
  }

  // Add a replica to the partition assignment and return the id of this replica
  private def addReplicaToAssignment(leaderId: Int): Int = {
    val currentAssignment = serverForId(leaderId).get.replicaManager.getPartitionOrException(topicPartition, expectLeader = true).inSyncReplicaIds.toSeq
    val adminClient = createAdminClient()
    val allNodeIds = adminClient.describeCluster().nodes().get().asScala.map(_.id).toSet
    val replicaToAdd = allNodeIds.find(x => !currentAssignment.contains(x)).getOrElse(-1)
    if (replicaToAdd == -1)
      throw new IllegalStateException("Cannot add a replica because # of brokers == # of replicas")
    val targetAssignment = currentAssignment :+ replicaToAdd
    val logDirs = List.fill(targetAssignment.size)("\"any\"")
    val reassignmentJson = """{"version":1,"partitions":""" +
      s"""[{"topic":"foo","partition":0,"replicas":${targetAssignment.mkString("[", ",", "]")},"log_dirs":${logDirs.mkString("[", ",", "]")}}""" +
      """]}"""
    // Run re-assignment
    executeAssignment(adminClient, additional = false, reassignmentJson, -1L, -1L)
    // Wait for assignment to change
    var latestResult: VerifyAssignmentResult = null
    val expectedResult = new VerifyAssignmentResult(Map(
      new TopicPartition("foo", 0) -> PartitionReassignmentState(Assignment(targetAssignment, Seq.empty), 
        Assignment(targetAssignment, Seq.empty), done = true)
    ))
    TestUtils.waitUntilTrue(
      () => {
        latestResult = verifyAssignment(zkClient, reassignmentJson, preserveThrottles = false)
        expectedResult.equals(latestResult)
      }, s"Timed out waiting for verifyAssignment result ${expectedResult}. " +
        s"The latest result was ${latestResult}", pause = 10L)
    val newAssignment = zkClient.getReplicasForPartition(topicPartition)
    debug(newAssignment.mkString("NewAssignment: [", ",", "]"))
    replicaToAdd
  }

  // Append messages to the leader, validate that data is replicated to the follower and has been tiered as well.
  private def appendMessagesAndWaitUntilTieredAndSynced(leaderId: Int, followerId: Int): Unit = {
    val endOffset = appendMessagesToLeaderAndWaitUntilTiered(leaderId)
    val followerLog = serverForId(followerId).get.replicaManager.getLog(topicPartition).get
    TestUtils.waitUntilTrue(() => endOffset == followerLog.logEndOffset,
      "Timeout waiting for all messages to be written, synced and tiered", 60000)
  }

  // Append messages to the leader, verify that some segments have tiered and return the number of messages appended
  private def appendMessagesToLeaderAndWaitUntilTiered(leaderId: Int, numSegments: Int = 5): Int = {
    val log = serverForId(leaderId).get.replicaManager.getLog(topicPartition).get
    val numMessages = 100
    var totalMessages = 0
    while (log.numberOfSegments <= numSegments) {
      TestUtils.generateAndProduceMessages(servers.toSeq, topicName, numMessages)
      totalMessages += numMessages
    }
    TestUtils.waitUntilTrue(() =>
      log.logEndOffset == totalMessages &&
        log.tierPartitionState.numSegments() >= log.numberOfSegments - 1,
      "Timeout waiting for all messages to be written, synced and tiered", 60000)
    totalMessages
  }

  // Fetch data for consumer and replica from tiered segments and local segments and validate responses
  private def fetchTieredAndLocalDataAndValidate(leaderId: Int, followerIdOpt: Option[Int]): Unit = {
    val leaderLog = serverForId(leaderId).get.replicaManager.getLog(topicPartition).get
    // Make sure that not all data is local
    TestUtils.waitUntilTrue(() => leaderLog.logStartOffset < leaderLog.localLogStartOffset,
      s"Timed out waiting for hot set retention to kick in logStartOffset: ${leaderLog.logStartOffset} localLogStartOffset ${leaderLog.localLogStartOffset}", 60000)

    val leaderEpoch = serverForId(leaderId).get.replicaManager.getPartitionOrException(topicPartition, expectLeader = true).getLeaderEpoch
    var fetchSizeBytes = leaderLog.tierPartitionState.totalSize()
    fetchDataAndValidateResponses(leaderId, followerIdOpt, leaderEpoch, leaderLog.logStartOffset, fetchSizeBytes, Errors.OFFSET_TIERED)
    fetchSizeBytes = leaderLog.size - leaderLog.tierPartitionState.totalSize()
    fetchDataAndValidateResponses(leaderId, followerIdOpt, leaderEpoch, leaderLog.tierPartitionState.endOffset() + 1, fetchSizeBytes, Errors.NONE)
  }

  // Generate Fetch requests for consumer and replica and validate responses
  private def fetchDataAndValidateResponses(leaderId: Int, followerIdOpt: Option[Int], leaderEpoch: Int, fetchOffset: Long, size: Long, replicaErr: Errors): Unit = {
    val ver: Short = 11
    val partitionMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    partitionMap.put(topicPartition, new FetchRequest.PartitionData(fetchOffset, 0, size.toInt, Optional.of[Integer](leaderEpoch)))
    val consumerFetchRequest = FetchRequest.Builder
      .forConsumer(0, 0, partitionMap)
      .build(ver.toShort)
    val consumerFetchResponse = connectAndReceive[FetchResponse[MemoryRecords]](consumerFetchRequest, destination = brokerSocketServer(leaderId))
    val consumerResponsePartitionData = consumerFetchResponse.responseData.get(topicPartition)
    assertEquals(s"Unexpected error returned by consumer fetch request", Errors.NONE, consumerResponsePartitionData.error)

    if (followerIdOpt.isDefined) {
      val replicaFetchRequest = FetchRequest.Builder
        .forReplica(ver.toShort, followerIdOpt.get, 0, size.toInt, partitionMap)
        .build(ver.toShort)
      val replicaFetchResponse = connectAndReceive[FetchResponse[MemoryRecords]](replicaFetchRequest, destination = brokerSocketServer(leaderId))
      val replicaResponsePartitionData = replicaFetchResponse.responseData().get(topicPartition)
      assertEquals(s"Unexpected error returned by replica fetch request", replicaErr, replicaResponsePartitionData.error)
    }
  }

  // Switch leader and follower. This method is made to work only for two replicas in isr, and require auto.leader.rebalance.enable set to false
  private def switchLeader(currentLeaderId: Int, currentFollowerId: Int): Unit = {
    serverForId(currentLeaderId).get.shutdown()
    val partition = serverForId(currentFollowerId).get.replicaManager.getPartitionOrException(topicPartition, expectLeader = false)
    TestUtils.waitUntilTrue(() => partition.isLeader, s"Timed out waiting for new leader to elect", 30000)
    serverForId(currentLeaderId).get.startup()
    TestUtils.waitUntilTrue(() => partition.inSyncReplicaIds.contains(currentLeaderId), s"Timed out waiting for server to start", 30000)
  }

  private def createTopicAndGetReplicas(): (Int, Int) = {
    TestUtils.waitUntilControllerElected(zkClient)
    val partitionToLeaderMap = createTopic(topicPartition.topic(), 1, 2, props)
    val replica1 = partitionToLeaderMap(topicPartition.partition())
    val replica2 = TestUtils.findFollowerId(topicPartition, servers)
    (replica1, replica2)
  }

  private def changeTopicConfig(propKey: String, propValue: String, topic: String = topicPartition.topic()): Unit = {
    val alterConfigOp = new AlterConfigOp(new ConfigEntry(propKey, propValue), AlterConfigOp.OpType.SET)
    val configs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]
    configs.put(new ConfigResource(ConfigResource.Type.TOPIC, topic), Collections.singletonList(alterConfigOp))
    val adminClient = createAdminClient()
    adminClient.incrementalAlterConfigs(configs).all().get(5, TimeUnit.SECONDS)
  }
}
