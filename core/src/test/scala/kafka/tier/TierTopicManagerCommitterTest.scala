package kafka.tier

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.io.IOException
import java.util
import java.util.{Collections, Optional, UUID}

import kafka.server.LogDirFailureChannel
import kafka.tier.state.{OffsetAndEpoch, TierPartitionState}
import kafka.tier.topic.TierTopicManagerConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions

class TierTopicManagerCommitterTest {
  @Test
  def earliestOffsetTest(): Unit = {
    val positions1 = Collections.singletonMap(Integer.valueOf(3), new OffsetAndEpoch(5L, Optional.of(3)))
    val positions2 = Collections.singletonMap(Integer.valueOf(3), new OffsetAndEpoch(2L, Optional.of(1)))
    assertEquals(new OffsetAndEpoch(2L, Optional.of(1)), TierTopicManagerCommitter.earliestOffsets(util.Arrays.asList(positions1, positions2)).get(3))
    // reverse order
    assertEquals(new OffsetAndEpoch(2L, Optional.of(1)), TierTopicManagerCommitter.earliestOffsets(util.Arrays.asList(positions2, positions1)).get(3))
  }

  @Test
  def offsetInOneNotOther(): Unit = {
    val positions1 = Collections.singletonMap(Integer.valueOf(2), new OffsetAndEpoch(5L, Optional.of(2)))
    val positions2 = Collections.singletonMap(Integer.valueOf(3), new OffsetAndEpoch(5L, Optional.of(3)))
    assertTrue("Overall offset positions not reset, even though positions were missing.",
      TierTopicManagerCommitter.earliestOffsets(util.Arrays.asList(positions1, positions2)).isEmpty)
  }

  @Test
  def offsetsEmptyInOneDir(): Unit = {
    val positions1 = Collections.emptyMap[Integer, OffsetAndEpoch]()
    val positions2 = Collections.singletonMap(Integer.valueOf(3), new OffsetAndEpoch(5L, Optional.empty()))
    assertTrue("Overall offset positions not reset, even though positions were missing.",
      TierTopicManagerCommitter.earliestOffsets(util.Arrays.asList(positions1, positions2)).isEmpty)
  }

  @Test
  def writeReadTest(): Unit = {
    val logDir = System.getProperty("java.io.tmpdir") + "/" + UUID.randomUUID.toString
    val file = new File(logDir)
    file.mkdir()
    val numPartitions = 6: Short
    val tierTopicManagerConfig = new TierTopicManagerConfig(
      () => Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "bootstrap"),
      null,
      numPartitions,
      1,
      33,
      "cluster99",
      200L,
      500,
      500,
      Collections.singletonList(logDir))

    val committer = new TierTopicManagerCommitter(tierTopicManagerConfig, EasyMock.mock(classOf[LogDirFailureChannel]))
    committer.updatePosition(3, new OffsetAndEpoch(1L, Optional.of(1)))
    committer.updatePosition(5, new OffsetAndEpoch(4L, Optional.of(2)))
    committer.updatePosition(5, new OffsetAndEpoch(5L, Optional.empty()))
    committer.flush(new util.ArrayList[TierPartitionState]().iterator)

    val committer2 = new TierTopicManagerCommitter(tierTopicManagerConfig, EasyMock.mock(classOf[LogDirFailureChannel]))
    val expectedPositions: Array[Option[OffsetAndEpoch]] = Array.apply(None,
      None,
      None,
      Some(new OffsetAndEpoch(1L, Optional.of(1))),
      None,
      Some(new OffsetAndEpoch(5L, Optional.empty[Integer])))

    for (partitionId <- 0 until numPartitions)
      assertEquals(expectedPositions(partitionId).getOrElse(null), committer2.positionFor(partitionId))
  }

  @Test
  def failedFlushTest(): Unit = {
    val logDir = System.getProperty("java.io.tmpdir")+"/"+UUID.randomUUID.toString
    val file = new File(logDir)
    file.mkdir()
    val numPartitions = 6: Short
    val tierTopicManagerConfig = new TierTopicManagerConfig(
      () => Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "bootstrap"),
      null,
      numPartitions,
      1,
      33,
      "cluster99",
      200L,
      500,
      500,
      Collections.singletonList(logDir))

    val logDirFailureChannel = new LogDirFailureChannel(10)
    val tps: TierPartitionState = EasyMock.mock(classOf[TierPartitionState])
    EasyMock.expect(tps.flush()).andThrow(new IOException("failed to flush"))
    EasyMock.expect(tps.dir()).andReturn(new File(logDir)).anyTimes()
    EasyMock.replay(tps)

    val committer = new TierTopicManagerCommitter(tierTopicManagerConfig, logDirFailureChannel)
    committer.updatePosition(3, new OffsetAndEpoch(1L, Optional.of(2)))
    committer.updatePosition(5, new OffsetAndEpoch(4L, Optional.of(1)))
    committer.updatePosition(5, new OffsetAndEpoch(5L, Optional.of(1)))
    committer.flush(Collections.singletonList(tps).iterator)

    val committer2 = new TierTopicManagerCommitter(tierTopicManagerConfig, logDirFailureChannel)
    // none of the positions should have committed as a result of the TierPartitionState flush failure
    val expectedPositions: Array[Option[Long]] = Array.apply(None, None, None, None, None, None)

    for (partitionId <- 0 until numPartitions)
      assertEquals(expectedPositions(partitionId).getOrElse(null), committer2.positionFor(partitionId))

    assertEquals(System.getProperty("java.io.tmpdir").replaceFirst("/$", ""), logDirFailureChannel.takeNextOfflineLogDir())
  }


  @Test
  def unsupportedVersionResetsPositions(): Unit = {
    val testDir = TestUtils.tempDir()
    val file = new File(testDir.getAbsolutePath + "/tier.offsets")
    val fileWriter = new FileWriter(file)
    try {
      val write = new BufferedWriter(fileWriter)
      try {
        write.write(Integer.toString(TierTopicManagerCommitter.CURRENT_VERSION.version + 1))
        write.newLine()
        write.write("0 3")
        write.newLine()
      } finally {
        write.flush()
        write.close()
      }
    } finally {
      fileWriter.close()
    }
    assertTrue(TierTopicManagerCommitter.committed(testDir.getAbsolutePath, EasyMock.mock(classOf[LogDirFailureChannel])).isEmpty)
  }

  @Test
  def invalidVersionResetsPositions(): Unit = {
    val testDir = TestUtils.tempDir()
    val file = new File(testDir.getAbsolutePath + "/tier.offsets")
    val fileWriter = new FileWriter(file)
    try {
      val write = new BufferedWriter(fileWriter)
      try {
        write.write("aaa")
        write.newLine()
        write.write("0 3")
        write.newLine()
      } finally {
        write.flush()
        write.close()
      }
    } finally {
      fileWriter.close()
    }
    assertTrue(TierTopicManagerCommitter.committed(testDir.getAbsolutePath, EasyMock.mock(classOf[LogDirFailureChannel])).isEmpty)
  }

  @Test
  def invalidOffsetsLinePositions(): Unit = {
    val testDir = TestUtils.tempDir()
    val file = new File(testDir.getAbsolutePath + "/tier.offsets")
    val fileWriter = new FileWriter(file)
    try {
      val write = new BufferedWriter(fileWriter)
      try {
        write.write("0")
        write.newLine()
        write.write("3")
        write.newLine()
        write.write("0 5")
      } finally {
        write.flush()
        write.close()
      }
    } finally {
      fileWriter.close()
    }
    assertTrue(TierTopicManagerCommitter.committed(testDir.getAbsolutePath, EasyMock.mock(classOf[LogDirFailureChannel])).isEmpty)
  }

  @Test
  def testUpdatePosition(): Unit = {
    val logDir = TestUtils.tempDir().getAbsolutePath
    val file = new File(logDir)
    file.mkdir()
    val numPartitions = 6: Short
    val tierTopicManagerConfig = new TierTopicManagerConfig(
      () => Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "bootstrap"),
      null,
      numPartitions,
      1,
      33,
      "cluster99",
      200L,
      500,
      500,
      Collections.singletonList(logDir))

    val partitionId = 10
    val committer = new TierTopicManagerCommitter(tierTopicManagerConfig, EasyMock.mock(classOf[LogDirFailureChannel]))
    committer.updatePosition(partitionId, new OffsetAndEpoch(100, Optional.of(5)))

    // updating to higher offset is permissible
    committer.updatePosition(partitionId, new OffsetAndEpoch(200, Optional.of(5)))
    assertEquals(new OffsetAndEpoch(200, Optional.of(5)), committer.positionFor(partitionId))

    // updating to higher offset with higher epoch is permissible
    committer.updatePosition(partitionId, new OffsetAndEpoch(350, Optional.of(7)))
    assertEquals(new OffsetAndEpoch(350, Optional.of(7)), committer.positionFor(partitionId))

    // updating to higher offset with no epoch is permissible (message format downgrade)
    committer.updatePosition(partitionId, new OffsetAndEpoch(375, Optional.empty[Integer]))
    assertEquals(new OffsetAndEpoch(375, Optional.empty[Integer]), committer.positionFor(partitionId))

    // updating to higher offset with a valid epoch is permissible (message format upgrade)
    committer.updatePosition(partitionId, new OffsetAndEpoch(400, Optional.of(10)))
    assertEquals(new OffsetAndEpoch(400, Optional.of(10)), committer.positionFor(partitionId))

    // updating to lower (or equal) offset is not permissible
    Assertions.assertThrows[IllegalStateException] {
      committer.updatePosition(partitionId, new OffsetAndEpoch(400, Optional.of(10)))
    }
    Assertions.assertThrows[IllegalStateException] {
      committer.updatePosition(partitionId, new OffsetAndEpoch(399, Optional.of(10)))
    }

    // updating to lower epoch is not permissible
    Assertions.assertThrows[IllegalStateException] {
      committer.updatePosition(partitionId, new OffsetAndEpoch(500, Optional.of(9)))
    }
    assertEquals(new OffsetAndEpoch(400, Optional.of(10)), committer.positionFor(partitionId))
  }
}
