package kafka.tier

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.util
import java.util.{Collections, UUID}
import java.util.concurrent.CountDownLatch

import kafka.server.LogDirFailureChannel
import kafka.tier.state.TierPartitionState
import kafka.utils.TestUtils
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.Test

class TierTopicManagerCommitterTest {
  @Test
  def earliestOffsetTest(): Unit = {
    val positions1 = new util.HashMap[Integer, java.lang.Long]
    val positions2 = new util.HashMap[Integer, java.lang.Long]
    positions1.put(3, 5L)
    positions2.put(3, 2L)
    assertEquals(2L, TierTopicManagerCommitter.earliestOffsets(util.Arrays.asList(positions1, positions2)).get(3))
    // reverse order
    assertEquals(2L, TierTopicManagerCommitter.earliestOffsets(util.Arrays.asList(positions2, positions1)).get(3))
  }

  @Test
  def offsetInOneNotOther(): Unit = {
    val positions1 = new util.HashMap[Integer, java.lang.Long]
    positions1.put(2, 5L)
    val positions2 = new util.HashMap[Integer, java.lang.Long]
    positions2.put(3, 5L)
    assertTrue("Overall offset positions not reset, even though positions were missing.",
      TierTopicManagerCommitter.earliestOffsets(util.Arrays.asList(positions1, positions2)).isEmpty)
  }

  @Test
  def offsetsEmptyInOneDir(): Unit = {
    val positions1 = new util.HashMap[Integer, java.lang.Long]
    val positions2 = new util.HashMap[Integer, java.lang.Long]
    positions2.put(3, 5L)
    assertTrue("Overall offset positions not reset, even though positions were missing.",
      TierTopicManagerCommitter.earliestOffsets(util.Arrays.asList(positions1, positions2)).isEmpty)
  }

  @Test
  def writeReadTest(): Unit = {
    val logDir = System.getProperty("java.io.tmpdir")+"/"+UUID.randomUUID.toString
    val file = new File(logDir)
    file.mkdir()
    val tierTopicManagerConfig = new TierTopicManagerConfig(
      "bootstrap",
      null,
      1,
      1,
      33,
      "cluster99",
      200L,
      500,
      500,
      Collections.singletonList(logDir))


    val metadataManager : TierMetadataManager = EasyMock.createMock(classOf[TierMetadataManager])
    EasyMock.expect(metadataManager.tierEnabledPartitionStateIterator()).andReturn(new util.ArrayList[TierPartitionState]().iterator)
    EasyMock.replay(metadataManager)

    val committer = new TierTopicManagerCommitter(tierTopicManagerConfig, metadataManager, EasyMock.mock(classOf[LogDirFailureChannel]), new CountDownLatch(1))
    committer.updatePosition(3, 1L)
    committer.updatePosition(5, 4L)
    committer.updatePosition(5, 5L)
    committer.flush()

    val committer2 = new TierTopicManagerCommitter(tierTopicManagerConfig, metadataManager, EasyMock.mock(classOf[LogDirFailureChannel]), new CountDownLatch(1))
    val positions = committer2.positions
    val expected = new util.HashMap[Integer, Long]()
    expected.put(3,1L)
    expected.put(5,5L)
    assertEquals(expected, positions)
  }

  @Test
  def unsupportedVersionResetsPositions(): Unit = {
    val testDir = TestUtils.tempDir()
    val file = new File(testDir.getAbsolutePath + "/tier.offsets")
    val fileWriter = new FileWriter(file)
    try {
      val write = new BufferedWriter(fileWriter)
      try {
        write.write(Integer.toString(TierTopicManagerCommitter.CURRENT_VERSION+1))
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
}
