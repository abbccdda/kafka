package kafka.tier

import java.io.File
import java.util
import java.util.{Collections, UUID}
import java.util.concurrent.CountDownLatch

import kafka.tier.state.TierPartitionState
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
    val positions2 = new util.HashMap[Integer, java.lang.Long]
    positions1.put(3, 5L)
    assertEquals(5L, TierTopicManagerCommitter.earliestOffsets(util.Arrays.asList(positions1, positions2)).get(3))
    // reverse order
    assertEquals(5L, TierTopicManagerCommitter.earliestOffsets(util.Arrays.asList(positions2, positions1)).get(3))
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

    val committer = new TierTopicManagerCommitter(tierTopicManagerConfig, metadataManager, new CountDownLatch(1))
    committer.updatePosition(3, 1L)
    committer.updatePosition(5, 4L)
    committer.updatePosition(5, 5L)
    committer.flush()

    val committer2 = new TierTopicManagerCommitter(tierTopicManagerConfig, metadataManager, new CountDownLatch(1))
    val positions = committer2.positions
    val expected = new util.HashMap[Integer, Long]()
    expected.put(3,1L)
    expected.put(5,5L)
    assertEquals(expected, positions)
  }

}
