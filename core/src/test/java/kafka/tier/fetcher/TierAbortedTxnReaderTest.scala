package kafka.tier.fetcher


import kafka.log.AbortedTxn
import kafka.tier.tasks.archive.ArchiveTask
import org.apache.kafka.common.utils.ByteBufferInputStream
import org.junit.Test
import org.junit.Assert
import scala.collection.JavaConverters._

class TierAbortedTxnReaderTest {
  private def roundTrip(abortedTxns: Seq[AbortedTxn]): Unit = {
    ArchiveTask.serializeAbortedTransactions(abortedTxns) match {
      case Some(serialized) =>
        val inputStream = new ByteBufferInputStream(serialized)
        val parsedAbortedTxns = TierAbortedTxnReader.readInto(CancellationContext.newContext(), inputStream, 0, Long.MaxValue)
        Assert.assertEquals(abortedTxns, parsedAbortedTxns.asScala)
      case None =>
        if (abortedTxns.nonEmpty)
          Assert.fail("expected some aborted transactions to be deserialized")
    }
  }

  @Test
  def testRoundTripAbortedTxns(): Unit = {
    roundTrip(Seq())
    roundTrip(Seq(new AbortedTxn(1, 2, 3, 4)))
    roundTrip(Seq(new AbortedTxn(0, 0, 0, 0)))
    roundTrip(Seq(
      new AbortedTxn(0, 0, 0, 0),
      new AbortedTxn(1, 1, 1, 1)
    ))
  }

  @Test
  def testSerializeDoesNotModifyMarker(): Unit = {
    val abortedTxn = new AbortedTxn(1, 1, 1, 1)
    ArchiveTask.serializeAbortedTransactions(Seq(abortedTxn))
    Assert.assertEquals(0, abortedTxn.buffer.position())
    Assert.assertEquals(1, abortedTxn.producerId)
    Assert.assertEquals(1, abortedTxn.firstOffset)
    Assert.assertEquals(1, abortedTxn.lastOffset)
    Assert.assertEquals(1, abortedTxn.lastStableOffset)
  }

  @Test
  def testSeekOffsetRange(): Unit = {
    val ctx = CancellationContext.newContext()
    val serialized = ArchiveTask.serializeAbortedTransactions(Seq(
      new AbortedTxn(0, 3, 10, 0),
      new AbortedTxn(0, 15, 20, 0),
      new AbortedTxn(0, 30, 50, 0),
    )).get

    Assert.assertEquals("expected to find a single aborted transaction marker",
      Seq(new AbortedTxn(0, 3, 10, 0)),
      TierAbortedTxnReader.readInto(ctx, new ByteBufferInputStream(serialized), 0, 5).asScala)

    serialized.position(0)
    Assert.assertEquals("expected to find zero aborted transaction markers",
      Seq(),
      TierAbortedTxnReader.readInto(ctx, new ByteBufferInputStream(serialized), 0, 1).asScala)

    serialized.position(0)
    Assert.assertEquals("expected to find two aborted transaction markers",
      Seq(new AbortedTxn(0, 15, 20, 0),
        new AbortedTxn(0, 30, 50, 0)),
      TierAbortedTxnReader.readInto(ctx, new ByteBufferInputStream(serialized), 11, 35).asScala)

    serialized.position(0)
    Assert.assertEquals("expected to find two aborted transaction markers",
      Seq(new AbortedTxn(0, 3, 10, 0),
        new AbortedTxn(0, 15, 20, 0)),
      TierAbortedTxnReader.readInto(ctx, new ByteBufferInputStream(serialized), 0, 16).asScala)

    serialized.position(0)
    Assert.assertEquals("expected to find adjacent aborted transaction markers",
      Seq(new AbortedTxn(0, 3, 10, 0)),
      TierAbortedTxnReader.readInto(ctx, new ByteBufferInputStream(serialized), 0, 3).asScala)

    serialized.position(0)
    Assert.assertEquals("expected to find fully overlapped aborted transaction markers",
      Seq(new AbortedTxn(0, 3, 10, 0)),
      TierAbortedTxnReader.readInto(ctx, new ByteBufferInputStream(serialized), 3, 10).asScala)

    serialized.position(0)
    Assert.assertEquals("expected to find zero aborted transaction markers",
      Seq(),
      TierAbortedTxnReader.readInto(ctx, new ByteBufferInputStream(serialized), 100, 150).asScala)
  }

  @Test // Test that there is no reliance on offset ordering when seeking.
  def testSeekOffsetRangeOverlappingAborts(): Unit = {
    val ctx = CancellationContext.newContext()
    val serialized = ArchiveTask.serializeAbortedTransactions(Seq(
      new AbortedTxn(producerId=3, firstOffset=1999, lastOffset=2000, lastStableOffset=0),
      new AbortedTxn(producerId=1, firstOffset=1246, lastOffset=2000, lastStableOffset=0)
    )).get

    Assert.assertEquals(Set(
      new AbortedTxn(producerId=1, firstOffset=1246, lastOffset=2000, lastStableOffset=0)
    ), TierAbortedTxnReader.readInto(ctx, new ByteBufferInputStream(serialized), 0, 1776).asScala.toSet)

    val serialized2 = ArchiveTask.serializeAbortedTransactions(Seq(
      new AbortedTxn(producerId=1, firstOffset=1, lastOffset=1180, lastStableOffset=0),
    )).get

    Assert.assertEquals(Set(
      new AbortedTxn(producerId=1, firstOffset=1, lastOffset=1180, lastStableOffset=0)
    ), TierAbortedTxnReader.readInto(ctx, new ByteBufferInputStream(serialized2), 0, 1).asScala.toSet)
  }

  @Test
  def testAbortedTxnsRange(): Unit = {
    Assert.assertTrue(TierAbortedTxnReader.abortedTxnInRange(0, 10, new AbortedTxn(0, 0, 5, 0)))
    Assert.assertTrue(TierAbortedTxnReader.abortedTxnInRange(5, 10, new AbortedTxn(0, 0, 25, 0)))
    Assert.assertFalse(TierAbortedTxnReader.abortedTxnInRange(0, 2, new AbortedTxn(0, 3, 5, 0)))
    Assert.assertFalse(TierAbortedTxnReader.abortedTxnInRange(20, 30, new AbortedTxn(0, 3, 5, 0)))
  }
}
