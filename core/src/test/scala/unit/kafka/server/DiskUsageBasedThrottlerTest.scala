/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server

import java.nio.file.attribute.{FileAttributeView, FileStoreAttributeView}
import java.nio.file.FileStore
import java.util.concurrent.atomic.AtomicLong

import org.apache.kafka.common.config.internals.ConfluentConfigs
import org.apache.kafka.common.utils.{MockTime, Time}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.{After, Before, Test}

class DiskUsageBasedThrottlerTest {

  import DiskUsageBasedThrottlerTest.{DeterministicFileStore, TestableDiskUsageBasedThrottleListener, getThrottler}

  private val throughput = 64 * 1024L
  private val threshold = 5 * 1024 * 1024 * 1024L
  private val largeFileSize = 12 * 1024 * 1024 * 1024L
  // we are creating 2 different fileStoresRefOpt to ensure the minimum amongst them is considered for throttling
  private val logDirs = Seq("/some/fileA", "/some/fileB")
  private val fileStores = Seq(new DeterministicFileStore, new DeterministicFileStore)
  private val config = DiskUsageBasedThrottlingConfig(
    freeDiskThresholdBytes = threshold,
    throttledProduceThroughput = throughput,
    logDirs = logDirs,
    enableDiskBasedThrottling = true,
    diskCheckFrequencyMs = 500
  )
  private val mockTime = new MockTime
  private val listener = new TestableDiskUsageBasedThrottleListener

  @Before
  def setup(): Unit = {
    DiskUsageBasedThrottler.registerListener(listener)
  }

  @After
  def teardown(): Unit = {
    DiskUsageBasedThrottler.deRegisterListener(listener)
  }

  @Test
  def testDefaultConfig(): Unit = {
    val defaultConfig = DiskUsageBasedThrottlingConfig()
    assertFalse(defaultConfig.enableDiskBasedThrottling)
    assertEquals(ConfluentConfigs.BACKPRESSURE_DISK_THRESHOLD_BYTES_DEFAULT, defaultConfig.freeDiskThresholdBytes)
    assertEquals(ConfluentConfigs.BACKPRESSURE_PRODUCE_THROUGHPUT_DEFAULT, defaultConfig.throttledProduceThroughput)
    assertEquals(Seq.empty, defaultConfig.logDirs)
  }

  @Test
  def testConfigRejectsIllegalValues(): Unit = {
    val configWithIllegalValues = DiskUsageBasedThrottlingConfig(freeDiskThresholdBytes = 10L, throttledProduceThroughput = 42L)
    assertEquals(DiskUsageBasedThrottlingConfig.MinDiskThresholdBytes, configWithIllegalValues.freeDiskThresholdBytes)
    assertEquals(DiskUsageBasedThrottlingConfig.MinThroughputBytesPerSec, configWithIllegalValues.throttledProduceThroughput)
  }

  @Test
  def testConfigRejectsNegativeValues(): Unit = {
    val configWithIllegalValues = DiskUsageBasedThrottlingConfig(freeDiskThresholdBytes = -10L, throttledProduceThroughput = -42L)
    assertEquals(DiskUsageBasedThrottlingConfig.MinDiskThresholdBytes, configWithIllegalValues.freeDiskThresholdBytes)
    assertEquals(DiskUsageBasedThrottlingConfig.MinThroughputBytesPerSec, configWithIllegalValues.throttledProduceThroughput)
  }

  @Test
  def testEmptyLogDirsDisableThrottling(): Unit = {
    val configWithEmptyLogDirs = DiskUsageBasedThrottlingConfig(logDirs = Seq.empty, enableDiskBasedThrottling = true)
    assertFalse(configWithEmptyLogDirs.enableDiskBasedThrottling)
    assertEquals(Seq.empty, configWithEmptyLogDirs.logDirs)
  }

  @Test
  def testListenerRegistered(): Unit = {
    val newListener = new TestableDiskUsageBasedThrottleListener
    DiskUsageBasedThrottler.registerListener(newListener)
    assertTrue(DiskUsageBasedThrottler.getListeners.contains(newListener))
    DiskUsageBasedThrottler.deRegisterListener(newListener)
    assertFalse(DiskUsageBasedThrottler.getListeners.contains(newListener))
  }

  @Test
  def testHandleDiskSpaceLow(): Unit = {
    listener.handleDiskSpaceLow(throughput)
    assertEquals(throughput, listener.counter.get())
  }

  @Test
  def testCapValuesOnListeners(): Unit = {
    val produceListener = new TestableDiskUsageBasedThrottleListener
    val followerListener = new TestableDiskUsageBasedThrottleListener {
      override protected[server] val quotaType: QuotaType = QuotaType.FollowerReplication
    }
    DiskUsageBasedThrottler.registerListener(produceListener)
    DiskUsageBasedThrottler.registerListener(followerListener)
    val throttler = getThrottler(config, mockTime, fileStores)
    withLargeFileWritten { _ =>
      throttler.checkAndUpdateQuotaOnDiskUsage(mockTime.milliseconds())
      assertEquals(throughput, produceListener.counter.get())
      assertEquals(2 * throughput, followerListener.counter.get())
    }
    DiskUsageBasedThrottler.deRegisterListener(produceListener)
    DiskUsageBasedThrottler.deRegisterListener(followerListener)
  }

  @Test
  def testHandleDiskSpaceRecovered(): Unit = {
    listener.handleDiskSpaceRecovered()
    assertEquals(Long.MaxValue, listener.counter.get())
  }

  @Test
  def testMinDiskUsableBytes(): Unit = {
    val throttler = getThrottler(config, mockTime, fileStores)
    val previousUsableSpaceBytes = throttler.minDiskUsableBytes
    // we will write a large 12GB file
    withLargeFileWritten { fileSize =>
      val currentUsableBytes = throttler.minDiskUsableBytes
      // the current usable bytes should be equal to the existing usable bytes minus the size of the file
      assertEquals(previousUsableSpaceBytes - fileSize, currentUsableBytes)
      // verifying that the second log dir's usable space hasn't changed
      assertEquals(previousUsableSpaceBytes, fileStores(1).getUsableSpace)
    }
  }

  @Test
  def testMinDiskTotalBytes(): Unit = {
    val throttler = getThrottler(config, mockTime, fileStores)
    val existingTotalBytes = throttler.minDiskTotalBytes
    // we will write a large 12GB file
    withLargeFileWritten { _ =>
      val currentTotalBytes = throttler.minDiskTotalBytes
      // the total bytes should be unchanged even with the new file written
      assertEquals(existingTotalBytes, currentTotalBytes)
    }
  }

  @Test
  def testBasicThrottling(): Unit = {
    val threshold = fileStores.map(_.getUsableSpace).min - largeFileSize + 1L
    val throttler = getThrottler(config.copy(freeDiskThresholdBytes = threshold), mockTime, fileStores)

    // initially, since we haven't written the large file, the listener shouldn't be throttled
    throttler.checkAndUpdateQuotaOnDiskUsage(mockTime.milliseconds())
    assertEquals(Long.MaxValue, listener.counter.get)
    assertFalse(DiskUsageBasedThrottler.diskThrottlingActive(listener))

    // Next we will write the large file and then throttling should have kicked in
    withLargeFileWritten { _ =>
      throttler.checkAndUpdateQuotaOnDiskUsage(mockTime.milliseconds())
      assertEquals(throughput, listener.counter.get)
      assertEquals(throughput, listener.lastSignalledQuotaOptRef.get.get)
      assertTrue(DiskUsageBasedThrottler.diskThrottlingActive(listener))
    }

    // Once we are out of the block, the large file should be deleted
    // As a result, the current free space should once again be > 1.5x the threshold value
    throttler.checkAndUpdateQuotaOnDiskUsage(mockTime.milliseconds())
    assertEquals(Long.MaxValue, listener.counter.get)
    assertFalse(DiskUsageBasedThrottler.diskThrottlingActive(listener))
  }

  @Test
  def testThroughputIsUpdatedDuringThrottling(): Unit = {
    val threshold = fileStores.map(_.getUsableSpace).min - largeFileSize + 1L
    val throttler = getThrottler(config.copy(freeDiskThresholdBytes = threshold), mockTime, fileStores)

    // initially, since we haven't written the large file, the listener shouldn't be throttled
    throttler.checkAndUpdateQuotaOnDiskUsage(mockTime.milliseconds())
    assertFalse(DiskUsageBasedThrottler.diskThrottlingActive(listener))

    // Next we will write the large file and then throttling should have kicked in
    withLargeFileWritten { _ =>
      // firstly we verify that the original throughput is honored
      throttler.checkAndUpdateQuotaOnDiskUsage(mockTime.milliseconds())
      assertEquals(throughput, listener.counter.get)
      assertEquals(throughput, listener.lastSignalledQuotaOptRef.get.get)
      assertTrue(DiskUsageBasedThrottler.diskThrottlingActive(listener))

      // next we update the throughput while throttling is active
      val updatedThroughput = 10 * throughput
      val updatedConfig = throttler.getCurrentConfig.copy(throttledProduceThroughput = updatedThroughput)
      mockTime.sleep(501)
      throttler.updateDiskThrottlingConfig(updatedConfig)
      throttler.checkAndUpdateQuotaOnDiskUsage(mockTime.milliseconds())
      assertEquals(updatedThroughput, listener.counter.get)
      assertEquals(updatedThroughput, listener.lastSignalledQuotaOptRef.get.get)
      assertTrue(DiskUsageBasedThrottler.diskThrottlingActive(listener))
    }
  }

  @Test
  def testEnableFlagIsRespected(): Unit = {
    val threshold = fileStores.map(_.getUsableSpace).min - largeFileSize + 1L
    val throttler = getThrottler(config.copy(freeDiskThresholdBytes = threshold, enableDiskBasedThrottling = false), mockTime, fileStores)

    // initially, since we haven't written the large file, the listener shouldn't be throttled
    throttler.checkAndUpdateQuotaOnDiskUsage(mockTime.milliseconds())
    assertFalse(DiskUsageBasedThrottler.diskThrottlingActive(listener))

    // Next we will write the large file, however, throttling still shouldn't kick in
    withLargeFileWritten { _ =>
      throttler.checkAndUpdateQuotaOnDiskUsage(mockTime.milliseconds())
      assertEquals(Long.MaxValue, listener.counter.get)
      assertFalse(DiskUsageBasedThrottler.diskThrottlingActive(listener))
    }
  }

  @Test
  def testTimeIsRespected(): Unit = {
    val threshold = fileStores.map(_.getUsableSpace).min - largeFileSize + 1L
    val throttler = getThrottler(config.copy(freeDiskThresholdBytes = threshold, diskCheckFrequencyMs = 1000), mockTime, fileStores)

    // initially, since we haven't written the large file, the listener shouldn't be throttled
    throttler.checkAndUpdateQuotaOnDiskUsage(mockTime.milliseconds())
    assertFalse(DiskUsageBasedThrottler.diskThrottlingActive(listener))

    // Next we will write the large file, however, throttling still shouldn't kick in
    withLargeFileWritten { _ =>
      throttler.checkAndUpdateQuotaOnDiskUsage(mockTime.milliseconds())
      assertEquals(Long.MaxValue, listener.counter.get)
      assertFalse(DiskUsageBasedThrottler.diskThrottlingActive(listener))

      // now we will sleep for more time so that the time period is elapsed
      mockTime.sleep(500)
      throttler.checkAndUpdateQuotaOnDiskUsage(mockTime.milliseconds())
      assertEquals(throughput, listener.counter.get)
      assertEquals(throughput, listener.lastSignalledQuotaOptRef.get.get)
    }
  }

  // partial function which writes a large file of 12GB and ensure its cleanup
  private def withLargeFileWritten(inner: Long => Unit, fileSize: Long = largeFileSize): Unit = {
    // we are only writing the large file to the first log dir to ensure the verification of the minimum logic
    val fileStore = fileStores.head
    fileStore.writeLargeFile(fileSize)
    mockTime.sleep(501)
    inner(fileSize)
    mockTime.sleep(501)
    fileStore.deleteLargeFile(fileSize)
  }

}

object DiskUsageBasedThrottlerTest {

  // this is a dummy file-store to return deterministic available usable bytes
  class DeterministicFileStore extends FileStore {
    val TotalAvailableBytes: Long = 20 * 1024 * 1024 * 1024L
    val InitialUsableBytes: Long = 15 * 1024 * 1024 * 1024L

    private val availableBytes = new AtomicLong(InitialUsableBytes)

    override def name(): String = getClass.getName

    override def isReadOnly: Boolean = false

    def writeLargeFile(fileSize: Long): Unit = {
      if (fileSize > availableBytes.get) {
        throw new IllegalArgumentException("Can't write a file with size " + fileSize + " > available size: " + availableBytes.get)
      }
      availableBytes.updateAndGet(_ - fileSize)
    }

    def deleteLargeFile(fileSize: Long): Unit = {
      if (availableBytes.get + fileSize > TotalAvailableBytes) {
        throw new IllegalArgumentException("Can't delete the large file, because the file might not have been written already")
      }
      availableBytes.addAndGet(fileSize)
    }

    override def getTotalSpace: Long = TotalAvailableBytes

    override def getUsableSpace: Long = availableBytes.get

    override def getUnallocatedSpace: Long = TotalAvailableBytes - availableBytes.get

    override def supportsFileAttributeView(`type`: Class[_ <: FileAttributeView]): Boolean = ???

    override def supportsFileAttributeView(name: String): Boolean = ???

    override def getFileStoreAttributeView[V <: FileStoreAttributeView](`type`: Class[V]): V = ???

    override def getAttribute(attribute: String): AnyRef = ???

    override def `type`(): String = ???
  }

  // this is a testable listener which will update the counter on the handle signals
  class TestableDiskUsageBasedThrottleListener extends DiskUsageBasedThrottleListener {
    override protected[server] val quotaType: QuotaType = QuotaType.Produce
    val counter = new AtomicLong(Long.MaxValue)

    override def handleDiskSpaceLow(cappedQuotaInBytesPerSec: Long): Unit = counter.set(cappedQuotaInBytesPerSec)

    override def handleDiskSpaceRecovered(): Unit = counter.set(Long.MaxValue)
  }

  // this helper method will generate the throttler instance with the provided config
  def getThrottler(config: DiskUsageBasedThrottlingConfig = DiskUsageBasedThrottlingConfig(), mockTime: Time, mockFileStores: Seq[FileStore]): DiskUsageBasedThrottler = {
    new DiskUsageBasedThrottler {
      override protected def diskThrottlingConfig: DiskUsageBasedThrottlingConfig = config

      override protected def time: Time = mockTime

      override protected def getFileStores: collection.Seq[FileStore] = mockFileStores
    }
  }
}
