package kafka.tier.store

import java.nio.ByteBuffer
import java.util.{Optional, UUID}

import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Bucket
import com.google.cloud.storage.Storage
import com.google.cloud.WriteChannel
import kafka.tier.TopicIdPartition
import kafka.utils.TestUtils
import org.junit.Test
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{mock, times, verify, when}

class GcsTierObjectStoreTest {

  @Test
  def testSinglePut(): Unit = {
    val storage = mock(classOf[Storage])
    val bucket = mock(classOf[Bucket])
    when(storage.get(anyString(), any(classOf[Storage.BucketGetOption]))).thenReturn(bucket)
    when(storage.writer(any(classOf[BlobInfo]))).thenReturn(mock(classOf[WriteChannel]))
    when(bucket.getLocation).thenReturn("region")

    val config = new GcsTierObjectStoreConfig("cluster", 3, "bucket", "region", 10240, 1024)
    val objectStore = new GcsTierObjectStore(storage, config)
    val metadata = new TierObjectStore.ObjectMetadata(new TopicIdPartition("foo", UUID.randomUUID, 0), UUID.randomUUID, 0, 0, false)
    val testFile = TestUtils.tempFile()

    objectStore.putSegment(metadata, testFile, testFile, testFile, Optional.empty(), Optional.empty(), Optional.empty())

    verify(storage, times(3)).writer(any(classOf[BlobInfo]))
  }

  @Test
  def testSinglePutWithAbortedTxns(): Unit = {
    val storage = mock(classOf[Storage])
    val bucket = mock(classOf[Bucket])
    when(storage.get(anyString(), any(classOf[Storage.BucketGetOption]))).thenReturn(bucket)
    when(storage.writer(any(classOf[BlobInfo]))).thenReturn(mock(classOf[WriteChannel]))
    when(bucket.getLocation).thenReturn("region")

    val config = new GcsTierObjectStoreConfig("cluster", 3, "bucket", "region", 10240, 1024)
    val objectStore = new GcsTierObjectStore(storage, config)
    val metadata = new TierObjectStore.ObjectMetadata(new TopicIdPartition("foo", UUID.randomUUID, 0), UUID.randomUUID, 0, 0, true)
    val testFile = TestUtils.tempFile()
    val bb = ByteBuffer.allocate(0)

    objectStore.putSegment(metadata, testFile, testFile, testFile, Optional.empty(), Optional.of(bb), Optional.empty())

    verify(storage, times(4)).writer(any(classOf[BlobInfo]))
  }

  @Test
  def testSinglePutProducerStateEpochState(): Unit = {
    val storage = mock(classOf[Storage])
    val bucket = mock(classOf[Bucket])
    when(storage.get(anyString(), any(classOf[Storage.BucketGetOption]))).thenReturn(bucket)
    when(storage.writer(any(classOf[BlobInfo]))).thenReturn(mock(classOf[WriteChannel]))
    when(bucket.getLocation).thenReturn("region")

    val config = new GcsTierObjectStoreConfig("cluster", 3, "bucket", "region", 10240, 1024)
    val objectStore = new GcsTierObjectStore(storage, config)
    val metadata = new TierObjectStore.ObjectMetadata(new TopicIdPartition("foo", UUID.randomUUID, 0), UUID.randomUUID, 0, 0, false)
    val testFile = TestUtils.tempFile()

    objectStore.putSegment(metadata, testFile, testFile, testFile, Optional.of(testFile), Optional.empty(), Optional.of(testFile))

    verify(storage, times(5)).writer(any(classOf[BlobInfo]))
  }
}
