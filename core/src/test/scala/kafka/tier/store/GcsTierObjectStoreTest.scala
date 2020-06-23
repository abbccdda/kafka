package kafka.tier.store

import java.io.File
import java.nio.ByteBuffer
import java.util
import java.util.{Optional, UUID}

import com.google.cloud.storage.{Blob, BlobId, BlobInfo, Bucket, Storage}
import com.google.cloud.WriteChannel
import kafka.tier.exceptions.TierObjectStoreRetriableException
import kafka.tier.TopicIdPartition
import kafka.tier.store.TierObjectStore.ObjectMetadata
import kafka.utils.TestUtils
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{atLeastOnce, mock, times, verify, when}

class GcsTierObjectStoreTest {
  val storage = mock(classOf[Storage])
  val bucket = mock(classOf[Bucket])
  val config = GcsTierObjectStoreConfig.createWithEmptyClusterIdBrokerId("bucket", "prefix", "region", 10240, "path")
  val metadata = new ObjectMetadata(new TopicIdPartition("foo", UUID.randomUUID, 0), UUID.randomUUID, 0, 0, false, false, false)
  val testFile : File = TestUtils.tempFile()
  val bb = ByteBuffer.allocate(0)
  var objectStore : GcsTierObjectStore = _

  @Before
  def setup(): Unit = {
    when(storage.get(anyString(), any(classOf[Storage.BucketGetOption]))).thenReturn(bucket)
    when(storage.writer(any(classOf[BlobInfo]))).thenReturn(mock(classOf[WriteChannel]))
    when(bucket.getLocation).thenReturn("REGION")
    objectStore = new GcsTierObjectStore(storage, config)
  }

  @Test
  def testSinglePut(): Unit = {
    objectStore.putSegment(metadata, testFile, testFile, testFile, Optional.empty(), Optional.empty(), Optional.empty())
    verify(storage, times(3)).writer(any(classOf[BlobInfo]))
  }

  @Test
  def testSinglePutWithAbortedTxns(): Unit = {
    objectStore.putSegment(metadata, testFile, testFile, testFile, Optional.empty(), Optional.of(bb), Optional.empty())
    verify(storage, times(4)).writer(any(classOf[BlobInfo]))
  }

  @Test
  def testSinglePutProducerStateEpochState(): Unit = {
    objectStore.putSegment(metadata, testFile, testFile, testFile, Optional.of(testFile), Optional.empty(), Optional.of(bb))
    verify(storage, times(5)).writer(any(classOf[BlobInfo]))
  }

  @Test
  def testDeleteAllSuccess(): Unit = {
    val deleteResult : util.List[java.lang.Boolean] = new util.ArrayList[java.lang.Boolean]()
    deleteResult.add(true)
    deleteResult.add(true)
    deleteResult.add(true)
    when(storage.delete(any(classOf[util.ArrayList[BlobId]]))).thenReturn(deleteResult)

    objectStore.deleteSegment(metadata)
    verify(storage, times(0)).get(any(classOf[BlobId]))
  }

  @Test
  def testDeleteSomeFailedGetException(): Unit = {
    val deleteResult : util.List[java.lang.Boolean] = new util.ArrayList[java.lang.Boolean]()
    deleteResult.add(true)
    deleteResult.add(false)
    deleteResult.add(true)
    when(storage.delete(any(classOf[util.ArrayList[BlobId]]))).thenReturn(deleteResult)

    // simulate object still in bucket after delete
    val blob : Blob = mock(classOf[Blob])
    when(storage.get(any(classOf[BlobId]))).thenReturn(blob)

    try {
      objectStore.deleteSegment(metadata)
    } catch {
      case e: TierObjectStoreRetriableException =>
        verify(storage, times(1)).get(any(classOf[BlobId]))
        verify(blob, atLeastOnce()).getBlobId
        return
    }
    fail("TierObjectStoreRetriableException should have been thrown when attempting to delete segments")
  }

  @Test
  def testDeleteSomeFailedGetNoException(): Unit = {
    val deleteResult : util.List[java.lang.Boolean] = new util.ArrayList[java.lang.Boolean]()
    deleteResult.add(true)
    deleteResult.add(false)
    deleteResult.add(false)
    when(storage.delete(any(classOf[util.ArrayList[BlobId]]))).thenReturn(deleteResult)

    // simulate object not present after a "failed" delete
    when(storage.get(any(classOf[BlobId]))).thenReturn(null)

    objectStore.deleteSegment(metadata)
    verify(storage, times(2)).get(any(classOf[BlobId]))
  }
}
