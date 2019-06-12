package kafka.tier.store

import java.io.File
import java.nio.ByteBuffer
import java.util.{Optional, UUID}

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{InitiateMultipartUploadResult, UploadPartResult}
import kafka.tier.TopicIdPartition
import kafka.tier.domain.TierObjectMetadata
import org.junit.Assert._
import org.junit.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, times, verify, when}

class S3TierObjectStoreTest {
  @Test
  def testSinglePut(): Unit = {
    val segmentSize = 100
    val partSize = 200

    val client = mock(classOf[AmazonS3])
    val config = new TierObjectStoreConfig("cluster", 3, "bucket", "region", "key", "id", "endpoint", "signer", "sseAlgorithm", partSize)
    val objectStore = new S3TierObjectStore(client, config)
    val metadata = new TierObjectStore.ObjectMetadata(new TopicIdPartition("foo", UUID.randomUUID, 0), UUID.randomUUID, 0, 0, false)
    val segmentData = mock(classOf[File])

    when(segmentData.length).thenReturn(segmentSize)
    objectStore.putSegment(metadata, segmentData, null, null, Optional.empty(), Optional.empty(), Optional.empty())
    // expect 3 `put` calls: segment, offset index, and time index
    verify(client, times(3)).putObject(any())
  }


  @Test
  def testSinglePutWithAbortedTxns(): Unit = {
    val segmentSize = 100
    val partSize = 200

    val client = mock(classOf[AmazonS3])
    val config = new TierObjectStoreConfig("cluster", 3, "bucket", "region", "key", "id", "endpoint", "signer", "sseAlgorithm", partSize)
    val objectStore = new S3TierObjectStore(client, config)
    val metadata = new TierObjectStore.ObjectMetadata(new TopicIdPartition("foo", UUID.randomUUID, 0), UUID.randomUUID, 0, 0, true)
    val segmentData = mock(classOf[File])

    when(segmentData.length).thenReturn(segmentSize)
    objectStore.putSegment(metadata, segmentData, null, null, Optional.empty(), Optional.of(ByteBuffer.allocate(0)), Optional.empty())
    // expect 4 `put` calls: segment, offset index, time index, and aborted transactions
    verify(client, times(4)).putObject(any())
  }

  @Test
  def testMultiPartPut(): Unit = {
    val segmentSize = 100
    val partSize = 33

    val client = mock(classOf[AmazonS3])
    val config = new TierObjectStoreConfig("cluster", 3, "bucket", "region", "key", "id", "endpoint", "signer", "sseAlgorithm", partSize)
    val objectStore = new S3TierObjectStore(client, config)
    val metadata = new TierObjectStore.ObjectMetadata(new TopicIdPartition("foo", UUID.randomUUID, 0), UUID.randomUUID, 0, 0, false)
    val segmentData = mock(classOf[File])

    when(segmentData.length).thenReturn(segmentSize)
    when(client.initiateMultipartUpload(any())).thenReturn(mock(classOf[InitiateMultipartUploadResult]))
    when(client.uploadPart(any())).thenReturn(mock(classOf[UploadPartResult]))

    objectStore.putSegment(metadata, segmentData, null, null, Optional.empty(), Optional.of(ByteBuffer.allocate(0)), Optional.empty())
    verify(client, times(3)).putObject(any())
    verify(client, times(math.ceil(segmentSize.toDouble / partSize).toInt)).uploadPart(any())
  }

  @Test
  def testSinglePutProducerStateEpochState(): Unit = {
    val segmentSize = 100
    val partSize = 200

    val client = mock(classOf[AmazonS3])
    val config = new TierObjectStoreConfig("cluster", 3, "bucket", "region", "key", "id", "endpoint", "signer", "sseAlgorithm", partSize)
    val objectStore = new S3TierObjectStore(client, config)
    val metadata = new TierObjectStore.ObjectMetadata(new TopicIdPartition("foo", UUID.randomUUID, 0), UUID.randomUUID, 0, 0, false)
    val segmentData = mock(classOf[File])

    when(segmentData.length).thenReturn(segmentSize)
    objectStore.putSegment(metadata, segmentData, null, null, Optional.of(segmentData), Optional.of(ByteBuffer.allocate(0)), Optional.of(segmentData))
    // expect 6 `put` calls: segment, offset index, time index, transaction index, producer snapshot and epoch state
    verify(client, times(6)).putObject(any())
  }

  @Test
  def testS3KeyGeneration(): Unit = {
    val partSize = 100
    val client = mock(classOf[AmazonS3])
    val config = new TierObjectStoreConfig("cluster", 3, "bucket", "region", "key", "id", "endpoint", "signer", "sseAlgorithm", partSize)
    val objectStore = new S3TierObjectStore(client, config)

    val topicId = UUID.fromString("43aeca7f-a684-4b60-bff8-9b3b783691bb")
    val metadata = new TierObjectMetadata(new TopicIdPartition("foo", topicId, 0), 0, UUID.randomUUID,
      0, 100, 100, 1000, TierObjectMetadata.State.SEGMENT_UPLOAD_COMPLETE, false, false, false)
    assertEquals(s"0/${metadata.objectIdAsBase64}/Q67Kf6aES2C_-Js7eDaRuw/0/00000000000000000000_0_v0.segment",
      objectStore.keyPath(new TierObjectStore.ObjectMetadata(metadata), TierObjectStore.FileType.SEGMENT))
  }
}
