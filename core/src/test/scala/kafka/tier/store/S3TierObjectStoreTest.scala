package kafka.tier.store

import java.io.File
import java.util.Optional
import java.util.UUID

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{InitiateMultipartUploadResult, UploadPartResult}
import kafka.tier.domain.TierObjectMetadata
import kafka.tier.TopicIdPartition
import kafka.tier.store.TierObjectStore.TierObjectStoreFileType
import org.junit.Test
import org.junit.Assert._
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
    val metadata = new TierObjectMetadata(new TopicIdPartition("foo", UUID.randomUUID(), 0), 0, 0, 100, 100, 1000, 1000, false, false, false, 0.toByte)
    val segmentData = mock(classOf[File])

    when(segmentData.length).thenReturn(segmentSize)
    objectStore.putSegment(metadata, segmentData, null, null, null, null, Optional.empty())
    // expect 5 `put` calls: segment, offset index, time index, transaction index, producer snapshot
    verify(client, times(5)).putObject(any())
  }

  @Test
  def testMultiPartPut(): Unit = {
    val segmentSize = 100
    val partSize = 33

    val client = mock(classOf[AmazonS3])
    val config = new TierObjectStoreConfig("cluster", 3, "bucket", "region", "key", "id", "endpoint", "signer", "sseAlgorithm", partSize)
    val objectStore = new S3TierObjectStore(client, config)
    val metadata = new TierObjectMetadata(new TopicIdPartition("foo", UUID.randomUUID(), 0), 0, 0, 100, 100, 1000, 1000, false, false, false, 0.toByte)
    val segmentData = mock(classOf[File])

    when(segmentData.length).thenReturn(segmentSize)
    when(client.initiateMultipartUpload(any())).thenReturn(mock(classOf[InitiateMultipartUploadResult]))
    when(client.uploadPart(any())).thenReturn(mock(classOf[UploadPartResult]))

    objectStore.putSegment(metadata, segmentData, null, null, null, null, Optional.empty())
    verify(client, times(4)).putObject(any())
    verify(client, times(math.ceil(segmentSize.toDouble / partSize).toInt)).uploadPart(any())
  }

  @Test
  def testS3KeyGeneration(): Unit = {
    val partSize = 100
    val client = mock(classOf[AmazonS3])
    val config = new TierObjectStoreConfig("cluster", 3, "bucket", "region", "key", "id", "endpoint", "signer", "sseAlgorithm", partSize)
    val objectStore = new S3TierObjectStore(client, config)

    val topicId = UUID.fromString("43aeca7f-a684-4b60-bff8-9b3b783691bb")
    val metadata = new TierObjectMetadata(new TopicIdPartition("foo", topicId, 0), 0,
      0, 100, 100, 1000, 1000, false, false, false, 0.toByte)
    assertEquals("0/" + metadata.messageId + "/43aeca7f-a684-4b60-bff8-9b3b783691bb/0/00000000000000000000_0_v0.segment",
      objectStore.keyPath(metadata, TierObjectStoreFileType.SEGMENT))
  }

}
