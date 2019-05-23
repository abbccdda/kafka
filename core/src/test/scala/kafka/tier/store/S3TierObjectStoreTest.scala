package kafka.tier.store

import java.io.File
import java.util.Optional

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{InitiateMultipartUploadResult, UploadPartResult}
import kafka.tier.domain.TierObjectMetadata
import org.apache.kafka.common.TopicPartition
import org.junit.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, times, verify, when}

class S3TierObjectStoreTest {
  @Test
  def testSinglePut(): Unit = {
    val segmentSize = 100
    val partSize = 200

    val client = mock(classOf[AmazonS3])
    val config = new TierObjectStoreConfig("bucket", "region", "key", "id", "endpoint", "signer", "sseAlgorithm", partSize)
    val objectStore = new S3TierObjectStore(client, config)
    val metadata = new TierObjectMetadata(new TopicPartition("foo", 0), 0, 0, 100, 100, 1000, 1000, false, false, false, 0.toByte)
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
    val config = new TierObjectStoreConfig("bucket", "region", "key", "id", "endpoint", "signer", "sseAlgorithm", partSize)
    val objectStore = new S3TierObjectStore(client, config)
    val metadata = new TierObjectMetadata(new TopicPartition("foo", 0), 0, 0, 100, 100, 1000, 1000, false, false, false, 0.toByte)
    val segmentData = mock(classOf[File])

    when(segmentData.length).thenReturn(segmentSize)
    when(client.initiateMultipartUpload(any())).thenReturn(mock(classOf[InitiateMultipartUploadResult]))
    when(client.uploadPart(any())).thenReturn(mock(classOf[UploadPartResult]))

    objectStore.putSegment(metadata, segmentData, null, null, null, null, Optional.empty())
    verify(client, times(4)).putObject(any())
    verify(client, times(math.ceil(segmentSize.toDouble / partSize).toInt)).uploadPart(any())
  }
}
