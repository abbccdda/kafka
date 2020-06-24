package kafka.tier.store

import java.io.File
import java.nio.ByteBuffer
import java.util.{Optional, UUID}

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.PutObjectRequest
import kafka.tier.TopicIdPartition
import org.junit.Assert._
import org.junit.Test
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Captor
import org.mockito.Mockito.{mock, times, verify, when}

import scala.jdk.CollectionConverters._

class S3TierObjectStoreTest {
  @Test
  def testSinglePut(): Unit = {
    val segmentSize = 100

    val client = mock(classOf[AmazonS3])
    when(client.getBucketLocation("bucket")).thenReturn("region")
    val config = S3TierObjectStoreConfig.createWithEmptyClusterIdBrokerId("bucket", "region", "credFilePath",
      "endpoint", "signer", "sseAlgorithm", "", 0, "", "")
    val objectStore = new S3TierObjectStore(client, config)
    val metadata = new TierObjectStore.ObjectMetadata(new TopicIdPartition("foo", UUID.randomUUID, 0), UUID.randomUUID, 0, 0, false, false, false)
    val segmentData = mock(classOf[File])

    when(segmentData.length).thenReturn(segmentSize)
    objectStore.putSegment(metadata, segmentData, null, null, Optional.empty(), Optional.empty(), Optional.empty())
    // expect 3 `put` calls: segment, offset index, and time index
    verify(client, times(3)).putObject(any())
  }

  @Test
  def testSinglePutWithAbortedTxns(): Unit = {
    val segmentSize = 100

    val client = mock(classOf[AmazonS3])
    when(client.getBucketLocation("bucket")).thenReturn("region")
    val config = S3TierObjectStoreConfig.createWithEmptyClusterIdBrokerId("bucket", "region", "credFilePath",
      "endpoint", "signer", "sseAlgorithm", "", 0, "", "")
    val objectStore = new S3TierObjectStore(client, config)
    val metadata = new TierObjectStore.ObjectMetadata(new TopicIdPartition("foo", UUID.randomUUID, 0), UUID.randomUUID, 0, 0, true, false, false)
    val segmentData = mock(classOf[File])

    when(segmentData.length).thenReturn(segmentSize)
    objectStore.putSegment(metadata, segmentData, null, null, Optional.empty(), Optional.of(ByteBuffer.allocate(0)), Optional.empty())
    // expect 4 `put` calls: segment, offset index, time index, and aborted transactions
    verify(client, times(4)).putObject(any())
  }

  @Test
  def testSinglePutProducerStateEpochState(): Unit = {
    val segmentSize = 100

    val client = mock(classOf[AmazonS3])
    when(client.getBucketLocation("bucket")).thenReturn("region")
    val config = S3TierObjectStoreConfig.createWithEmptyClusterIdBrokerId("bucket", "region", "credFilePath", "endpoint", "signer", "sseAlgorithm", "", 0, "", "")
    val objectStore = new S3TierObjectStore(client, config)
    val metadata = new TierObjectStore.ObjectMetadata(new TopicIdPartition("foo", UUID.randomUUID, 0), UUID.randomUUID, 0, 0, true, true, true)
    val segmentData = mock(classOf[File])

    @Captor
    val captor = ArgumentCaptor.forClass(classOf[PutObjectRequest])
    when(segmentData.length).thenReturn(segmentSize)
    val producerIndexLength = 100
    val producerIndexBuf = ByteBuffer.allocate(producerIndexLength)
    producerIndexBuf.limit(producerIndexLength)

    val epochLength = 100
    val epochBuf = ByteBuffer.allocate(epochLength)
    epochBuf.limit(epochLength)
    objectStore.putSegment(metadata, segmentData, null, null, Optional.of(segmentData), Optional.of(producerIndexBuf), Optional.of(epochBuf))
    // expect 6 `put` calls: segment, offset index, time index, transaction index, producer snapshot and epoch state
    verify(client, times(6)).putObject(captor.capture())
    assertEquals(producerIndexLength,
      captor
      .getAllValues
      .asScala.map(_.asInstanceOf[PutObjectRequest])
      .find(_.getKey.contains(".transaction-index"))
      .get
      .getMetadata
      .getContentLength)

  }
}
