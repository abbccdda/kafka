/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util.{Arrays, Collections}
import java.util.concurrent._

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.junit.{After, Test}
import org.junit.Assert._

class ClusterLinkMetadataTest {

  private val linkName = "testLink"
  private val brokerConfig = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
  private val metadata = new ClusterLinkMetadata(brokerConfig, linkName, 100, 60000)
  private var executor: ExecutorService = _

  @After
  def tearDown(): Unit = {
    metadata.close()
    if (executor != null) {
      executor.shutdownNow()
    }
  }

  @Test
  def testRefreshListener(): Unit = {
    var requestCount = 0
    val listener = new MetadataRefreshListener {
      override def onMetadataRequestUpdate(): Unit = {
        requestCount += 1
      }
    }
    metadata.setRefreshListener(listener)
    (1 to 2).foreach { i =>
      metadata.requestUpdate()
      assertEquals(i, requestCount)
    }
  }

  /**
    * Verify that metadata lock is not held when refresh listener is invoked
    */
  @Test
  def testRefreshListenerSynchronization(): Unit = {
    val latch = new CountDownLatch(1)
    executor = Executors.newSingleThreadExecutor

    val listener = new MetadataRefreshListener {
      override def onMetadataRequestUpdate(): Unit = {
        executor.submit(() => {
          metadata synchronized {
            latch.countDown()
          }
        }, 0)
        latch.await(10, TimeUnit.SECONDS)
      }
    }
    metadata.setRefreshListener(listener)
    metadata.requestUpdate()
    assertEquals(0, latch.getCount)
  }

  @Test
  def testMetadataTopics(): Unit = {
    val nowMs = System.currentTimeMillis()
    assertFalse(metadata.retainTopic("test", isInternal = false, nowMs))
    assertEquals(Collections.emptyList, metadata.newMetadataRequestBuilder().topics())
    metadata.setTopics(Set("test"))
    assertEquals(Collections.singletonList("test"), metadata.newMetadataRequestBuilder().topics())
    assertTrue(metadata.retainTopic("test", isInternal = false, nowMs))
    metadata.setTopics(Set("test1", "test2"))
    assertEquals(Arrays.asList("test1", "test2"), metadata.newMetadataRequestBuilder().topics())
    assertFalse(metadata.retainTopic("test", isInternal = false, nowMs))
    assertTrue(metadata.retainTopic("test1", isInternal = false, nowMs))
    assertTrue(metadata.retainTopic("test2", isInternal = false, nowMs))
    metadata.setTopics(Set())
    assertEquals(Collections.emptyList, metadata.newMetadataRequestBuilder().topics())
    assertFalse(metadata.retainTopic("test1", isInternal = false, nowMs))
    assertFalse(metadata.retainTopic("test2", isInternal = false, nowMs))
  }
}
