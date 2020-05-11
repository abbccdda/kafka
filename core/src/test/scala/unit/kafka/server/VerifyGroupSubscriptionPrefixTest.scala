// (Copyright) [2019 - 2019] Confluent, Inc.

package kafka.server

import java.util.Properties

import kafka.api.IntegrationTestHarness
import kafka.utils.TestUtils._
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.{KafkaFuture, TopicPartition}
import org.apache.kafka.common.config.internals.ConfluentConfigs
import org.apache.kafka.common.errors.GroupSubscribedToTopicException
import org.junit.Assert.assertNull
import org.junit.Test

import scala.jdk.CollectionConverters._
import scala.collection.Seq

object VerifyGroupSubscriptionPrefixTest {
  val prefix = "lkc-12cd56g_"
}

/**
 * This is a test of a Confluent-Cloud-specific configuration option,
 * confluent.verify.group.subscription.prefix.
 */
class VerifyGroupSubscriptionPrefixTest extends IntegrationTestHarness {
  import VerifyGroupSubscriptionPrefixTest._

  override protected def brokerCount: Int = 2

  override def modifyConfigs(props: Seq[Properties]): Unit = {
    props.foreach { p =>
      p.put(ConfluentConfigs.VERIFY_GROUP_SUBSCRIPTION_PREFIX, "true")
    }
  }

  @Test
  def testSuccessfullyDeleteOffsetsWithPrefixes(): Unit = {
    // As all resources are prefixed with the tenant, deleting the offsets of the
    // unsubscribed topic must succeed.
    val group = prefix + "group"
    val subscribedTopic = prefix + "subscribed-topic"
    val unsubscribedTopic = prefix + "not-subscribed-topic"

    val offsetDeleteResult = testDeleteOffsets(group, subscribedTopic, unsubscribedTopic)

    assertFutureExceptionTypeEquals(
      offsetDeleteResult.partitionResult(new TopicPartition(subscribedTopic, 0)),
      classOf[GroupSubscribedToTopicException])
    assertFutureNull(
      offsetDeleteResult.partitionResult(new TopicPartition(unsubscribedTopic, 0)))
  }

  @Test
  def testSuccessfullyDeleteOffsetsWithoutPrefixes(): Unit = {
    // As none of the resources are prefixed with the tenant, deleting the offsets of the
    // unsubscribed topic must succeed.
    val group = "group"
    val subscribedTopic = "subscribed-topic"
    val unsubscribedTopic = "not-subscribed-topic"

    val offsetDeleteResult = testDeleteOffsets(group, subscribedTopic, unsubscribedTopic)

    assertFutureExceptionTypeEquals(
      offsetDeleteResult.partitionResult(new TopicPartition(subscribedTopic, 0)),
      classOf[GroupSubscribedToTopicException])
    assertFutureNull(
      offsetDeleteResult.partitionResult(new TopicPartition(unsubscribedTopic, 0)))
  }

  @Test
  def testUnsuccessfullyDeleteOffsets(): Unit = {
    // As the topics are not prefixed with the tenant, deleting the offsets of the
    // unsubscribed topic must fail because the group coordinator must not be aware
    // the subscribed topics.
    val group = prefix + "group"
    val subscribedTopic = "subscribed-topic"
    val unsubscribedTopic = "not-subscribed-topic"

    val offsetDeleteResult = testDeleteOffsets(group, subscribedTopic, unsubscribedTopic)

    assertFutureExceptionTypeEquals(
      offsetDeleteResult.partitionResult(new TopicPartition(subscribedTopic, 0)),
      classOf[GroupSubscribedToTopicException])
    assertFutureExceptionTypeEquals(
      offsetDeleteResult.partitionResult(new TopicPartition(unsubscribedTopic, 0)),
      classOf[GroupSubscribedToTopicException])
  }

  private def testDeleteOffsets(group: String, subscribedTopic: String, unsubscribedTopic: String): DeleteConsumerGroupOffsetsResult = {
    createTopic(subscribedTopic)
    createTopic(unsubscribedTopic)

    val producer = createProducer()
    producer.send(new ProducerRecord(subscribedTopic, 0, null, null)).get
    producer.send(new ProducerRecord(unsubscribedTopic, 0, null, null)).get

    val newConsumerConfig = new Properties(consumerConfig)
    newConsumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group)
    // Increase timeouts to avoid having a rebalance during the test
    newConsumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.MAX_VALUE.toString)
    newConsumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Defaults.GroupMaxSessionTimeoutMs.toString)
    val consumer = createConsumer(configOverrides = newConsumerConfig)
    subscribeAndWaitForRecords(subscribedTopic, consumer)
    consumer.commitSync()

    val admin = createAdminClient()
    admin.deleteConsumerGroupOffsets(group, Set(
      new TopicPartition(subscribedTopic, 0),
      new TopicPartition(unsubscribedTopic, 0)
    ).asJava)
  }

  private def assertFutureNull(future: KafkaFuture[_], message: Option[String] = None): Unit = {
    message match {
      case Some(msg) => assertNull(msg, future.get)
      case None => assertNull(future.get)
    }
  }
}
