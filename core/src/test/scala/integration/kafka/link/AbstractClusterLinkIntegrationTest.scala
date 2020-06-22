/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.link

import java.util.concurrent.TimeUnit
import java.util.{Optional, Properties}

import com.yammer.metrics.core.{Gauge, Histogram, Meter, Metric}
import kafka.metrics.KafkaYammerMetrics
import kafka.server._
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.test.{IntegrationTest, TestUtils => JTestUtils}
import org.junit.Assert._
import org.junit.experimental.categories.Category
import org.junit.{After, Before}

import scala.collection.{Map, Seq, mutable}
import scala.jdk.CollectionConverters._

@Category(Array(classOf[IntegrationTest]))
class AbstractClusterLinkIntegrationTest extends Logging {

  val sourceCluster = new ClusterLinkTestHarness(SecurityProtocol.SASL_SSL)
  val destCluster = new ClusterLinkTestHarness(SecurityProtocol.SASL_PLAINTEXT)
  val topic = "linkedTopic"
  var numPartitions = 4
  val linkName = "testLink"
  val producedRecords = mutable.Buffer[ProducerRecord[Array[Byte], Array[Byte]]]()
  var nextProduceIndex: Int = 0

  @Before
  def setUp(): Unit = {
    sourceCluster.setUp()
    destCluster.setUp()
  }

  @After
  def tearDown(): Unit = {
    destCluster.tearDown()
    sourceCluster.tearDown()
  }

  protected def verifyMirror(topic: String): Unit = {
    waitForMirror(topic)
    destCluster.unlinkTopic(topic, linkName)
    consume(destCluster, topic)
  }

  protected def waitForMirror(topic: String,
                              servers: Seq[KafkaServer] = destCluster.servers,
                              maxWaitMs: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Unit = {
    val offsetsByPartition = (0 until numPartitions).map { i =>
      i -> producedRecords.count(_.partition == i).toLong
    }.toMap
    partitions.foreach { tp =>
      val expectedOffset = offsetsByPartition.getOrElse(tp.partition, 0L)
      val leader = TestUtils.waitUntilLeaderIsKnown(servers, tp)
      servers.foreach { server =>
        server.replicaManager.nonOfflinePartition(tp).foreach { _ =>
          val (offset, _) = TestUtils.computeUntilTrue(logEndOffset(server, tp).getOrElse(-1), maxWaitMs)(_ == expectedOffset)
          assertEquals(s"Unexpected offset on broker ${server.config.brokerId} leader $leader", expectedOffset, offset)
        }
      }
    }
  }

  protected def logEndOffset(server: KafkaServer, tp: TopicPartition): Option[Long] = {
    server.replicaManager.getLog(tp).map(_.localLogEndOffset)
  }

  protected def partitions: Seq[TopicPartition] = (0 until numPartitions).map { i => new TopicPartition(topic, i) }

  protected def produceToSourceCluster(numRecords: Int): Unit = {
    val producer = sourceCluster.createProducer()
    produceRecords(producer, topic, numRecords)
    producer.close()
  }

  protected def produceRecords(producer: KafkaProducer[Array[Byte], Array[Byte]], topic: String, numRecords: Int): Unit = {
    val numPartitions = producer.partitionsFor(topic).size()
    assertTrue(s"Invalid partition count $numPartitions", numPartitions > 0)
    val futures = (0 until numRecords).map { _ =>
      val index = nextProduceIndex
      nextProduceIndex += 1
      val record = new ProducerRecord(topic, index % numPartitions, null, s"key $index".getBytes, s"value $index".getBytes)
      producedRecords += record
      producer.send(record)
    }
    futures.foreach(_.get(15, TimeUnit.SECONDS))
  }

  protected def produceUntil(producer: KafkaProducer[Array[Byte], Array[Byte]], condition: () => Boolean, errorMessage: String): Unit = {
    var iteration = 0
    do {
      iteration += 1
      produceRecords(producer, topic, 20)
    } while (!condition.apply() && iteration < 100)
    assertTrue(errorMessage, condition.apply())
  }

  protected def consume(cluster: ClusterLinkTestHarness, topic: String): Unit = {
    val consumer = cluster.createConsumer()
    consumer.assign(partitions.asJava)
    consumeRecords(consumer)
    consumer.close()
  }

  protected def commitOffsets(cluster: ClusterLinkTestHarness, topic: String, partition: Int, offset: Long, consumerGroup: String): Unit = {
    val consumerProps = new Properties()
    consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup)
    val consumer = cluster.createConsumer(configOverrides = consumerProps)
    val offsetEntries = Map[TopicPartition, OffsetAndMetadata](
      new TopicPartition(topic, partition) -> new OffsetAndMetadata(offset, Optional.empty(), "")
    )
    consumer.commitSync(offsetEntries.asJava)
    consumer.close()
  }

  protected def consumeRecords(consumer: KafkaConsumer[Array[Byte], Array[Byte]]): Unit = {
    val consumedRecords = TestUtils.consumeRecords(consumer, producedRecords.size, waitTimeMs = 20000)
    val producedByPartition = producedRecords.groupBy(_.partition)
    val consumedByPartition = consumedRecords.groupBy(_.partition)
    producedByPartition.foreach { case (partition, produced) =>
      val consumed = consumedByPartition(partition)
      assertEquals(produced.size, consumed.size)
      produced.zipWithIndex.foreach { case (producedRecord, i) =>
        val consumedRecord = consumed(i)
        assertEquals(i, consumedRecord.offset)
        assertEquals(topic, consumedRecord.topic)
        assertEquals(new String(producedRecord.key), new String(consumedRecord.key))
        assertEquals(new String(producedRecord.value), new String(consumedRecord.value))
      }
    }
  }

  protected def kafkaMetricMaxValue(server: KafkaServer, name: String, group: String): Double = {
    val values = server.metrics.metrics().asScala
      .filter { case (metricName, _) => metricName.name == name && metricName.group == group && metricName.tags.get("link-name") == linkName }
      .map(_._2.metricValue().asInstanceOf[Double])
    assertTrue(s"Metric does not exist: $group:$name", values.nonEmpty)
    values.max
  }

  protected def yammerMetricMaxValue(prefix: String, linkOpt: Option[String] = Some(linkName)): Double = {
    def matches(mbeanName: String) = mbeanName.startsWith(prefix) &&
      linkOpt.forall { name => mbeanName.contains(s"link-name=$name") }

    val values = KafkaYammerMetrics.defaultRegistry().allMetrics().asScala
      .filter { case (metricName, _) => matches(metricName.getMBeanName) }
      .values
    assertTrue(s"Metric does not exist: $prefix", values.nonEmpty)
    values.map(yammerMetricValue).max
  }

  private def yammerMetricValue(metric: Metric): Double = {
    metric match {
      case m: Meter => m.count.toDouble
      case m: Histogram => m.max
      case m: Gauge[_] => m.value.toString.toDouble
      case m => throw new IllegalArgumentException(s"Unexpected broker metric of class ${m.getClass}")
    }
  }
}
