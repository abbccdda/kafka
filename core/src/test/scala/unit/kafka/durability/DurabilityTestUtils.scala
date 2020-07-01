/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability

import java.time.Duration
import java.util
import java.util.Collections
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, Future, TimeUnit}
import java.util.function.Supplier

import kafka.durability.db.{DbTestUtils, DurabilityDB}
import kafka.durability.topic.{DurabilityTopicConfig, DurabilityTopicConsumer, DurabilityTopicManager, DurabilityTopicProducer}
import kafka.zk.AdminZkClient
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito.mock

/**
 * Various DurabilityTopic related method used for writing test cases. Most of the method will be enhanced and extended
 * in future for more accommodating future demands.
 */
object DurabilityTestUtils {
  var offset = 0

  /**
   * AdminZkClient supplier's mocker.
   */
  val adminZkClientSupplier = new Supplier[AdminZkClient] {
    override def get(): AdminZkClient = mock(classOf[AdminZkClient])
  }

  /**
   * DurabilityDB instance simulation. The sample parameter topic and partition make sure that the db has at-least one
   * topic partition state.
   * @param topic name of the topic
   * @param partition partition number
   * @return
   */
  def getDB(topic: String, partition: Int): DurabilityDB = DbTestUtils.getDbInstance(topic, partition)

  /**
   * Simulate the durability topic manager's config.
   * @return DurabilityTopicConfig
   */
  def getDurabilityConfig() = DurabilityTopicConfig(
    () => Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:80"),
    "test-topic", 50, 3, 2, "test-cluster", 10000, 6000)

  /**
   * Simulates the durability topic manager with modified consumer and producer. Consumer is modified to simulate the poll
   * method. Instead of using a topic partition the producer and consumer produce and polls on a fifo queue.
   * @param topic topic name
   * @param partition partition count
   * @param topicPartitionQueue queue for simulating topic partition
   * @param resultQueue optional queue for collection consumed messages.
   * @return
   */
  def getDurabilityTopicManager(topic: String,
                                partition: Int,
                                topicPartitionQueue: BlockingQueue[ConsumerRecords[Array[Byte], Array[Byte]]] =
                                  new ArrayBlockingQueue[ConsumerRecords[Array[Byte], Array[Byte]]](32),
                                resultQueue: Option[BlockingQueue[ConsumerRecords[Array[Byte], Array[Byte]]]] = None): DurabilityTopicManager =
    new DurabilityTopicManager(getDurabilityConfig(), getDB(topic, partition), adminZkClientSupplier) {
      override lazy val consumerProvider: DurabilityTopicConsumer = getDurabilityConsumer(topic, partition, topicPartitionQueue, resultQueue)
      override lazy val producerProvider: DurabilityTopicProducer = getDurabilityProducer(getDurabilityConfig(), topicPartitionQueue)
    }

  /**
   * Utility to convert ProducerRecord to ConsumerRecords.
   * @param record producer record.
   * @return ConsumerRecords - map of TopicPartition with mapping of list of CustomerRecord.
   */
  private def toConsumerRecord(record: ProducerRecord[Array[Byte], Array[Byte]]): ConsumerRecords[Array[Byte], Array[Byte]] = {
    val records = new util.HashMap[TopicPartition, util.List[ConsumerRecord[Array[Byte], Array[Byte]]]]()
    val crecord = new ConsumerRecord[Array[Byte], Array[Byte]](record.topic(), record.partition(), offset, record.key(), record.value())
    offset += 1
    val crecords = new util.ArrayList[ConsumerRecord[Array[Byte], Array[Byte]]]()
    crecords.add(crecord)
    records.put(new TopicPartition(record.topic(), record.partition()), crecords)
    new ConsumerRecords[Array[Byte], Array[Byte]](records)
  }

  /**
   * Simulating the durability consumer. poll from fifo queue instead of topic partition and optionally report fetched
   * records in resultQueue. Also, seek functionality is no-op right now and will be modified in future.
   *
   * @param topic name of the topic
   * @param partition count of partition
   * @param topicPartitionQueue queue for simulating topicPartitionQueue
   * @param resultQueue optional queue for reporting fetched records.
   * @return DurabilityTopicConsumer.
   */
  def getDurabilityConsumer(topic: String, partition: Int,
                            topicPartitionQueue: BlockingQueue[ConsumerRecords[Array[Byte], Array[Byte]]],
                            resultQueue: Option[BlockingQueue[ConsumerRecords[Array[Byte], Array[Byte]]]] = None): DurabilityTopicConsumer =
    new DurabilityTopicConsumer(DurabilityTestUtils.getDurabilityConfig(), DurabilityTestUtils.getDB(topic, partition)) {
      val properties = getDurabilityConfig().toProducerProperties("dummies")
      properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
      properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")

      override lazy val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](properties) {
        override def poll(timeout: Duration): ConsumerRecords[Array[Byte], Array[Byte]] = topicPartitionQueue.poll(timeout.toMillis, TimeUnit.MILLISECONDS)

        // Right now seek will be a no-op.
        override def seek(partition: TopicPartition, offset: Long): Unit = ()
      }

      override private[durability] def processRecords(records: ConsumerRecords[Array[Byte], Array[Byte]]): Unit = resultQueue match {
        case Some(queue) => if (records != null) queue.put(records)
        case _ =>
      }
  }

  /**
   * Simulating DurabilityProducer with producing to fifo queue instead of topic partition.
   * @param config durabilityProducer's config.
   * @param queue fifo for simulating topic partition.
   * @return
   */
  def getDurabilityProducer(config: DurabilityTopicConfig,
                            queue: BlockingQueue[ConsumerRecords[Array[Byte], Array[Byte]]]): DurabilityTopicProducer = new DurabilityTopicProducer(config) {

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](getDurabilityConfig().toProducerProperties("dummy")) {
      override def send(record: ProducerRecord[Array[Byte], Array[Byte]]): Future[RecordMetadata] = {
        queue.add(toConsumerRecord(record))
        mock(classOf[Future[RecordMetadata]])
      }
    }

    override lazy val get: KafkaProducer[Array[Byte], Array[Byte]] = producer
  }
}
