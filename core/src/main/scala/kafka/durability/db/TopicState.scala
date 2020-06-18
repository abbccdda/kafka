/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability.db

import com.google.flatbuffers.FlatBufferBuilder
import kafka.durability.db.serdes.TopicInfo

import scala.collection.mutable

/**
 * Class TopicState represents durability state of a topic.
 * These state information are periodically check pointed in durability database.
 *
 * @param topic is TopicName.
 * @param partitions array of PartitionInfo.
 */
private[db] class TopicState(val topic: String, val partitions: mutable.HashMap[Int, PartitionState]) {
  def serialize(builder: FlatBufferBuilder): Int = this.synchronized {
    val partitionsOffset = partitions.values.map { v => v.serialize(builder) }
    val offset = TopicInfo.createPartitionsVector(builder, partitionsOffset.toArray)
    val topicOffset = builder.createString(topic)
    TopicInfo.createTopicInfo(builder, topicOffset, offset)
  }

  override def equals(obj: Any): Boolean = obj match {
    case that: TopicState =>
      topic == that.topic &&
      partitions.equals(that.partitions)
    case _ => false
  }

  override def hashCode(): Int = topic.hashCode
}

object TopicState {
  /**
   * Default constructor.
   * @param topic name
   * @param partitions partition state map based on its id
   * @return TopicState
   */
  def apply(topic: String, partitions: mutable.HashMap[Int, PartitionState]): TopicState =
    new TopicState(topic, partitions)

  /**
   * Construct from on-disk TopicInfo object.
   * @param state TopicInfo
   * @return TopicState
   */
  def apply(state: TopicInfo): TopicState = {
    val partitions = mutable.HashMap[Int, PartitionState]()
    for (ii <- 0 to state.partitionsLength() - 1)
      partitions.put(state.partitions(ii).partition(), PartitionState(state.partitions(ii)))
    new TopicState(state.topic(), partitions)
  }
}
