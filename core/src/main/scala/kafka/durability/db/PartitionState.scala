/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability.db

import com.google.flatbuffers.FlatBufferBuilder
import kafka.durability.db.serdes.{EpochChain, PartitionInfo}

import scala.collection.mutable

/**
 * Class 'PartitionState' represents both persistent and in-memory information related to perform
 * durability audit of a topic partition.
 * TBd in-memory state information will be added based on further work.
 *
 * @param partition            is partition id
 * @param startOffset          is partition's start offset
 * @param highWaterMark        is partition's high water mark
 * @param externalLostMessages is partition's externalLostMessages found in current audit run.
 * @param totalMessages        is partition's totalMessages evaluated in current audit run.
 * @param retentionMs          is retention time in ms.
 * @param retentionSz          is retention sz in bytes.
 * @param epochChain           is partition's epoch chain.
 */
private[db] class PartitionState (val partition: Int,
                                  var startOffset: Long,
                                  var highWaterMark: Long,
                                  var externalLostMessages: Long,
                                  var totalMessages: Long,
                                  var retentionMs: Long,
                                  var retentionSz: Long,
                                  val epochChain: mutable.HashMap[Int, Long]) {
  def serialize(builder: FlatBufferBuilder): Int = this.synchronized {
    PartitionInfo.startEpochChainVector(builder, epochChain.size)
    epochChain.foreach { case (k, v) => { EpochChain.createEpochChain(builder, k, v) } }
    val epochChainsOffset = builder.endVector()

    PartitionInfo.createPartitionInfo(builder, partition, startOffset,
      highWaterMark, externalLostMessages, totalMessages, retentionMs,
      retentionSz, epochChainsOffset
    )
  }

  override def equals(obj: Any): Boolean = obj match {
    case that: PartitionState =>
      partition == that.partition &&
      startOffset == that.startOffset &&
      highWaterMark == that.highWaterMark &&
      externalLostMessages == that.externalLostMessages &&
      totalMessages == that.totalMessages &&
      retentionMs == that.retentionMs &&
      retentionSz == that.retentionSz &&
      epochChain == epochChain
    case _ => false
  }

  /**
   * Simple hashcode based on unique partition id. This is in context to a given topic.
   * @return int
   */
  override def hashCode(): Int = partition
}

object PartitionState {
  /**
   *  Construct using 'PartitionState' fields.
   *
   *  @return PartitionState
   */
  def apply(partition: Int,
            startOffset: Long,
            highWaterMark: Long,
            externalLostMessages: Long,
            totalMessages: Long,
            retentionMs: Long,
            retentionSz: Long,
            epochChain: mutable.HashMap[Int, Long]): PartitionState =
    new PartitionState(partition, startOffset, highWaterMark, externalLostMessages, totalMessages,
      retentionMs, retentionSz, epochChain)

  /**
   * Construct using PartitionInfo - on-disk
   */
  def apply(info: PartitionInfo): PartitionState = fromPartitionInfo(info)

  /**
   * Provides static access to method 'PartitionInfo' to deserialize into 'PartitionState', from serialized
   * persistent state.
   *
   * @param state is PartitionInfo, the durable data of partitionState.
   *
   * @returns PartitionState
   */
  def fromPartitionInfo(state: PartitionInfo): PartitionState = {
    val epochChainMap = mutable.HashMap[Int, Long]()

    for (ii <- 0 to state.epochChainLength() -1) {
      epochChainMap.put(state.epochChain(ii).epoch(), state.epochChain(ii).start())
    }

    new PartitionState(state.partition(),
      state.startOffset(),
      state.highWaterMark(),
      state.externalLostMessages(),
      state.totalMessages(),
      state.retentionTime(),
      state.retentionSize(),
      epochChainMap)
  }
}
