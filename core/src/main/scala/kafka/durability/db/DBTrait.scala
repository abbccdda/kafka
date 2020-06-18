/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability.db

import java.nio.ByteBuffer

import kafka.durability.db.serdes.{Database, Header}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable
import java.io.{File, IOException}

import com.google.flatbuffers.FlatBufferBuilder
import kafka.durability.exceptions.{DurabilityDBNotReadyException, DurabilityObjectNotFoundException}

/**
 * Trait 'DBTrait' provides database api which will be used for updating, fetching, check pointing and
 * recovering, durability state related information for entire partitions in the broker.
 * The api related to access the database storage (ex File) are implemented by class which extends the trait.
 * This represents the single database which holds durability state information for all the partitions in the broker.
 *
 * Synchronization. Audit jobs are slow moving jobs, keeping simplicity in mind all operations which can be affected
 * by write/update operation are done under db lock.
 *
 * DB API are fat APIs, which means the application will perform fine level updates of different
 * properties and will add/update at coarse granularity (ex partitionState, topicState).
 */
trait DbTrait {
  /**
   * Initial assumed size of the byteBuffer to hold entire persistent state of the database. if needed more it will
   * be dynamically allocated by FlatBufferBuilder.
   */
  private val DB_BUFFER_INIT_LENGTH = 1024 * 1024

  /**
   * DurabilityEventsTopicPartition count.
   */
  private[db] val DURABILITY_EVENTS_TOPIC_PARTITION_COUNT = 50

  /**
   * Status of the database. Until and unless it's in online state, none of the api access will be allowed.
   * Currently we are not making status information persistent, it's only used to track recovery of the data base on start.
   */
  @volatile private[db] var status: DbStatus.DbStatus  = DbStatus.Init

  /**
   * The 'topicStates' is the map for accessing topicState object. They are loaded on startup and are always updated
   * in-memory. They are persisted on check pointing.
   */
  private[db] val topicStates = mutable.HashMap[String, TopicState]()

  /**
   * The base path of the durability folder which will be used to contain all the durability state and database related
   * files.
   */
  val dir: File

  /**
   * The database header. On initialization, recovery from the persistent store will also initialize the last stored
   * header information.
   */
  protected[db] var header: DbHeader

  /**
   * Database storage specific 'checkpoint' method, serializes and stores entire durability state. This method needs
   * to be synchronized under class level lock. The call to checkpoint is controlled via external caller.
   *
   * @throws: IOException.
   */
  @throws[IOException]
  def checkpoint(): Unit

  /**
   * Recovers the header from the Header object created on deserialization of byteBuffer.
   *
   * @param hdr flat buffer generated class for the header.
   */
  private[db] def recoverHeader(hdr: Header): DbHeader = {
    val committedOffsets = Array.fill( DURABILITY_EVENTS_TOPIC_PARTITION_COUNT)(0L)
    for (ii <- 0 to hdr.durabilityPartitionsOffsetsLength() - 1)
      committedOffsets.update(ii, hdr.durabilityPartitionsOffsets(ii))
    DbHeader(hdr.version(), hdr.durabilityRunId(), committedOffsets)
  }

  /**
   * Method 'recover' is used for restoring the durability database state from the persisted database.
   * This method is dependent on choice of the storage. Should be synchronized under class level lock. At the end
   * of recovery, all the in-memory states like 'topicStates', 'header' etc will be initialized with last stored
   * value. The database state is set to Online after successful recovery.
   *
   * At the successful recovery, the status is changed to online and database will be ready to handle api requests.
   *
   * @throws IOException
   */
  @throws[IOException]
  def recover(): Unit

  /**
   * Method which provides readiness check for db before applying function (func: => T).
   * @param fun call by name param which evaluates in lazy manner.
   * @tparam T return type
   * @return return value of type T
   *
   * @throws DurabilityDBNotReadyException if db not online.
   */
  @throws[DurabilityDBNotReadyException]
  private def withReadinessCheck[T](fun: => T): T = {
    status match {
      case (DbStatus.Online) => fun
      case _ => throw new DurabilityDBNotReadyException("DurabilityDB is not yet ready: " + status)
    }
  }

  /**
   * Fetches the latest known partition state for a given topic partition.
   *
   * @param: id of topic partition
   * @returns: PartitionState of the id else None.
   *
   * @thows DurabilityDBNotReadyException
   */
  @throws[DurabilityDBNotReadyException]
  def fetchPartitionState(id: TopicPartition): Option[PartitionState] = this.synchronized(
    withReadinessCheck(topicStates.get(id.topic()).map(x => x.partitions.get(id.partition()).get)))

  /**
   * Adds new topic partition with initial PartitionState. It is assumed that topic has already
   * been added to the db. Needs to be synchronized against recovery and check pointing.
   * If the id exists, it will be overwritten and previous value will be returned.
   *
   * @param id is the topic partition.
   * @param state is the initial partition state.
   *
   * @return previous partitionState if already present else None
   *
   * @throw DurabilityDBNotReadyException if db is not online.
   * @throws DurabilityObjectNotFoundException if topic itself is not part of db.
   */
  @throws[DurabilityDBNotReadyException]
  @throws[DurabilityObjectNotFoundException]
  def addPartition(id: TopicPartition, state: PartitionState): Option[PartitionState] = this.synchronized {
    withReadinessCheck(
        if (topicStates.contains(id.topic())) topicStates.get(id.topic()).get.partitions.put(id.partition(), state)
        else throw new DurabilityObjectNotFoundException("Topic: " + id.topic() + " not found in database")
    )
  }

  /**
   * 'addTopic' adds a topic to the db. The topicState may be initialized with retention params and
   * any partitionState if ready.
   * @param topic
   * @param state
   * @return None in general or previous state, if already exists.
   *
   * @throws  DurabilityDBNotReadyException id db if not ready.
   */
  @throws[DurabilityDBNotReadyException]
  def addTopic(topic: String, state: TopicState): Option[TopicState] = this.synchronized {
    withReadinessCheck(topicStates.put(topic, state))
  }

  /**
   * Serializes entire database persistent information in a single call. The current design assumes file structure consisting
   * of flat buffer for the header followed by single flat buffer for entire partition state. The advantage of this approach
   * is to reduce the number of I/O done while reading and check pointing the state. We never do random query on the disk
   * image of a partition state, rather read entire db in in one go. The caller need to make of concurrency.
   *
   * @return byteBuffer containing serialized state of the entire db.
   */
  protected[db] def serialize(): ByteBuffer = {
    val builder = new FlatBufferBuilder(DB_BUFFER_INIT_LENGTH).forceDefaults(true)

    val topicIdx = topicStates.values.map(_.serialize(builder))
    val topicOffsets = Database.createTopicsVector(builder, topicIdx.toArray)

    val hdrOffset = header.serialize(builder)

    val end = Database.createDatabase(builder, hdrOffset, topicOffsets)
    builder.finish(end)
    Database.getRootAsDatabase(builder.dataBuffer()).getByteBuffer.duplicate()
  }

  /**
   * Recovers the entire database from the serialized buffer containing the database image. the caller need to make
   * sure of concurrency.
   *
   * @param buffer containing serialized state of the entire database.
   */
  protected[db] def deserialize(buffer: ByteBuffer): Unit = {
    val database = Database.getRootAsDatabase(buffer)
    val hdr = database.header()
    header = recoverHeader(hdr)

    for (ii <- 0 to database.topicsLength() - 1) {
      val topicState = TopicState(database.topics(ii))
      topicStates.put(topicState.topic, topicState)
    }
  }

  /**
   * if true then only we will let access to the api.
   *
   * @return true if online else false.
   */
  def isOnline(): Boolean = status == DbStatus.Online

  /**
   * Updates the durability topic partition's offset. All the updates are in-memory and periodically check pointed to db.
   *
   * @throw DurabilityDBNotReadyException when database is not online.
   * @throw IndexOutOfBoundException when partition is not allocated.
   */
  @throws[DurabilityDBNotReadyException]
  @throws[IndexOutOfBoundsException]
  def updateDurabilityTopicPartitionOffset(partition: Int, offset: Long): Unit = this.synchronized {
    withReadinessCheck(header.offsets(partition) = offset)
  }

  /**
   * Fetches the latest durability events topic partition's offset mapping.
   *
   *
   * @returns the list of committed offset in order of partition's number.
   *
   * @throw DurabilityDBNotReadyException when database is not online.
   */
  def getDurabilityTopicPartitionOffsets(): Seq[Long] = this.synchronized {
    withReadinessCheck(header.offsets.toSeq)
  }

  /**
   * Close the db. It checkpoints.
   */
  @throws[IOException]
  def close(): Unit = checkpoint()
}

/**
 * Database header information.
 * @param version
 * @param durabilityRunCounter
 * @param offsets
 */
case class DbHeader(version: Int, var durabilityRunCounter: Int, var offsets: Array[Long]) {
  def serialize(builder: FlatBufferBuilder): Int = {
    val offset = Header.createDurabilityPartitionsOffsetsVector(builder, offsets)

    Header.startHeader(builder)
    Header.addVersion(builder, version)
    Header.addDurabilityRunId(builder, durabilityRunCounter)
    Header.addDurabilityPartitionsOffsets(builder, offset)
    Header.endHeader(builder)
  }

  override def equals(that: Any): Boolean = that match {
    case that: DbHeader =>
      version == that.version &&
        durabilityRunCounter == that.durabilityRunCounter &&
        offsets.sameElements(that.offsets)
    case _ => false
  }
  // hashCode for header may not be  needed, but if needed only immutable param version should
  // be sufficient.
  override def hashCode(): Int = version

  override def toString: String = "{" + version + ", " + durabilityRunCounter + ", " + offsets.mkString(",") + "}"
}

/**
 * Enum for representing various dbStatus.
 */
object DbStatus extends Enumeration {
  type DbStatus = Value
  val Online = Value(0x01.toByte)
  val Init = Value(0x02.toByte)
}
