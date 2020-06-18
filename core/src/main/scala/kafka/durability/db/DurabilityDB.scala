/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability.db

import java.io.{File, FileNotFoundException, IOException}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.FileChannel
import java.nio.file.{NoSuchFileException, StandardOpenOption}

import org.apache.kafka.common.utils.Utils

/**
 * DurabilityDB is the main implementation of the DBTrait. It's an in-memory db which uses flat file for check pointing
 * and recovery of its state. It uses flat file, 'dbFile' to checkpoint entire database state in serialized(flat buffer)
 * form. The checkpoint interface is controlled by external caller.
 * On startup, it recovers it's in-memory state from the check pointed file.
 */

private[db] class DurabilityDB (path: File) extends DbTrait {
  private[db] val dbFile = new File(path, "durability.db")
  private[db] val tmpFile = new File(path, "durability.db.tmp")

  private val version: Int = 1

  /**
   * 'header' maintains the in-memory state of the db header information. Gets initialized at end of the recover
   * call. Not accessible outside DB class.
   */
  override protected[db] var header: DbHeader = null

  /**
   * Check points the db state in flat file after serializing entire persistent state using flat buffers.
   *
   * @throws IOException on file issues.
   */
  @throws[IOException]
  override def checkpoint(): Unit = this.synchronized {
    val channel: FileChannel = FileChannel.open(tmpFile.toPath, StandardOpenOption.CREATE, StandardOpenOption.READ,
      StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);

    val buffer = serialize()
    Utils.writeFully(channel, 0, buffer)
    channel.force(true)
    Utils.atomicMoveWithFallback(tmpFile.toPath, dbFile.toPath)
    channel.close()
  }

  /**
   * Directory path for durability db contents.
   */
  override val dir: File = path

  /**
   * Recovers the in-memory state of the database from last check pointed state. If file not found than the recovery state
   * is initializes to an empty state.
   * @throws IOException
   */
  @throws[IOException]
  override def recover(): Unit = this.synchronized{
    var channel: FileChannel = null
    try {
      channel = FileChannel.open(dbFile.toPath, StandardOpenOption.READ)
      val buffer: ByteBuffer = ByteBuffer.allocate(channel.size().toInt).order(ByteOrder.LITTLE_ENDIAN)
      Utils.readFully(channel, buffer, 0)
      buffer.flip()
      deserialize(buffer)
      status = DbStatus.Online
    } catch {
      case _: FileNotFoundException | _: NoSuchFileException => initDB()
      case ex: Exception => throw ex
    } finally {
      if (channel != null) channel.close()
    }
  }

  /**
   * Initializes db to an empty state. This will be called during very first invocation.
   */
  private def initDB(): Unit = {
    header = DbHeader(version, 1, Array.fill(DURABILITY_EVENTS_TOPIC_PARTITION_COUNT)(0L))
    status = DbStatus.Online
    checkpoint()
  }
}

/**
 * Durability DB object. Any static method can be placed here.
 */
object DurabilityDB {
  @throws[IOException]
  @throws[FileNotFoundException]
  @throws[IllegalAccessError]
  def apply(path: File): DurabilityDB = {
    val db = new DurabilityDB(path)
    db.recover()
    db
  }
}
