/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import kafka.utils.Json
import org.apache.kafka.common.utils.Time

import scala.collection.Map
import scala.jdk.CollectionConverters._

abstract class ClusterLinkTopicState {

  /**
    * Name of the cluster link for the topic.
    * Link name is returned even if mirroring is in paused or failed state
    * to prevent updates to local topic.
    */
  def linkName: String

  /**
    * State of the topic link, persisted in ZK and propagated to brokers in LeaderAndIsrRequest
    */
  def state: String

  /**
    * Returns true if this topic and its metadata should be sync'ed from source.
    * Returns false if this topic link has failed or is in a paused state.
    */
  def shouldSync: Boolean

  /**
    * Returns a JSON-like map of the state's data.
    */
  def toMap: Map[String, _]

  /**
    * Converts the state into a JSON string.
    */
  def toJsonString: String = ClusterLinkTopicState.toJsonString(this)
}

object ClusterLinkTopicState {

  private val mirrorKey = "mirror"
  private val failedMirrorKey = "failed_mirror"
  private val stoppedMirrorKey = "stopped_mirror"
  private val stateKeys = List(mirrorKey, failedMirrorKey, stoppedMirrorKey)

  /**
    * Indicates an active mirroring setup, where the local topic is the destination.
    *
    * @param linkName the name of the cluster link that the topic mirrors from
    * @param timeMs the time, in milliseconds, at which the state transition occurred
    */
  case class Mirror(linkName: String,
                    timeMs: Long = Time.SYSTEM.milliseconds())
      extends ClusterLinkTopicState {

    override def state: String = mirrorKey

    override def shouldSync: Boolean = true

    override def toMap: Map[String, _] = {
      Map(
        "version" -> 1,
        "time_ms" -> timeMs,
        "link_name" -> linkName
      )
    }
  }

  /**
    * Indicates topic mirror where the local topic is the destination but the link has failed.
    * Active mirrors may be in this state because source topic was deleted.
    *
    * @param linkName the name of the cluster link that the topic mirrors from
    * @param timeMs the time, in milliseconds, at which the state transition occurred
    */
  case class FailedMirror(linkName: String,
                          timeMs: Long = Time.SYSTEM.milliseconds())
    extends ClusterLinkTopicState {

    override def state: String = failedMirrorKey

    override def shouldSync: Boolean = false

    override def toMap: Map[String, _] = {
      Map(
        "version" -> 1,
        "time_ms" -> timeMs,
        "link_name" -> linkName
      )
    }
  }

  /**
    * Indicates a mirror that has been stopped, i.e. the local topic is mutable and behaves as
    * a normal topic.
    *
    * Note this state is recorded for re-synchronization purposes. For example, assume that the
    * local topic was the destination but a fail-over event occurs, and records are produced to
    * the local topics. The topics will have diverged, as there may be unreplicated records on the
    * remote topic, and newly appended records on the local topic. The log end offsets for every
    * partition are recorded so that, if the remote topic is set to mirror this one, then it's
    * known to which offset the logs were in-sync.
    *
    * @param linkName the name of the cluster link that the topic mirrors from
    * @param logEndOffsets the log end offsets of every partition, in ascending partition order
    * @param timeMs the time, in milliseconds, at which the state transition occurred
    */
  case class StoppedMirror(linkName: String,
                           logEndOffsets: Seq[Long],
                           timeMs: Long = Time.SYSTEM.milliseconds())
      extends ClusterLinkTopicState {

    override def state: String = stoppedMirrorKey

    override def shouldSync: Boolean = false

    override def toMap: Map[String, _] = {
      Map(
        "version" -> 1,
        "time_ms" -> timeMs,
        "link_name" -> linkName,
        "log_end_offsets" -> logEndOffsets.asJava
      )
    }
  }

  /**
    * Constructs and returns the JSON string representing the topic state.
    *
    * @return the JSON string
    */
  def toJsonString(state: ClusterLinkTopicState): String = {
    val name = state match {
      case cfg: ClusterLinkTopicState.Mirror => mirrorKey
      case cfg: ClusterLinkTopicState.FailedMirror => failedMirrorKey
      case cfg: ClusterLinkTopicState.StoppedMirror => stoppedMirrorKey
      case _ => throw new IllegalStateException("Unexpected cluster link topic state")
    }

    Json.encodeAsString(Map(name -> state.toMap.asJava).asJava)
  }

  /**
    * Parses the JSON string, returning the corresponding topic state.
    *
    * @return the topic state
    */
  def fromJsonString(json: String): ClusterLinkTopicState = {
    Json.parseFull(json) match {
      case Some(jsonValue) =>
        val jsonObj = jsonValue.asJsonObject

        val entries = stateKeys.map(key => key -> jsonObj.get(key)).filter(_._2.isDefined)
        if (entries.size != 1)
          throw new IllegalStateException("Invalid cluster link topic state(s)")

        val (key, jsonOpt) = entries.head match {
          case (k, v) => (k, v.get.asJsonObject)
        }

        def validateVersion(expectedVersion: Int, actualVersion: Int): Unit =
          if (expectedVersion != actualVersion)
            throw new IllegalStateException(s"Unexpected version '$expectedVersion', actual version '$actualVersion'")

        key match {
          case `mirrorKey` =>
            validateVersion(1, jsonOpt("version").to[Int])
            val timeMs = jsonOpt("time_ms").to[Long]
            val linkName = jsonOpt("link_name").to[String]
            Mirror(linkName, timeMs)
          case `failedMirrorKey` =>
            validateVersion(1, jsonOpt("version").to[Int])
            val timeMs = jsonOpt("time_ms").to[Long]
            val linkName = jsonOpt("link_name").to[String]
            FailedMirror(linkName, timeMs)
          case `stoppedMirrorKey` =>
            validateVersion(1, jsonOpt("version").to[Int])
            val timeMs = jsonOpt("time_ms").to[Long]
            val linkName = jsonOpt("link_name").to[String]
            val logEndOffsets = jsonOpt("log_end_offsets").to[Seq[Long]]
            new StoppedMirror(linkName, logEndOffsets, timeMs)
          case _ =>
            // During rolling upgrade, we should ensure that some brokers with new states cannot
            // put the link into a state not known by other brokers. For now, mark this as failed.
            validateVersion(1, jsonOpt("version").to[Int])
            val timeMs = jsonOpt("time_ms").to[Long]
            val linkName = jsonOpt("link_name").to[String]
            FailedMirror(linkName, timeMs)
        }

      case None =>
        throw new IllegalStateException(s"Invalid topic state JSON: $json")
    }
  }

  def fromState(linkName: String, state: String): ClusterLinkTopicState = {
    state match {
      case `mirrorKey` => Mirror(linkName)
      case `failedMirrorKey` => FailedMirror(linkName)
      case `stoppedMirrorKey` => StoppedMirror(linkName, Seq.empty)
      case _ => FailedMirror(linkName) // If we don't understand the state, don't sync.
    }
  }
}
