/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import kafka.common.BaseEnum
import kafka.utils.Json
import org.apache.kafka.common.utils.Time

import scala.collection.Map
import scala.jdk.CollectionConverters._

sealed trait TopicLinkState extends BaseEnum {
  /**
    * Returns true if this topic and its metadata should be sync'ed from source.
    * Returns false if this topic link has failed or is in a paused state.
    */
  def shouldSync: Boolean
}

case object TopicLinkMirror extends TopicLinkState {
  val name: String = "Mirror"
  val shouldSync: Boolean = true
}

case object TopicLinkFailedMirror extends TopicLinkState {
  val name: String = "FailedMirror"
  val shouldSync: Boolean = false
}

case object TopicLinkStoppedMirror extends TopicLinkState {
  val name: String = "StoppedMirror"
  val shouldSync: Boolean = false
}

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
  def state: TopicLinkState

  /**
    * Returns true if this topic mirror is established
    * Returns false if this topic's mirror is stopped
    */
  def mirrorIsEstablished: Boolean

  /**
    * Returns a JSON-like map of the state's data.
    */
  def toMap: Map[String, _]

  /**
    * Converts the state into a JSON string.
    */
  def toJsonString: String = ClusterLinkTopicState.toJsonString(this)
}

object TopicLinkState {

  val states = List(TopicLinkMirror, TopicLinkFailedMirror, TopicLinkStoppedMirror)

  def fromString(name: String): TopicLinkState = {
    states.find(_.name == name).getOrElse(throw new IllegalArgumentException(s"Unknown state $name"))
  }
}

object ClusterLinkTopicState {
  /**
    * Indicates an active mirroring setup, where the local topic is the destination.
    *
    * @param linkName the name of the cluster link that the topic mirrors from
    * @param timeMs the time, in milliseconds, at which the state transition occurred
    */
  case class Mirror(linkName: String,
                    timeMs: Long = Time.SYSTEM.milliseconds())
      extends ClusterLinkTopicState {

    override def state: TopicLinkState = TopicLinkMirror

    override def mirrorIsEstablished: Boolean = true

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

    override def state: TopicLinkState = TopicLinkFailedMirror

    override def mirrorIsEstablished: Boolean = true

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
    * local topic was the destination mirror for a remote source but a fail-over event occurred,
    * and records are produced to the local topic. The local and remote topic data will have
    * diverged, as there may be unreplicated records on the remote topic, and newly appended
    * records on the local topic. Therefore, when topic mirroring is stopped, the local log-end
    * offsets for every partition are recorded so that if mirroring is ever restarted, then it's
    * known at which point the local and remote topics are synchronized.
    *
    * @param linkName the name of the cluster link that the topic mirrors from
    * @param logEndOffsets the log end offsets of every partition, in ascending partition order
    * @param timeMs the time, in milliseconds, at which the state transition occurred
    */
  case class StoppedMirror(linkName: String,
                           logEndOffsets: Seq[Long],
                           timeMs: Long = Time.SYSTEM.milliseconds())
      extends ClusterLinkTopicState {

    override def state: TopicLinkState = TopicLinkStoppedMirror

    override def mirrorIsEstablished: Boolean = false

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
    Json.encodeAsString(Map(state.state.name -> state.toMap.asJava).asJava)
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

        val entries = TopicLinkState.states.map(key => key -> jsonObj.get(key.name)).filter(_._2.isDefined)
        if (entries.size != 1)
          throw new IllegalStateException("Invalid cluster link topic state(s)")

        val (key, jsonOpt) = entries.head match {
          case (k, v) => (k, v.get.asJsonObject)
        }

        def validateVersion(expectedVersion: Int, actualVersion: Int): Unit =
          if (expectedVersion != actualVersion)
            throw new IllegalStateException(s"Unexpected version '$expectedVersion', actual version '$actualVersion'")

        key match {
          case TopicLinkMirror =>
            validateVersion(1, jsonOpt("version").to[Int])
            val timeMs = jsonOpt("time_ms").to[Long]
            val linkName = jsonOpt("link_name").to[String]
            Mirror(linkName, timeMs)
          case TopicLinkFailedMirror =>
            validateVersion(1, jsonOpt("version").to[Int])
            val timeMs = jsonOpt("time_ms").to[Long]
            val linkName = jsonOpt("link_name").to[String]
            FailedMirror(linkName, timeMs)
          case TopicLinkStoppedMirror =>
            validateVersion(1, jsonOpt("version").to[Int])
            val timeMs = jsonOpt("time_ms").to[Long]
            val linkName = jsonOpt("link_name").to[String]
            val logEndOffsets = jsonOpt("log_end_offsets").to[Seq[Long]]
            StoppedMirror(linkName, logEndOffsets, timeMs)
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
}
