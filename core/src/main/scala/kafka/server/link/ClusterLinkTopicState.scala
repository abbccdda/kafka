/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import kafka.utils.Json
import org.apache.kafka.common.utils.Time

import scala.collection.Map
import scala.collection.JavaConverters._

abstract class ClusterLinkTopicState {

  /**
    * Name of the active cluster link for the topic, or none if not linked.
    */
  def activeLinkName: Option[String]

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
  private val stateKeys = List(mirrorKey)

  /**
    * Indicates an active mirroring setup, where the local topic is the destination.
    *
    * @param linkName the name of the cluster link that the topic mirrors from
    * @param topic the name of the topic over the cluster link to mirror
    * @param timeMs the time, in milliseconds, at which the state transition occurred
    */
  case class Mirror(val linkName: String,
                    val timeMs: Long = Time.SYSTEM.milliseconds())
      extends ClusterLinkTopicState {

    override def activeLinkName: Option[String] = Some(linkName)

    override def toMap: Map[String, _] = {
      Map(
        "version" -> 1,
        "time_ms" -> timeMs,
        "link_name" -> linkName
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
            new Mirror(linkName, timeMs)
        }

      case None =>
        throw new IllegalStateException(s"Invalid topic state JSON: $json")
    }
  }

}
