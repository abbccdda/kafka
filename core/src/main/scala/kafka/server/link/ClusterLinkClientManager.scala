/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.clients.admin.ConfluentAdmin

import scala.collection.Set
import scala.collection.mutable

/**
  * The ClusterLinkClientManager is responsible for managing an admin client instance for the
  * cluster link, and manages periodic tasks that may use the admin client.
  *
  * Thread safety:
  *   - ClusterLinkClientManager is thread-safe.
  */
class ClusterLinkClientManager(val linkName: String,
                               private var config: ClusterLinkConfig,
                               private val adminFactory: ClusterLinkConfig => ConfluentAdmin) extends Logging {

  @volatile private var admin: Option[ConfluentAdmin] = None

  // Protects `topics` and `config`.
  private val lock = new Object

  // Contains the set of linked topics that this client manager is responsible for, i.e. the set of topics
  // for which the leader of a topic's first partition is on the local broker.
  private val topics = mutable.Set[String]()

  def startup(): Unit = {
    setAdmin(Some(adminFactory(config)))
  }

  def shutdown(): Unit = {
    setAdmin(None)
  }

  def reconfigure(newConfig: ClusterLinkConfig): Unit = {
    lock synchronized {
      config = newConfig
      setAdmin(Some(adminFactory(config)))
    }
  }

  def addTopics(addTopics: Set[String]): Unit = {
    lock synchronized {
      addTopics.foreach { topic =>
        if (topics.add(topic))
          debug(s"Added topic '$topic' for link '$linkName'")
      }
    }
  }

  def removeTopics(removeTopics: Set[String]): Unit = {
    lock synchronized {
      removeTopics.foreach { topic =>
        if (topics.remove(topic))
          debug(s"Removed topic '$topic' for link '$linkName'")
      }
    }
  }

  def getTopics: Set[String] = lock synchronized {
    topics.toSet
  }

  def getAdmin: ConfluentAdmin = admin.getOrElse(throw new IllegalStateException(s"Client manager for $linkName not initialized"))

  private def setAdmin(newAdmin: Option[ConfluentAdmin]): Unit = {
    val oldAdmin = admin
    admin = newAdmin
    oldAdmin.foreach(a => CoreUtils.swallow(a.close(), this))
  }

}
