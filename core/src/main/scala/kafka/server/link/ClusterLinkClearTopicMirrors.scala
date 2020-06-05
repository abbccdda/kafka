/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.server.link

import java.util.UUID
import java.util.concurrent.ExecutionException

import kafka.controller.KafkaController
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.admin.{AlterMirrorsOptions, ConfluentAdmin}
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.requests.{AlterMirrorsRequest, AlterMirrorsResponse}

import scala.jdk.CollectionConverters._

/**
  * Task that clears cluster link references for a deleted cluster link.
  *
  * Note this task will shut down automatically once completed.
  */
class ClusterLinkClearTopicMirrors(linkId: UUID,
                                   scheduler: ClusterLinkScheduler,
                                   zkClient: KafkaZkClient,
                                   controller: KafkaController,
                                   localAdmin: ConfluentAdmin,
                                   completionCallback: () => Unit,
                                   topicGroupSize: Int = 100,
                                   intervalMs: Int = 5000,
                                   retryDelayMs: Int = 1000)
    extends ClusterLinkScheduler.PeriodicTask(scheduler, name = "ClearTopicMirrors", intervalMs) {

  private var topicGroups: List[Set[String]] = _

  override protected def run(): Boolean = {
    if (!zkClient.clusterLinkExists(linkId)) {
      onCompletion()
    } else if (controller.isActive) {
      processTopics()
    } else {
      true
    }
  }

  /**
    * Looks up all topics in the cluster and begins processing them for cluster link
    * removal.
    *
    * @return whether the task has completed
    */
  private def processTopics(): Boolean = {
    topicGroups = zkClient.getAllTopicsInCluster(false).grouped(topicGroupSize).toList

    processTopicsOnce()
  }

  /**
    * Handles clearing topic mirror information for the first group of topics in
    * `topicGroups`, removing the group on success.
    *
    * @return whether the task has completed
    */
  private def processTopicsOnce(): Boolean = {
    if (topicGroups.isEmpty) {
      onCompletion()
    } else {
      val links = zkClient.getClusterLinkForTopics(topicGroups.head).filter(_._2.linkId == linkId)
      if (links.nonEmpty) {
        clearTopicMirrors(links.keys.toSet)
      } else {
        topicGroups = topicGroups.drop(1)
        scheduleOnce(() => processTopicsOnce())
        false
      }
    }
  }

  /**
    * Clears the topic mirrors for the provided topics. On success, removes the topic group,
    * otherwise on failure, schedules the group to be handled again.
    *
    * @param topics the topics to clear mirror information from
    * @return whether the task has completed
    */
  private def clearTopicMirrors(topics: Set[String]): Boolean = {
    debug(s"Clearing topic mirrors for topics '$topics'")

    try {
      val ops: List[AlterMirrorsRequest.Op] = topics.map(t => new AlterMirrorsRequest.ClearTopicMirrorOp(t)).toList
      val options = new AlterMirrorsOptions()
      val result = localAdmin.alterMirrors(ops.asJava, options)
      scheduleWhenComplete(result.all, () => clearTopicMirrorsComplete(result.result))
    } catch {
      case e: Throwable =>
        debug("Encountered error while clearing topic mirrors", e)
        scheduleOnce(() => processTopicsOnce(), retryDelayMs)
    }
    false
  }

  /**
    * Called when clearing topic mirrors has completed. On success, removes the topic group,
    * otherwise on failure, schedules the group to be handled again.
    *
    * @param results the `alterMirrors()` results
    * @return whether the task has completed
    */
  private def clearTopicMirrorsComplete(results: java.util.List[KafkaFuture[AlterMirrorsResponse.Result]]): Boolean = {
    val success = results.asScala.forall { res =>
      try {
        res.get
        true
      } catch {
        case e: ExecutionException => e.getCause match {
          case ee: UnknownTopicOrPartitionException =>
            true
          case ee: Throwable =>
            warn("Encountered error while clearing topic mirrors", ee)
            false
        }
        case e: Throwable =>
          error("Encountered error while clearing topic mirrors", e)
          false
      }
    }
    if (success) {
      topicGroups = topicGroups.drop(1)
      scheduleOnce(() => processTopicsOnce())
    } else {
      scheduleOnce(() => processTopicsOnce(), retryDelayMs)
    }
    false
  }

  /**
    * Completes the task, shutting it down from scheduling and invoking the completion callback.
    *
    * @return whether the task has completed
    */
  private def onCompletion(): Boolean = {
    shutdown()
    scheduler.scheduleOnce("ClearTopicMirrorsCompleted", completionCallback)
    true
  }

}
