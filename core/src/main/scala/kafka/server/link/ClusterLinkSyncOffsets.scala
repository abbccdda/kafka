/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util

import kafka.controller.KafkaController
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.errors.{GroupAuthorizationException, TopicAuthorizationException}
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.utils.SecurityUtils
import org.apache.kafka.common.{KafkaFuture, TopicPartition}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class ClusterLinkSyncOffsets(val clientManager: ClusterLinkClientManager,
                             config: ClusterLinkConfig,
                             controller: KafkaController,
                             val destAdminFactory: () => Admin)
  extends ClusterLinkScheduler.PeriodicTask(clientManager.scheduler, name = "SyncOffsets",
    config.consumerOffsetSyncMs) {

  val currentOffsets = mutable.Map.empty[(String, TopicPartition), Long]

  /**
   * Starts running the task, returning whether the task has completed.
   *
   * @return `true` if the task has completed, otherwise `false` if there's outstanding work to be done
   */
  override protected def run(): Boolean = {
    if (controller.isActive && config.consumerOffsetSyncEnable) {
      if (config.consumerGroupFilters.isEmpty) {
        warn(s"${ClusterLinkConfig.ConsumerOffsetSyncEnableProp} is true but no consumer group filters are specified. No consumer offsets will be migrated.")
        return true
      }
      try {
        trace("Attempting to retrieve consumer groups from source cluster...")
        val listConsumerGroupsResult = clientManager.getAdmin.listConsumerGroups()
        scheduleWhenComplete(listConsumerGroupsResult.all,
          () => {
            val filteredGroups = filterConsumerGroups(listConsumerGroupsResult.all.get.asScala
              .map(result => result.groupId()).toSet)
            updateOffsets(filteredGroups)
            false
          })
        false
      } catch {
        case ex: Throwable =>
          warn("Unable to list consumer groups on source cluster. Offsets will not be migrated.", ex)
          true
      }
    } else {
      true
    }
  }


  /**
   * Method to filter out consumer groups to match those that we are interested in.
   *
   * @param groups the full set of consumer groups fetched from the source cluster
   *
   * @return a filtered set of consumer groups relevant to migration
   */
  private def filterConsumerGroups(groups: Set[String]) : Set[String] = {

    val groupFilters = config.consumerGroupFilters.get

    val matches = (filter:GroupFilter,group:String) =>  {
      val patternType = SecurityUtils.patternType(filter.patternType)
      ((filter.name == "*" && patternType == PatternType.LITERAL)
        || (patternType == PatternType.PREFIXED && group.startsWith(filter.name))
        || (patternType == PatternType.LITERAL && group == filter.name))
    }

    // determine if there any filters that match no groups and warn
    groupFilters.groupFilters
      .filterNot { filter => groups.exists(matches(filter, _) ) }
      .foreach(unusedFilter => warn(s"The filter $unusedFilter does not match any consumer " +
        "group. This filter may not be required or the groups it referred to may not have the correct DESCRIBE ACL " +
        "for the cluster link principal on the source cluster."))

    groups.filter(group => {
      val matchedFilters = groupFilters.groupFilters.filter(filter =>
        matches(filter,group)
      )
      // if there are no filters that match this group we deny it by default
      if (matchedFilters.isEmpty) {
        false
      } else {
        matchedFilters.forall(_.filterType == FilterType.WHITELIST_OPT)
      }
    })
  }

  /**
   * Method to update offsets for the given set of consumer groups on the destination cluster
   *
   * This method fetches the current consumer offsets for a set of groups from the source cluster
   * and applies them to the destination cluster for mirrored topics
   *
   * @param groups a set of consumer groups to migrate offsets for
   */
  private def updateOffsets(groups: Set[String]): Unit = {

    val groupFutures = listGroupOffsets(groups)

    val allGroupFutures = KafkaFuture.allOf(groupFutures.values.toSeq:_*)
    scheduleWhenComplete(allGroupFutures,
      () => {
        val commitFutures = getCommitFutures(groupFutures)
        val allCommitFutures = KafkaFuture.allOf(commitFutures.values.toSeq:_*)

        commitOffsets(allCommitFutures,groupFutures)
        false
      })
  }

  private def commitOffsets(allCommitFutures: KafkaFuture[Void],commitFutures: Map[String, KafkaFuture[util.Map[TopicPartition, OffsetAndMetadata]]]) = {
    scheduleWhenComplete(allCommitFutures, () => {
      // clear current offsets to be remove old groups
      currentOffsets.clear()
      commitFutures.foreach { case (group, future)  =>
        try {
          future.get.asScala.foreach(t => currentOffsets += ((group, t._1) -> t._2.offset()))
        } catch {
          case ex: GroupAuthorizationException =>
            warn(s"Unable to commit offsets for consumer group $group on the destination cluster, due to authorization issues." +
              " Please add READ ACLs for the consumer group. This action is taken by the inter-broker principal defined in the broker " +
              "configuration so ACLs should be added for this principal ", ex)
          case ex: TopicAuthorizationException =>
            warn(s"Unable to commit offsets for consumer group $group on the destination cluster, due to authorization issues." +
              " Please add READ ACLs for the topics being migrated. This action is taken by the inter-broker principal defined in the broker " +
              "configuration so ACLs should be added for this principal ", ex)
          case ex: Throwable =>
            warn(s"Unable to commit offsets for consumer group $group on destination cluster.", ex)
        }
      }
      true
    })
  }

  /**
   * This method takes a set of consumer offsets fetched from the source cluster and commits them to the destination cluster
   *
   * Any offsets that do not refer to mirrored topics or that we have already seen will be filtered out.
   *
   * @param groupFutures map of group -> offsets as retrieved from the source cluster
   *
   * @return map of group -> future to track async offsets commits that have been submitted to the destination cluster.
   */
  private def getCommitFutures(groupFutures: Map[String, KafkaFuture[util.Map[TopicPartition, OffsetAndMetadata]]]) = {
    groupFutures.flatMap{ case (group, groupFuture) =>
      val offsets = groupFuture.get.asScala
        .filter(t => controller.controllerContext.linkedTopics.get(t._1.topic).exists(_.mirrorIsEstablished)
          && currentOffsets.getOrElse((group, t._1), -1) != t._2.offset())
      if (offsets.nonEmpty) {
        Some(group -> destAdminFactory().alterConsumerGroupOffsets(group,offsets.asJava).all())
      } else {
        None
      }
    }
  }

  /**
   * This method takes a set of consumer groups and fetches current offsets for them from the source cluster.
   *
   * @param groups set of groups to fetch offsets for
   *
   * @return map of group -> future to track offset listing calls that have been submitted to the source cluster.
   */
  private def listGroupOffsets(groups: Set[String]) = {
    groups.flatMap(group => {
      try {
        Some(group -> clientManager.getAdmin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata())
      } catch {
        case ex: GroupAuthorizationException =>
          warn(s"Unable to list offsets for consumer group $group on the source cluster, due to authorization issues." +
            " Please add DESCRIBE ACLs for the consumer group", ex)
          None
        case ex: TopicAuthorizationException =>
          warn(s"Unable to list offsets for consumer group $group on the source cluster, due to authorization issues." +
            " Please add DESCRIBE ACLs for the topics being migrated", ex)
          None
        case ex: Throwable =>
          warn("Unable to list consumer groups on source cluster. Offsets will not be migrated.", ex)
          None
      }
    }).toMap
  }
}
