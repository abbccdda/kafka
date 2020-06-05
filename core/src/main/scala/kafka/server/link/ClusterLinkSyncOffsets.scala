/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util

import kafka.controller.KafkaController
import kafka.zk.ClusterLinkData
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.errors.{GroupAuthorizationException, TopicAuthorizationException}
import org.apache.kafka.common.metrics.{Metrics, Sensor}
import org.apache.kafka.common.metrics.stats.{CumulativeSum, Rate}
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.utils.SecurityUtils
import org.apache.kafka.common.{KafkaFuture, MetricName, TopicPartition}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class ClusterLinkSyncOffsets(val clientManager: ClusterLinkClientManager,
                             linkData: ClusterLinkData,
                             config: ClusterLinkConfig,
                             controller: KafkaController,
                             val destAdminFactory: () => Admin,
                             metrics: Metrics,
                             metricsTags: java.util.Map[String, String])
  extends ClusterLinkScheduler.PeriodicTask(clientManager.scheduler, name = "SyncOffsets",
    config.consumerOffsetSyncMs) {

  private[link] val currentOffsets = mutable.Map.empty[(String, TopicPartition), Long]
  private val groupFilters: Seq[Filter] = destinationFilters(config.consumerGroupFilters.map(_.groupFilters).getOrElse(Seq.empty))
  private var consumerOffsetCommitSensor: Sensor = _

  override def startup(): Unit = {
    consumerOffsetCommitSensor = metrics.sensor("consumer-offset-commit-sensor")
    val consumerOffsetCommitTotal = new MetricName("consumer-offset-committed-total",
      "cluster-link-metrics", "Total number of consumer offset commits.",
      metricsTags)
    val consumerOffsetCommitRate = new MetricName("consumer-offset-committed-rate",
      "cluster-link-metrics", "Rate of consumer offset commits.",
      metricsTags)
    consumerOffsetCommitSensor.add(consumerOffsetCommitTotal, new CumulativeSum)
    consumerOffsetCommitSensor.add(consumerOffsetCommitRate, new Rate)
    super.startup()
  }

  override def shutdown(): Unit = {
    metrics.removeSensor("consumer-offset-commit-sensor")
    super.shutdown()
  }

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

    val usedFilters = mutable.Buffer[Filter]()
    val filtered = groups.filter { group =>
      val matchedFilters = groupFilters.filter(_.matches(group))
      // if there are no filters that match this group we deny it by default
      if (matchedFilters.isEmpty) {
        false
      } else {
        usedFilters ++= matchedFilters
        matchedFilters.forall(_.isWhiteList)
      }
    }

    groupFilters.diff(usedFilters).foreach { unusedFilter =>
      warn(s"The filter $unusedFilter does not match any consumer group. This filter may not be " +
        "required or the groups it referred to may not have the correct DESCRIBE ACL " +
        "for the cluster link principal on the source cluster.")
    }
    filtered
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

        commitOffsets(allCommitFutures, groupFutures)
        false
      })
  }

  private def commitOffsets(allCommitFutures: KafkaFuture[Void],
                            commitFutures: Map[String, KafkaFuture[util.Map[TopicPartition, OffsetAndMetadata]]]) = {
    scheduleWhenComplete(allCommitFutures, () => {
      // clear current offsets to be remove old groups
      currentOffsets.clear()
      commitFutures.foreach { case (group, future)  =>
        try {
          future.get.asScala.foreach(t => currentOffsets += ((group, t._1) -> t._2.offset()))
          consumerOffsetCommitSensor.record()
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
        Some(group -> destAdminFactory().alterConsumerGroupOffsets(group, offsets.asJava).all())
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

  private def destinationFilters(groupFilters: Seq[GroupFilter]) : Seq[Filter] = {
    groupFilters.map { filter =>
      val patternType = SecurityUtils.patternType(filter.patternType)

      linkData.tenantPrefix match {
        case Some(prefix) =>
          patternType match {
            case PatternType.LITERAL =>
              if (filter.name == "*" && patternType == PatternType.LITERAL)
                Filter(prefix, PatternType.PREFIXED, filter.filterType, filter)
              else
                Filter(prefix + filter.name, patternType, filter.filterType, filter)
            case PatternType.PREFIXED =>
              Filter(prefix + filter.name, patternType, filter.filterType, filter)
            case _ =>
              throw new IllegalStateException(s"Unexpected pattern type ${filter.patternType}")
          }
        case None => Filter(filter.name, patternType, filter.filterType, filter)
      }
    }
  }

  private case class Filter(name : String,
                            patternType: PatternType,
                            filterType: String,
                            configuredFilter: GroupFilter) {
    val isWildcard = name == "*" && patternType == PatternType.LITERAL
    val isWhiteList = FilterType.fromString(filterType).contains(FilterType.WHITELIST)

    def matches(group: String): Boolean = {
      isWildcard || (patternType == PatternType.PREFIXED && group.startsWith(name)) ||
        (patternType == PatternType.LITERAL && group == name)
    }

    override def toString: String = configuredFilter.toString
  }
}
