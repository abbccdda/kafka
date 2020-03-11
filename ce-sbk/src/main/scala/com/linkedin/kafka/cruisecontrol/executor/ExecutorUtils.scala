/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor

import java.util
import java.util.concurrent.ExecutionException
import java.util.{Optional, Properties}

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig
import kafka.admin.PreferredReplicaLeaderElectionCommand
import org.apache.kafka.clients.admin.{Admin, NewPartitionReassignment}
import kafka.zk.{AdminZkClient, KafkaZkClient, ZkVersion}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * This class is a Java interface wrapper of open source ReassignPartitionCommand. This class is needed because
 * scala classes and Java classes are not compatible.
 */
object ExecutorUtils {
  val LOG: Logger = LoggerFactory.getLogger(ExecutorUtils.getClass.getName)

  /**
   * Add a list of replica reassignment tasks to execute. Replica reassignment indicates tasks that (1) relocate a replica
   * within the cluster, (2) introduce a new replica to the cluster (3) remove an existing replica from the cluster.
   *
   * @param adminClient the KafkaAdmin class to use for partition reassignment.
   * @param kafkaZkClient the KafkaZKClient class to fall back on if the
   *                      Kafka Cluster does not support the Admin API for partition reassignment.
   * @param reassignmentTasks Replica reassignment tasks to be executed.
   * @param config the configuration of Cruise Control.
   */
  def executeReplicaReassignmentTasks(adminClient: Admin,
                                      kafkaZkClient: KafkaZkClient,
                                      reassignmentTasks: java.util.List[ExecutionTask],
                                      config: KafkaCruiseControlConfig) {
    if (reassignmentTasks != null && !reassignmentTasks.isEmpty) {
      val partitionsToReassign = reassignmentTasks.map(_.proposal.topicPartition()).toSet
      val (inProgressTargetReplicaReassignment: Map[TopicPartition, Seq[Int]], supportsAdminApi: Boolean) =
        fetchTargetReplicasBeingReassigned(adminClient, kafkaZkClient, Some(partitionsToReassign))

      var newReplicaAssignments = if (supportsAdminApi)
        mutable.Map.empty[TopicPartition, Seq[Int]]
      else
        // ZK reassignment is not incremental so we need to reissue everything
        scala.collection.mutable.Map(inProgressTargetReplicaReassignment.toSeq: _*)

      reassignmentTasks.foreach({ task =>
        val tp = task.proposal().topicPartition()
        val targetReplicas = replicasToWrite(adminClient, config,
          task, inProgressTargetReplicaReassignment.get(tp))

        if (targetReplicas.nonEmpty) {
          newReplicaAssignments += (tp -> targetReplicas)
        }
      })

      if (newReplicaAssignments.nonEmpty) {
        if (supportsAdminApi) {
          val reassignments = newReplicaAssignments.map {
            case (tp, targetReplicas) =>
              (tp, Optional.of(new NewPartitionReassignment(targetReplicas.map(i => i : java.lang.Integer))))
          }
          adminClient.alterPartitionReassignments(reassignments).all().get()
        } else {
          kafkaZkClient.setOrCreatePartitionReassignment(newReplicaAssignments, ZkVersion.MatchAnyVersion)
        }
      }
    }
  }

  /**
   * Given an ExecutionTask, return the targetReplicas we should write to the Kafka reassignments.
   * If we should not reassign a partition as part of this task, an empty replica set will be returned
   */
  def replicasToWrite(adminClient: Admin, config: KafkaCruiseControlConfig,
                      task: ExecutionTask, inProgressTargetReplicasOpt: Option[Seq[Int]]): Seq[Int] = {
    val tp = task.proposal.topicPartition()
    val oldReplicas = asScalaBuffer(task.proposal.oldReplicas).map(_.brokerId.toInt)
    val newReplicas = asScalaBuffer(task.proposal.newReplicas).map(_.brokerId.toInt)

    // If aborting an existing task, trigger a reassignment to the oldReplicas
    // If no reassignment is in progress, trigger a reassignment to newReplicas
    // else, do not trigger a reassignment
    inProgressTargetReplicasOpt match {
      case Some(inProgressTargetReplicas) =>
        if (task.state() == ExecutionTask.State.ABORTING) {
          oldReplicas
        } else if (task.state() == ExecutionTask.State.DEAD
          || task.state() == ExecutionTask.State.ABORTED
          || task.state() == ExecutionTask.State.COMPLETED) {
          Seq.empty
        } else if (task.state() == ExecutionTask.State.IN_PROGRESS) {
          if (!newReplicas.equals(inProgressTargetReplicas)) {
            throw new RuntimeException(s"The provided new replica list $newReplicas" +
              s"is different from the in progress replica list $inProgressTargetReplicas for $tp")
          }
          Seq.empty
        } else {
          throw new IllegalStateException(s"Should never be here, the state is ${task.state()}")
        }
      case None =>
        if (task.state() == ExecutionTask.State.ABORTED
          || task.state() == ExecutionTask.State.DEAD
          || task.state() == ExecutionTask.State.ABORTING
          || task.state() == ExecutionTask.State.COMPLETED) {
          LOG.warn(s"No need to abort tasks $task because the partition is not in reassignment")
          Seq.empty
        } else {
          // verify with current assignment
          val currentReplicaAssignment = ExecutorAdminUtils.getReplicasForPartition(adminClient, tp, config)
          if (currentReplicaAssignment.isEmpty) {
            LOG.warn(s"Could not fetch the replicas for partition $tp. It is possible the topic or partition doesn't exist.")
            Seq.empty
          } else {
            // we are not verifying the old replicas because we may be reexecuting a task,
            // in which case the replica list could be different from the old replicas.
            newReplicas
          }
        }
    }
  }

  def executePreferredLeaderElection(kafkaZkClient: KafkaZkClient, tasks: java.util.List[ExecutionTask]) {
    val partitionsToExecute = tasks.map(task =>
      new TopicPartition(task.proposal.topic, task.proposal.partitionId)).toSet

    val preferredReplicaElectionCommand = new PreferredReplicaLeaderElectionCommand(kafkaZkClient, partitionsToExecute)
    preferredReplicaElectionCommand.moveLeaderToPreferredReplica()
  }

  def partitionsBeingReassigned(adminClient: Admin, kafkaZkClient: KafkaZkClient): util.Set[TopicPartition] =
    setAsJavaSet(fetchTargetReplicasBeingReassigned(adminClient, kafkaZkClient, None)._1.keySet)

  /**
   * Fetches the partitions being reassigned in the cluster
   *
   * @param adminClient - the KafkaAdmin class to use for partition reassignment.
   * @param kafkaZkClient - a ZK client to fall back on if the Kafka cluster
   *                        does not support the list partition reassignments API
   * @param partitionsOpt - an option of a set of partitions we want to check for reassignments.
   *                        An empty value will search for all reassigning partitions
   * @return a tuple of a map of partitions being reassigned and their target replicas and
   *                    a boolean indicating if we fell back to using the ZK API
   */
  def fetchTargetReplicasBeingReassigned(adminClient: Admin, kafkaZkClient: KafkaZkClient, partitionsOpt: Option[Set[TopicPartition]]):
    (Map[TopicPartition, Seq[Int]], Boolean) = {
    try {
      val listPartitionsResult = partitionsOpt match {
        case Some(partitions) => adminClient.listPartitionReassignments(partitions)
        case None => adminClient.listPartitionReassignments()
      }

      val reassigningTargetReplicas = listPartitionsResult.reassignments().get().map {
        case (tp, partitionReassignment) =>
          val targetReplicas = partitionReassignment.replicas().diff(partitionReassignment.removingReplicas())
          (tp, targetReplicas.map(_.toInt))
      }.toMap
      (reassigningTargetReplicas, true)
    } catch {
      case e: ExecutionException if (e.getCause.isInstanceOf[UnsupportedVersionException]) =>
        LOG.debug("Kafka cluster does not support the listPartitionReassignments API. Using ZooKeeper...", e.getCause)
        (kafkaZkClient.getPartitionReassignment.toMap, false)
      case t: Throwable =>
        LOG.error("Fetching reassigning replicas through the listPartitionReassignments API failed with an exception", t)
        throw t
    }
  }

  def ongoingLeaderElection(kafkaZkClient: KafkaZkClient): util.Set[TopicPartition] = {
    setAsJavaSet(kafkaZkClient.getPreferredReplicaElection)
  }

  def currentReplicasForPartition(adminClient: Admin, tp: TopicPartition, config: KafkaCruiseControlConfig): java.util.List[java.lang.Integer] = {
    ExecutorAdminUtils.getReplicasForPartition(adminClient, new TopicPartition(tp.topic(), tp.partition()), config).toList
  }

  def changeBrokerConfig(adminZkClient: AdminZkClient, brokerId: Int, config: Properties): Unit = {
    adminZkClient.changeBrokerConfig(Some(brokerId), config)
  }

  def changeTopicConfig(adminZkClient: AdminZkClient, topic: String, config: Properties): Unit = {
    adminZkClient.changeTopicConfig(topic, config)
  }

  def getAllLiveBrokerIdsInCluster(kafkaZkClient: KafkaZkClient): java.util.List[java.lang.Integer] = {
    seqAsJavaList(kafkaZkClient.getAllBrokersInCluster.map(_.id : java.lang.Integer))
  }
}
