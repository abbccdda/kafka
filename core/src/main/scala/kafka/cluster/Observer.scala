/*
 * Copyright 2019 Confluent Inc.
 */
package kafka.cluster

import java.util.Optional
import kafka.admin.{AdminUtils, BrokerMetadata}
import kafka.common.TopicPlacement
import kafka.common.TopicPlacement.ConstraintCount
import kafka.controller.ReplicaAssignment
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ApiError
import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq, mutable}
import scala.compat.java8.OptionConverters._

object Observer {
  /**
   * Perform assignment of replica to the list of brokers passed in as argument. The method can do assignment
   * for multiple partitions at a time, that allows it to evenly spread the assignment among the brokers. It
   * returns the assignment as "partition id" => "list of brokers".
   *
   * @param brokers the set of brokers and their metadata to use for the assignments
   * @param topicPlacement optional topic placement constraint that must be satisfied. If Some, then
   *                       replicationFactor will be ignored and the replication factor will be the sum of
   *                       all the counts in the constraints. Otherwise, replicationFactor will be used
   *                       as the replication factor.
   * @param numPartitions the number of partitions to assigned. The size of the resulting map will be
   *                      equal to this value.
   * @param replicationFactor the number of replicas to assigned per partition if topicPlacement is None.
   * @param fixedStartIndex   First index to use in the list of brokers to start assignment. So if broker list
   *                          is (1, 2, 3, 4) and this param is 2, the first broker used for first replica assignment
   *                          is 3.
   * @param startPartitionId  The id/index of the next partition to add. This determines the second broker that
   *                          gets picked in the replica assignment. So if broker list is (1, 2, 3, 4) and
   *                          fixedStartIndex is 2 and startPartitonId is 1, then the first broker will be 3 and
   *                          the second broker will be 1 (4 will be skipped). If however startPartitionId was
   *                          2, the second broker would be 2, for startPartitionId as 3 the second broker id will
   *                          be 4 and so on.
   */
  def getReplicaAssignment(brokers: Seq[BrokerMetadata],
                           topicPlacement: Option[TopicPlacement],
                           numPartitions: Int,
                           replicationFactor: Int,
                           fixedStartIndex: Int = -1,
                           startPartitionId: Int = -1): Map[Int, ReplicaAssignment] = {
    /**
     * Partition the brokers into different sets along with number of brokers that need to be picked from the set.
     * As an example, for following broker configuration:
     *
     * rack-1 => broker0, broker1, broker2, broker3, broker5, broker6
     * rack-2 => broker7, broker8, broker9
     *
     * and placement constraint asking 4 replicas in rack-1 and 2 in rack-2, we will get following:
     * [
     *   (4, [broker0, broker1, broker2, broker3, broker5, broker6]),
     *   (2, [broker7, broker8, broker9])
     * ]
     *
     * For the case where constraints are empty (getOrElse case below), the result will be the following for sync
     * replicas:
     * ```
     * [(replicationFactor, [broker0, broker1, broker2, broker3, broker5, broker6, broker7, broker8, broker9])]
     * ```
     * and the empty sequence for observers.
     *
     * Once partitioning is done, treat each partition as a normal replica assignment job. Use
     * AdminUtils to assign replicas and then merge the result and return it.
     */
    val (replicationAndSyncEligible, replicationAndObserverEligible) = topicPlacement
      .map { topicPlacement =>
        (
          partitionBrokersByConstraint(brokers)(topicPlacement.replicas().asScala),
          partitionBrokersByConstraint(brokers)(topicPlacement.observers().asScala)
        )
      }
      .getOrElse(
        (
          Seq(replicationFactor -> brokers),
          Seq.empty
        )
      )
    validatePartitioning(replicationAndSyncEligible ++ replicationAndObserverEligible)

    partitionReplicaAssignment(
      assignReplicasToPartitions(replicationAndSyncEligible, numPartitions, fixedStartIndex, startPartitionId),
      assignReplicasToPartitions(replicationAndObserverEligible, numPartitions, fixedStartIndex, startPartitionId)
    )
  }

  private[this] def assignReplicasToPartitions(
    replicationAndBrokers: Seq[(Int, Seq[BrokerMetadata])],
    partitions: Int,
    fixedStartIndex: Int,
    startPartitionId: Int): mutable.Map[Int, Seq[Int]] = {
    replicationAndBrokers
      .map { case (replication, brokerList) =>
        AdminUtils.assignReplicasToBrokers(brokerList, partitions, replication, fixedStartIndex, startPartitionId)
      }
      .foldLeft(mutable.Map.empty[Int, Seq[Int]])(mergeAssignmentMap)
  }

  private[this] def partitionReplicaAssignment(
    syncReplicas: Map[Int, Seq[Int]],
    observerReplicas: Map[Int, Seq[Int]]
  ): Map[Int, ReplicaAssignment] = {
    if (observerReplicas.nonEmpty && syncReplicas.keySet != observerReplicas.keySet) {
      val syncSize = syncReplicas.keySet.size
      val observerSize = observerReplicas.keySet.size
      throw new InvalidConfigurationException(
        s"Must assign observers to all or none of the partitions. $syncSize partitions with sync replicas. " +
        s"$observerSize partitions with observers."
      )
    }

    syncReplicas.map { case (partition, replicas) =>
      val observerAssignment = observerReplicas.getOrElse(partition, Seq.empty)

      partition -> ReplicaAssignment(replicas ++ observerAssignment, observerAssignment)
    }
  }

  /**
   * Method goes over each placement constraint and collects the broker that match that constraint. Returns a list
   * of tuple from "constraint replica count => broker metadata". The replica count is used to assign these many
   * replicas among the list of associated brokers.
   */
  private[cluster] def partitionBrokersByConstraint(brokers: Seq[BrokerMetadata])
         (constraints: Seq[ConstraintCount]): Seq[(Int, Seq[BrokerMetadata])] = {
    constraints.map(constraint => {
      val matchedBrokers = brokers.filter(broker => brokerMatchesPlacementConstraint(broker, constraint))
      if (matchedBrokers.size < constraint.count())
        throw new InvalidConfigurationException(s"Number of broker found (${matchedBrokers.size}) matching " +
          s"constraint $constraint is less than required count ${constraint.count()}")
      (constraint.count(), matchedBrokers)
    })
  }

  /**
   * Validate that we don't have any duplicate brokers within one constraint or across all of the constraints.
   * Otherwise a broker may get assigned multiple times to a topic partition.
   */
  private[cluster] def validatePartitioning(partitionedBrokers: Seq[(Int, Seq[BrokerMetadata])]): Unit = {
    val (count, allBrokers) = partitionedBrokers.foldLeft((0, Set.empty[BrokerMetadata])) { (acc, current) =>
      val (count, allBrokers) = acc
      val (_, brokers) = current

      if (brokers.toSet.size != brokers.size) {
        throw new InvalidConfigurationException(s"Duplicate eligible brokers ${brokers} match a placement constraints.")
      }

      (count + brokers.size, allBrokers ++ brokers)
    }

    if (count != allBrokers.size) {
      throw new InvalidConfigurationException(s"Some brokers satisfy more than one placement constraints: $partitionedBrokers")
    }
  }

  /**
   * Merge two assignment map containing "partition id -> seq[broker id]" into one assignment map. When applying
   * placement constraint we get one map for each constraint, which we need to merge. This method can be used
   * in "fold" call to do that.
   */
  private[cluster] def mergeAssignmentMap(
    mergedAssignment: mutable.Map[Int, Seq[Int]],
    currentAssignment: Map[Int, Seq[Int]]
  ): mutable.Map[Int, Seq[Int]] = {
    mergedAssignment ++ currentAssignment.map {
      case (partitionId, replicaIds) =>
        partitionId -> mergeReplicaLists(mergedAssignment.getOrElse(partitionId, Seq.empty), replicaIds)
    }
  }

  /**
   * Merge two list of replicas assigned to a partition based on two constraints. If a replica exists in both lists
   * then throw exception as we want constraints to produce disjoint set.
   */
  private[cluster] def mergeReplicaLists(brokerList1: Seq[Int], brokerList2: Seq[Int]): Seq[Int] = {
    val commonReplicas = brokerList1.intersect(brokerList2)
    if (commonReplicas.nonEmpty)
      throw new InvalidConfigurationException(s"Replica with ids ($commonReplicas) satisfy more than one placement constraints.")

    brokerList1 ++ brokerList2
  }

  /**
   * A simple predicate that matches if a broker "rack" matches to that specified in the constraint. This will be
   * used to filter out brokers that satisfy a placement constraint.
   */
  private[cluster] def brokerMatchesPlacementConstraint(broker: BrokerMetadata, constraint: ConstraintCount): Boolean = {
    val properties = broker.rack.map { rack =>
      "rack" -> rack
    }.toMap
    constraint.matches(properties.asJava)
  }

  /**
   * This method validates that one topic partition assignment is valid given an optional topic placement
   * constraint.
   *
   * If topic placement is given then validate that the assignment matches the constraint. This implementation
   * assumes that topic placement constraints are mutually exclusive.
   */
  def validateAssignment(
    topicPlacement: Option[TopicPlacement],
    assignment: ReplicaAssignment.Assignment,
    liveBrokerAttributes: Map[Int, Map[String, String]]
  ): Option[ApiError] = {
    validateAssignmentStructure(assignment).orElse(
      topicPlacement.flatMap { placementConstraint =>
        TopicPlacement.validateAssignment(
          placementConstraint,
          assignment.syncReplicas.map { id =>
            TopicPlacement.Replica.of(id, Optional.of(liveBrokerAttributes.getOrElse(id, Map.empty).asJava))
          }.asJava,
          assignment.observers.map { id =>
            TopicPlacement.Replica.of(id, Optional.of(liveBrokerAttributes.getOrElse(id, Map.empty).asJava))
          }.asJava
        ).asScala.map(message => new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT, message))
      }
    )
  }

  def validateAssignmentStructure(
    assignment: ReplicaAssignment.Assignment
  ): Option[ApiError] = {
    val replicas = assignment.replicas
    val replicaSet = replicas.toSet
    if (replicas.isEmpty || replicas.size != replicaSet.size) {
      Some(new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
        s"Duplicate replicas not allowed in partition assignment: ${replicas.mkString(", ")}."))
    } else if (replicas.exists(_ < 0)) {
      Some(new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
        s"Invalid replica id in partition assignment: ${replicas.mkString(", ")}"))
    } else if (!assignment.replicas.endsWith(assignment.observers)) {
      val observerMsg = assignment.observers.mkString(", ")
      Some(new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
        s"Assignment contains observers ($observerMsg) and the replicas' (${replicas.mkString(", ")}) " +
        "suffix doesn't match observers."))
    } else None
  }

  /**
   * This method validates that one topic partition reassignment is valid given an optional topic placement
   * constraint.
   *
   * If topic placement is given then validate that the assignment matches the constraints. When the observers
   * are offline and they were part of the original assignment then this function assumes that those observers
   * match anyone of the placement constraints.
   *
   * This implementation assumes that topic placement constraints are mutually exclusive.
   */
  def validateReassignment(
    topicPlacement: Option[TopicPlacement],
    reassignment: ReplicaAssignment,
    liveBrokerAttributes: Map[Int, Map[String, String]]
  ): Option[ApiError] = {
    reassignment.targetAssignment.flatMap { assignment =>
      topicPlacement.flatMap { placementConstraint =>
        TopicPlacement.validateAssignment(
          placementConstraint,
          assignment.syncReplicas.map { id =>
            TopicPlacement.Replica.of(id, Optional.of(liveBrokerAttributes.getOrElse(id, Map.empty).asJava))
          }.asJava,
          assignment.observers.map { id =>
            val attributes: Option[Map[String, String]] = {
              val attributes = liveBrokerAttributes.get(id)
              if (reassignment.originAssignment.replicas.contains(id)) {
                attributes
              } else attributes.orElse(Some(Map.empty))
            }

            TopicPlacement.Replica.of(id, attributes.map(_.asJava).asJava)
          }.asJava
        ).asScala.map(message => new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT, message))
      }
    }
  }
}
