/*
 * Copyright 2019 Confluent Inc.
 */
package kafka.cluster

import kafka.common.TopicPlacement
import scala.collection.JavaConverters._

object Observer {
  private[this] def brokerAttributes(broker: Broker): Map[String, String] = {
    broker.rack.iterator.map("rack" -> _).toMap
  }

  def isBrokerIsrEligible(
    topicPlacement: TopicPlacement,
    allReplicaIds: Seq[Int],
    aliveBrokers: Int => Option[Broker],
    leaderId: Int,
    brokerId: Int
  ): Boolean = {
    brokerId == leaderId || // Broker is the leader
    allReplicaIds.headOption.contains(brokerId) ||  // Broker is the preferred replicas
    (
      // Broker matches the replicas constraint
      allReplicaIds.contains(brokerId) &&
      aliveBrokers(brokerId)
        .exists(broker => topicPlacement.matchesReplicas(brokerAttributes(broker).asJava))
    )
  }

  def brokersIdsIsrEligible(
    topicPlacement: TopicPlacement,
    allReplicaIds: Seq[Int],
    aliveBrokers: Int => Option[Broker],
    leader: Int
  ): Set[Int] = {
    val replicas = allReplicaIds.headOption.iterator ++ 
    allReplicaIds.drop(1).iterator.flatMap { replicaId =>
      if (leader == replicaId) {
        Some(replicaId)
      } else {
        aliveBrokers(replicaId).flatMap { broker =>
          if (topicPlacement.matchesReplicas(brokerAttributes(broker).asJava)) {
            Some(replicaId)
          } else {
            None
          }
        }
      }
    }

    replicas.toSet
  }

  def brokerIdsOfflineOrIsrEligible(
    topicPlacement: TopicPlacement,
    allReplicaIds: Seq[Int],
    aliveBrokers: Int => Option[Broker],
    leader: Int
  ): Set[Int] = {
    val offlineReplicas = allReplicaIds
      .iterator
      .filter(aliveBrokers(_).isDefined)
      .toSet

    brokersIdsIsrEligible(
      topicPlacement,
      allReplicaIds,
      aliveBrokers,
      leader
    ) ++ offlineReplicas
  }
}
