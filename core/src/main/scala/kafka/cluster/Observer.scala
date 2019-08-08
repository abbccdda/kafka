/*
 * Copyright 2019 Confluent Inc.
 */
package kafka.cluster

import java.util.{Map => JMap}
import kafka.common.TopicPlacement
import scala.collection.JavaConverters._

object Observer {
  private[this] def brokerAttributes(broker: Broker): JMap[String, String] = {
    broker.rack.iterator.map("rack" -> _).toMap.asJava
  }

  /**
   * Returns true if anyone one of the following is true:
   *
   *   1. The leader matches the observers constraints - we implement this as a
   *      temporary solution to allow all replicas to join the ISR if the leader has
   *      failed over to an Observer because of unclean leader election.
   *   2. The broker is the leader
   *   3. The broker is the preferred replica
   *   4. The broker matches the replicas constraints
   *   5. The broker doesn't match the observers constraints
   */
  def isBrokerIsrEligible(
    topicPlacement: TopicPlacement,
    allReplicaIds: Seq[Int],
    aliveBrokers: Int => Option[Broker],
    leaderId: Int,
    brokerId: Int
  ): Boolean = {
    // Leader matches the observer constraint
    aliveBrokers(leaderId)
      .exists(broker => topicPlacement.matchesObservers(brokerAttributes(broker))) ||
    // Broker is the leader
    brokerId == leaderId ||
    // Broker is the preferred replicas
    allReplicaIds.headOption.contains(brokerId) ||
    aliveBrokers(brokerId).exists { broker =>
      val attributes = brokerAttributes(broker)
      // Broker matches the replicas constraint
      topicPlacement.matchesReplicas(attributes) ||
      // Broker doesn't match the observers constraint
      !topicPlacement.matchesObservers(attributes)
    }
  }

  def brokerIdsIsrEligible(
    topicPlacement: TopicPlacement,
    allReplicaIds: Seq[Int],
    aliveBrokers: Int => Option[Broker],
    leaderId: Int
  ): Set[Int] = {
    allReplicaIds.iterator.filter { replicaId =>
      isBrokerIsrEligible(
        topicPlacement,
        allReplicaIds,
        aliveBrokers,
        leaderId,
        replicaId
      )
    }.toSet
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

    brokerIdsIsrEligible(
      topicPlacement,
      allReplicaIds,
      aliveBrokers,
      leader
    ) ++ offlineReplicas
  }
}
