/*
 * Copyright 2019 Confluent Inc.
 */
package kafka.cluster

import kafka.admin.BrokerMetadata
import kafka.common.TopicPlacement
import kafka.controller.ReplicaAssignment
import org.apache.kafka.common.errors.{InvalidConfigurationException, InvalidReplicaAssignmentException}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.{Map, Seq, immutable, mutable}

class ObserverTest {

  private val placementJson = """{
                                | "version": 1,
                                |  "replicas": [{
                                |      "count": 3,
                                |      "constraints": {
                                |        "rack": "east-1"
                                |      }
                                |    },
                                |    {
                                |      "count": 2,
                                |      "constraints": {
                                |        "rack": "east-2"
                                |      }
                                |    }
                                |  ],
                                |  "observers": [{
                                |    "count": 2,
                                |    "constraints": {
                                |      "rack": "west-1"
                                |    }
                                |  }]
                                |}""".stripMargin
  private val topicWithObserverPlacementConstraint: Option[TopicPlacement] = Some(TopicPlacement.parse(placementJson))

  private val placementJsonWithoutObservers = """{
                                                | "version": 1,
                                                |  "replicas": [{
                                                |      "count": 3,
                                                |      "constraints": {
                                                |        "rack": "east-1"
                                                |      }
                                                |    },
                                                |    {
                                                |      "count": 2,
                                                |      "constraints": {
                                                |        "rack": "east-2"
                                                |      }
                                                |    },
                                                |    {
                                                |      "count": 2,
                                                |      "constraints": {
                                                |        "rack": "west-1"
                                                |      }
                                                |    }
                                                |  ]
                                                |}""".stripMargin
  private val topicWithoutObserversConstraint: Option[TopicPlacement] =
    Some(TopicPlacement.parse(placementJsonWithoutObservers))

  private val allBrokersAttributes = (0 to 9).map { id =>
    val rack = id match {
      case x if 0 to 2 contains x => "east-1"  // 0, 1, 2
      case x if 3 to 4 contains x => "east-2"  // 3, 4
      case x if 5 to 7 contains x => "west-1"  // 5, 6, 7
      case _ => "west-2"                       // 8, 9
    }

    id -> Map("rack" -> rack)
  }.toMap

  /**
   * Test a match is made for a broker that matches a rack of a constraint.
   */
  @Test
  def testPlacementConstraintPredicateSuccess(): Unit = {
    val replicaBroker = BrokerMetadata(2, Some("east-1"))
    val observerBroker = BrokerMetadata(2, Some("west-1"))
    topicWithObserverPlacementConstraint.foreach { topicPlacement =>
      val replicaConstraints = topicPlacement.replicas().asScala
      assertTrue(Observer.brokerMatchesPlacementConstraint(replicaBroker, replicaConstraints.head))
      assertFalse(Observer.brokerMatchesPlacementConstraint(replicaBroker, replicaConstraints.tail.head))
      assertTrue(Observer.brokerMatchesPlacementConstraint(observerBroker, topicPlacement.observers().get(0)))
    }
  }

  /**
   * Test that a broker with rack is not matched to a constraint with different rack.
   */
  @Test
  def testPlacementConstraintPredicateFailure(): Unit = {
    val broker = BrokerMetadata(2, Some("south-1"))
    topicWithObserverPlacementConstraint.foreach { topicPlacement =>
      assertFalse(Observer.brokerMatchesPlacementConstraint(broker, topicPlacement.replicas().get(0)))
      assertFalse(Observer.brokerMatchesPlacementConstraint(broker, topicPlacement.observers().get(0)))
    }
  }

  /**
   * If no constraint is specified, make sure we match all brokers.
   */
  @Test
  def testPlacementConstraintPredicateNoConstraints(): Unit = {
    val broker = BrokerMetadata(2, Some("south-1"))
    val placementJson = """{
                          | "version": 1,
                          |  "replicas": [{
                          |      "count": 2,
                          |      "constraints": {
                          |      }
                          |    }
                          |  ],
                          |  "observers": [{
                          |    "count": 1,
                          |    "constraints": {
                          |    }
                          |  }]
                          |}""".stripMargin
    val topicPlacement = TopicPlacement.parse(placementJson)
    // Empty constraint should match everything
    assertTrue(Observer.brokerMatchesPlacementConstraint(broker, topicPlacement.replicas().get(0)))
    assertTrue(Observer.brokerMatchesPlacementConstraint(broker, topicPlacement.observers().get(0)))
  }

  /**
   * If no rack property is specified in broker, test that match against a constraint fails.
   */
  @Test
  def testPlacementConstraintPredicateNoBrokerRack(): Unit = {
    val broker = BrokerMetadata(2, None)
    topicWithObserverPlacementConstraint.foreach { topicPlacement =>
      assertFalse(Observer.brokerMatchesPlacementConstraint(broker, topicPlacement.replicas().get(0)))
      assertFalse(Observer.brokerMatchesPlacementConstraint(broker, topicPlacement.observers().get(0)))
    }
  }

  /**
   * Test if two non intersecting maps can be merged correctly.
   */
  @Test
  def testMergeDisjointMaps(): Unit = {
    val firstMap: mutable.Map[Int, Seq[Int]] = mutable.Map(1 -> (1 to 5) , 2 -> (2 to 6))
    val secondMap: mutable.Map[Int, Seq[Int]] = mutable.Map(10 -> (10 to 15), 11 -> (11 to 16))

    val mergedMap = Observer.mergeAssignmentMap(firstMap, secondMap)
    assertEquals(firstMap ++ secondMap, mergedMap)
  }

  /**
   * Test if an empty map can be merged w/o any issue.
   */
  @Test
  def testMergeEmptyMap(): Unit = {
    val assignmentMap: mutable.Map[Int, Seq[Int]] = mutable.Map(1 -> (1 to 5) , 2 -> (2 to 6))

    assertEquals(assignmentMap, Observer.mergeAssignmentMap(assignmentMap, mutable.Map.empty))
    assertEquals(assignmentMap, Observer.mergeAssignmentMap(mutable.Map.empty, assignmentMap))
    assertEquals(mutable.Map.empty, Observer.mergeAssignmentMap(mutable.Map.empty, mutable.Map.empty))
  }

  /**
   * Test two non-disjoint maps can be merged, with common keys having same values.
   */
  @Test(expected = classOf[InvalidConfigurationException])
  def testMergeMapValueSame(): Unit = {
    val firstMap: mutable.Map[Int, Seq[Int]] = mutable.Map(1 -> (1 to 5) , 2 -> (2 to 6))
    val secondMap: mutable.Map[Int, Seq[Int]] = mutable.Map(1 -> (1 to 5), 10 -> (10 to 15), 11 -> (11 to 16))

    Observer.mergeAssignmentMap(firstMap, secondMap)
  }

  /**
   * Test if maps with same keys can be merged
   */
  @Test
  def testMergeMapKeySame(): Unit = {
    val firstMap: mutable.Map[Int, Seq[Int]] = mutable.Map(1 -> (1 to 5), 2 -> (2 to 6))
    val secondMap: mutable.Map[Int, Seq[Int]] = mutable.Map(1 -> (6 to 10), 10 -> (10 to 15))
    val expectedMergedMap = mutable.Map(1 -> (1 to 10), 2 -> (2 to 6), 10 -> (10 to 15))

    assertEquals(expectedMergedMap, Observer.mergeAssignmentMap(firstMap, secondMap))
  }

  /**
   * Test if two disjoint sequences can be merged.
   */
  @Test
  def testMergeDisjointSequences(): Unit = {
    val firstSeq = 1 to 10
    val secondSeq = 11 to 20
    assertEquals(1 to 20, Observer.mergeReplicaLists(firstSeq, secondSeq))
  }

  /**
   * Test sequences can be merged if one of them is empty.
   */
  @Test
  def testMergeEmptySequences(): Unit = {
    val testSeq = 1 to 10
    assertEquals(testSeq, Observer.mergeReplicaLists(testSeq, Seq.empty))
    assertEquals(testSeq, Observer.mergeReplicaLists(Seq.empty, testSeq))
    assertEquals(Seq.empty, Observer.mergeReplicaLists(Seq.empty, Seq.empty))
  }

  /**
   * Test if sequences contain common elements then we throw exception.
   */
  @Test(expected = classOf[InvalidConfigurationException])
  def mergeNonDisjointSequences(): Unit = {
    val firstSeq = 1 to 10
    val secondSeq = 5 to 15
    Observer.mergeReplicaLists(firstSeq, secondSeq)
  }

  /**
   * Test if replica and observer constraints partition list of brokers correctly.
   */
  @Test
  def testPartitionBrokersByConstraint(): Unit = {
    val placementJson = """{
                          | "version": 1,
                          |  "replicas": [{
                          |      "count": 3,
                          |      "constraints": {
                          |        "rack": "east-1"
                          |      }
                          |    },
                          |    {
                          |      "count": 2,
                          |      "constraints": {
                          |        "rack": "east-2"
                          |      }
                          |    }
                          |  ],
                          |  "observers": [{
                          |    "count": 3,
                          |    "constraints": {
                          |      "rack": "west-1"
                          |    }
                          |  }]
                          |}""".stripMargin
    val east1Brokers = (1 to 10).map(BrokerMetadata(_, Some("east-1")))
    val east2Brokers = (11 to 20).map(BrokerMetadata(_, Some("east-2")))
    val west1Brokers = (21 to 30).map(BrokerMetadata(_, Some("west-1")))
    val allBrokers = east1Brokers ++ east2Brokers ++ west1Brokers

    val topicPlacement = TopicPlacement.parse(placementJson)
    val partitionFunction = Observer.partitionBrokersByConstraint(allBrokers) _

    // Check if replicas get partitioned properly
    val replicasTuple = partitionFunction(topicPlacement.replicas().asScala)
    val replicas = replicasTuple.flatMap { case (_, replica) => replica}
    assertEquals(east1Brokers ++ east2Brokers, replicas)
    val replicaCount = replicasTuple.map { case (count, _) => count}
    assertEquals(List(3,2), replicaCount)

    // Check if observers get partitioned correctly
    val observersTuple = partitionFunction(topicPlacement.observers().asScala)
    val observers = observersTuple.flatMap { case (_, observer) => observer}
    assertEquals(west1Brokers, observers)
    val observersCount = observersTuple.map { case (count, _) => count}
    assertEquals(List(3), observersCount)
  }

  /**
   * Test if number of brokers that satisfy a constraint is less than the count that
   * is requested, we throw exception.
   */
  @Test(expected = classOf[InvalidConfigurationException])
  def testPartitionBrokersByConstraintInvalidCount(): Unit = {
    val placementJson = """{
                          | "version": 1,
                          |  "replicas": [{
                          |      "count": 3,
                          |      "constraints": {
                          |        "rack": "east-1"
                          |      }
                          |    },
                          |    {
                          |      "count": 5,
                          |      "constraints": {
                          |        "rack": "east-2"
                          |      }
                          |    }
                          |  ],
                          |  "observers": [{
                          |    "count": 3,
                          |    "constraints": {
                          |      "rack": "west-1"
                          |    }
                          |  }]
                          |}""".stripMargin
    val east1Brokers = (1 to 10).map(BrokerMetadata(_, Some("east-1")))
    val east2Brokers = (11 to 13).map(BrokerMetadata(_, Some("east-2")))
    val west1Brokers = (14 to 20).map(BrokerMetadata(_, Some("west-1")))
    val allBrokers = east1Brokers ++ east2Brokers ++ west1Brokers

    val topicPlacement = TopicPlacement.parse(placementJson)
    val partitionFunction = Observer.partitionBrokersByConstraint(allBrokers) _

    // Observers should get partitioned correctly as they have greater number of
    // matching brokers than the count requested.
    partitionFunction(topicPlacement.observers().asScala)

    // Replica should not get partitioned as we don't have enough brokers in "east-2" zone.
    partitionFunction(topicPlacement.replicas().asScala)
  }

  /**
   * Test replica assignment when no constraint is specified and brokers are not rack aware. This test
   * validates the replica assignment example in the javadoc of [[kafka.admin.AdminUtils#assignReplicasToBrokers]].
   */
  @Test
  def testRackUnawareNoConstraintReplicaAssignment(): Unit = {
    val brokers = (0 to 4).map(BrokerMetadata(_, None))
    val numPartitions = 10
    val replicationFactor: Short = 3

    // We have 10 partitions, replication factor as 3. So we will have 30 assignments
    val assignments = Observer.getReplicaAssignment(brokers, None, numPartitions, replicationFactor)
    validateRackUnawareReplicaAssignment(brokers, assignments)
  }

  /**
   * Same as [[kafka.cluster.ObserverTest#testRackUnawareNoConstraintReplicaAssignment]] but placement json having no
   * constraint in it. In this case the behavior should be same.
   */
  @Test
  def testRackUnawareReplicaAssignmentWithPlacementConstraintMissing(): Unit = {
    val brokers = (0 to 4).map(BrokerMetadata(_, None))
    val numPartitions = 10
    val replicationFactor: Short = 3

    // We have 10 partitions, replication factor as 3. So we will have 30 assignments
    val assignments = Observer.getReplicaAssignment(
      brokers, Some(TopicPlacement.parse("""{"version":1}""")), numPartitions, replicationFactor
    )
    validateRackUnawareReplicaAssignment(brokers, assignments)
  }

  private[this] def validateRackUnawareReplicaAssignment(
    brokers: immutable.IndexedSeq[BrokerMetadata],
    assignments: collection.Map[Int, ReplicaAssignment]
  ): Unit = {
    // Validate that assignments are spread evenly. To check that look at each partition assignment.
    // Each broker should be leader for 2 partitions, in second position for two other partitions
    // and in third position for another set of two partitions. To check this we need to flip the
    // assignment table from "partition -> replica" to "replica -> partition"
    val brokerAssignment = assignments.values.map(_.replicas).transpose
    brokerAssignment.foreach { assignedPartitions =>
      assignedPartitions.groupBy(brokerId => brokerId).values.foreach { brokerIds =>
        assertEquals(2, brokerIds.size)
      }
      // There should be 10 assignment for 10 partitions
      assertEquals(10, assignedPartitions.size)
      // and all 5 brokers should have appeared in the assignment
      assertEquals(5, assignedPartitions.toSet.size)
    }

    // Also check that each partition has different set of brokers assigned.
    assignments.values.map(_.replicas).foreach { brokers =>
      assertEquals(3, brokers.size)
    }
  }

  /**
   * Test replica assignment when no constraint is specified and brokers are rack aware.
   */
  @Test
  def testRackAwareNoConstraintReplicaAssignment(): Unit = {
    val numPartitions = 9
    val replicationFactor: Short = 3

    // Create 9 brokers, and place them on 3 racks, so we will have 3 brokers on each rack
    val racks = (1 to 3).flatMap { id => List.fill(3)("rack" + id) }
    val brokers = (0 to 8).zip(racks).map {
      case (id, rack) => BrokerMetadata(id, Some(rack))
    }

    // We have 9 partitions, replication factor as 3. So we will have 27 assignments
    val assignments = Observer.getReplicaAssignment(brokers, None, numPartitions, replicationFactor)

    /**
     * Validate assignments, we have 9 partitions and 9 brokers. The assignment should:
     * 1. spread out replicas among brokers, so each broker should be leader, second position and third position once
     * 2. Each rack should hold 3 leaders, 3 second position and 3 third position
     */
    assignments.values.map(_.replicas).foreach {
      // This checks if each partition is assigned to 3 different brokers
      assignedBrokers => assertEquals(3, assignedBrokers.toSet.size)
    }
    assignments.values.map(_.replicas).transpose.foreach {
      // This tests if each position (leader, second, third) in assignment is spread to brokers/racks evenly
      assignedBrokers => assertEquals(9, assignedBrokers.toSet.size)
    }
  }

  /**
   * Test replica assignment when placement constraint is specified.
   */
  @Test
  def testRackAwareWithConstraintReplicaAssignment(): Unit = {
    /**
     * Put 5 brokers in 3 different racks. Then use a placement constraint to distribute 10 partitions
     * among them.
     */
    val brokers = (0 to 14).map { id =>
      val rack = s"rack${id / 5 + 1}"
      BrokerMetadata(id, Some(rack))
    }

    val placementJson = """{
                           | "version": 1,
                           |  "replicas": [{
                           |      "count": 3,
                           |      "constraints": {
                           |        "rack": "rack1"
                           |      }
                           |    },
                           |    {
                           |      "count": 2,
                           |      "constraints": {
                           |        "rack": "rack2"
                           |      }
                           |    }
                           |  ],
                           |  "observers": [{
                           |    "count": 2,
                           |    "constraints": {
                           |      "rack": "rack3"
                           |    }
                           |  }]
                           |}""".stripMargin
    val partitionAssignment = Observer.getReplicaAssignment(
      brokers, Some(TopicPlacement.parse(placementJson)), numPartitions = 10,
      replicationFactor = 3)

    // Test if replica and observer assignment was done as per placement constraint
    partitionAssignment.values.map(_.replicas).foreach { assignedBrokers => {
        // This checks that each partition gets assigned to racks in placement constraint
        assertEquals(7, assignedBrokers.toSet.size)
        // First 3 should be on rack 1 (broker id from 0 to 4)
        assignedBrokers.take(3).foreach(brokerId => assertTrue(brokerId >= 0 && brokerId <= 4))
        // Next 2 should be on rack 2 (broker id between 5 and 9)
        assignedBrokers.slice(3, 5).foreach(brokerId => assertTrue(brokerId >= 5 && brokerId <= 9))
        // And last 2 should be on rack 3 (broker id from 10 to 14)
        assignedBrokers.slice(5, 7).foreach(brokerId => assertTrue(brokerId >= 10 && brokerId <= 14))
      }
    }

    partitionAssignment.values.map(_.observers).foreach { observers => {
        assertTrue(observers.mkString(","),
          observers.forall(observerId => observerId >= 10 && observerId <= 14))
      }
    }

    // Now test that each broker got its fair share of partitions at
    // each position (i.e. leader, first, second etc)
    val brokerAssignment = partitionAssignment.values.map(_.replicas).transpose
    brokerAssignment.foreach { assignedPartitions =>
      // Each broker should be present twice at each position make it fair distribution:
      // 10 partitions, each broker twice at each position.
      assignedPartitions.groupBy(brokerId => brokerId).values.foreach { brokerIds =>
        assertEquals(2, brokerIds.size)
      }
    }
  }

  /**
   * Test replica assignment when placement constraint is specified.
   */
  @Test
  def testRackAwareWithConstraintReplicaAssignmentWithStartIndex(): Unit = {
    /**
     * Put 5 brokers in 3 different racks. Then use a placement constraint to distribute 10 partitions
     * among them.
     */
    val brokers = (0 to 14).map { id =>
      val rack = s"rack${id / 5 + 1}"
      BrokerMetadata(id, Some(rack))
    }

    val placementJson = """{
                           | "version": 1,
                           |  "replicas": [{
                           |      "count": 4,
                           |      "constraints": {
                           |        "rack": "rack1"
                           |      }
                           |    }
                           |  ],
                           |  "observers": [{
                           |    "count": 3,
                           |    "constraints": {
                           |      "rack": "rack3"
                           |    }
                           |  }]
                           |}""".stripMargin
    val partitionAssignment = Observer.getReplicaAssignment(
      brokers, Some(TopicPlacement.parse(placementJson)), numPartitions = 15,
      replicationFactor = 3, fixedStartIndex = 2, startPartitionId = 3)

    // Confirm that first partition has assignment of (2, 1, 3, 4) and observer have (12, 11, 13)
    // This is because fixedStartIndex = 2, which picks the third broker (zero based indexing) in the list
    // of replica and observer broker as first broker. Then the second broker will be 4th in the list starting
    // with the third as startPartitionId = 3.
    // To take replica as an example, broker list is (0, 1, 2, 3, 4). The third in the list is 2, so the
    // assignment will start with 2. Then the 4th broker after 2 is 1 (skipping 3, 4, and 0). This creates
    // assignment of (2, 1, 3, 4)
    assertEquals(Seq(2, 1, 3, 4), partitionAssignment.values.head.replicas.slice(0, 4))
    assertEquals(Seq(12, 11, 13), partitionAssignment.values.head.observers)

    // Test if replica and observer assignment was done as per placement constraint
    partitionAssignment.values.map(_.replicas).foreach { assignedBrokers => {
        // This checks that each partition gets assigned to racks in placement constraint
        assertEquals(7, assignedBrokers.toSet.size)
        // First 4 should be on rack 1 (broker id from 0 to 4)
        assignedBrokers.take(4).foreach(brokerId => assertTrue(brokerId >= 0 && brokerId <= 4))
        // Last 3 should be on rack 3 (broker id from 10 to 14)
        assignedBrokers.slice(5, 7).foreach(brokerId => assertTrue(brokerId >= 10 && brokerId <= 14))
      }
    }

    partitionAssignment.values.map(_.observers).foreach { observers => {
        assertTrue(observers.mkString(","),
        observers.forall(observerId => observerId >= 10 && observerId <= 14))
      }
    }
  }

  /**
   * Test if the [[Observer.validatePartitioning()]] method is success for a partition with no overlapping
   * brokers.
   */
  @Test
  def validatePartitioningSuccess(): Unit = {
    val partitionedBrokers = (0 to 3).map {
      partitionNumber => partitionNumber -> (0 to 4).map {
        brokerId => BrokerMetadata(partitionNumber * 5 + brokerId, Some("rack"))
      }
    }

    Observer.validatePartitioning(partitionedBrokers)
  }

  /**
   * Test if the [[Observer.validatePartitioning()]] method fails when same broker appears in multiple partitions.
   */
  @Test(expected = classOf[InvalidConfigurationException])
  def validatePartitioningFailure(): Unit = {
    val commonBrokers = List[BrokerMetadata](BrokerMetadata(1, Some("rack-1")), BrokerMetadata(2, Some("rack-1")))
    val partitionedBrokers = Seq(
      (1, (List(BrokerMetadata(3, Some("rack-1"))) ++ commonBrokers)),
      (2, (List(BrokerMetadata(4, Some("rack-1"))) ++ commonBrokers))
    )

    Observer.validatePartitioning(partitionedBrokers)
  }

  /**
   * Test if the [[Observer.validatePartitioning()]] method fails when same broker appears in multiple partitions.
   */
  @Test(expected = classOf[InvalidConfigurationException])
  def validatePartitioningFailureMultiplePartitions(): Unit = {
    val commonBrokers = List[BrokerMetadata](BrokerMetadata(1, Some("rack-1")), BrokerMetadata(2, Some("rack-1")))
    val partitionedBrokers = Seq(
      (1, List(BrokerMetadata(3, Some("rack-1"))) ++ commonBrokers),
      (2, List(BrokerMetadata(4, Some("rack-1")))),
      (2, List(BrokerMetadata(5, Some("rack-1"))) ++ commonBrokers)
    )

    Observer.validatePartitioning(partitionedBrokers)
  }

  /**
   * Test if there is no topic placement, then replica assignment list validation doesn't throw.
   */
  @Test
  def testValidateReplicasNoPlacementConstraint(): Unit = {
    Observer.validateReplicaAssignment(None, ReplicaAssignment.Assignment(0 to 6, Seq.empty), allBrokersAttributes)
  }

  /**
   * Test if topic placement contains observers validation fails with exception.
   */
  @Test(expected = classOf[InvalidReplicaAssignmentException])
  def testValidateObserversConstraint(): Unit = {
    Observer.validateReplicaAssignment(
      topicWithObserverPlacementConstraint,
      ReplicaAssignment.Assignment(0 to 3, Seq.empty),
      allBrokersAttributes
    )
  }

  /**
   * Test if there are no observers and replica count matches those in placement constraint,
   * validation succeeds.
   */
  @Test
  def testValidateReplicasMatchesConstraint(): Unit = {
    Observer.validateReplicaAssignment(
      topicWithoutObserversConstraint,
      ReplicaAssignment.Assignment(0 to 6, Seq.empty),
      allBrokersAttributes
    )
  }

  /**
   * Test if replica count doesn't match those in constraint we fail.
   */
  @Test(expected = classOf[InvalidReplicaAssignmentException])
  def testValidateReplicasOverConstraintCount(): Unit = {
    Observer.validateReplicaAssignment(
      topicWithoutObserversConstraint,
      ReplicaAssignment.Assignment(0 to 7, Seq.empty),
      allBrokersAttributes
    )
  }

  /**
   * Test if replica count doesn't match those in constraint we fail.
   */
  @Test(expected = classOf[InvalidReplicaAssignmentException])
  def testValidateReplicasUnderConstraintCount(): Unit = {
    Observer.validateReplicaAssignment(
      topicWithoutObserversConstraint,
      ReplicaAssignment.Assignment(0 to 4, Seq.empty),
      allBrokersAttributes
    )
  }

  /**
   * Test if replica provided match the overall count, but not individual count validation fails.
   */
  @Test(expected = classOf[InvalidReplicaAssignmentException])
  def testReplicaIndividualConstraintCountNotSatisfied(): Unit = {
    Observer.validateReplicaAssignment(
      topicWithoutObserversConstraint,
      ReplicaAssignment.Assignment(Seq(0, 1, 3, 4, 5, 6, 7), Seq.empty),
      allBrokersAttributes
    )
  }

  @Test
  def testObseverMatchesConstraint(): Unit = {
    Observer.validateReplicaAssignment(
      topicWithObserverPlacementConstraint,
      ReplicaAssignment.Assignment(0 to 6, 5 to 6),
      allBrokersAttributes
    )
  }

  @Test(expected = classOf[InvalidReplicaAssignmentException])
  def testInvalidObseverCount(): Unit = {
    Observer.validateReplicaAssignment(
      topicWithObserverPlacementConstraint,
      ReplicaAssignment.Assignment(0 to 7, 5 to 7),
      allBrokersAttributes
    )
  }

  @Test(expected = classOf[InvalidReplicaAssignmentException])
  def testInvalidObseverAttribute(): Unit = {
    Observer.validateReplicaAssignment(
      topicWithObserverPlacementConstraint,
      ReplicaAssignment.Assignment((0 to 5) ++ Seq(9), Seq(5, 9)),
      allBrokersAttributes
    )
  }

  @Test(expected = classOf[InvalidReplicaAssignmentException])
  def testReplicasHasObserverAsSuffix(): Unit = {
    Observer.validateReplicaAssignment(
      None,
      ReplicaAssignment.Assignment(0 to 5, 0 to 1),
      allBrokersAttributes
    )
  }
}
