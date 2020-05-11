package kafka.server;

import java.util.Properties

import kafka.common.TopicPlacement
import kafka.log.LogConfig
import org.apache.kafka.common.config.ConfluentTopicConfig
import org.apache.kafka.common.errors.{InvalidReplicaAssignmentException, InvalidRequestException}
import org.apache.kafka.common.message.CreateTopicsRequestData.{CreatableReplicaAssignment, CreatableReplicaAssignmentCollection, CreatableTopic}
import org.junit.Assert.assertEquals
import org.junit.Test
import org.mockito.Mockito._
import org.scalatest.Assertions.intercept

import scala.jdk.CollectionConverters._
import scala.compat.java8.OptionConverters._

class ConfluentAdminManagerTest {

  private val placementJson = """{
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

  @Test
  def testTopicPlacementConstraints(): Unit = {
    val kafkaConfig: KafkaConfig = mock(classOf[KafkaConfig])
    val topicConfigs: Properties = new Properties()
    val topic: CreatableTopic = new CreatableTopic()

    // Check with null topic placement constraint
    assertEquals(None, AdminManager.validateAndGetTopicPlacement(kafkaConfig, LogConfig(topicConfigs), topic))

    // Check with empty topic placement constraint
    topicConfigs.setProperty(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, "")
    assertEquals(None, AdminManager.validateAndGetTopicPlacement(kafkaConfig, LogConfig(topicConfigs), topic))

    // Check with topic placement constraint with only whitespaces
    topicConfigs.setProperty(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, " ")
    assertEquals(None, AdminManager.validateAndGetTopicPlacement(kafkaConfig, LogConfig(topicConfigs), topic))

    topicConfigs.setProperty(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, "\t")
    assertEquals(None, AdminManager.validateAndGetTopicPlacement(kafkaConfig, LogConfig(topicConfigs), topic))

    topicConfigs.setProperty(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, placementJson)
    val topicPlacement = TopicPlacement.parse(placementJson).asScala

    when(kafkaConfig.isObserverSupportEnabled).thenReturn(true)
    topic.setReplicationFactor(-1)

    val topicPlacementOpt = AdminManager.validateAndGetTopicPlacement(kafkaConfig, LogConfig(topicConfigs), topic)
    assertEquals(topicPlacement.map(_.toJson), topicPlacementOpt.map(_.toJson))
  }

  @Test(expected = classOf[InvalidReplicaAssignmentException])
  def testTopicPlacementObserverNotSupported(): Unit = {
    val kafkaConfig: KafkaConfig = mock(classOf[KafkaConfig])
    val topic: CreatableTopic = new CreatableTopic()

    val topicConfigs: Properties = new Properties()
    topicConfigs.setProperty(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, placementJson)

    AdminManager.validateAndGetTopicPlacement(kafkaConfig, LogConfig(topicConfigs), topic)
  }

  @Test
  def testTopicPlacementBothTopicPlacementAndAssignmentSpecified(): Unit = {
    val kafkaConfig: KafkaConfig = mock(classOf[KafkaConfig])
    when(kafkaConfig.isObserverSupportEnabled).thenReturn(true)

    val topic: CreatableTopic = new CreatableTopic()
    val assignment: CreatableReplicaAssignment = new CreatableReplicaAssignment()
    val assignments: CreatableReplicaAssignmentCollection = new CreatableReplicaAssignmentCollection(
      List(assignment).asJava.iterator())
    topic.setAssignments(assignments)

    val topicConfigs: Properties = new Properties()
    topicConfigs.setProperty(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, placementJson)

    val caught = intercept[InvalidRequestException] {
      AdminManager.validateAndGetTopicPlacement(kafkaConfig, LogConfig(topicConfigs), topic)
    }

    val expectedErrMsg = "Both assignments and confluent.placement.constraints are set. Both cannot be used at the same time."
    assertEquals(caught.getMessage, expectedErrMsg)
  }

  @Test
  def testTopicPlacementBothTopicPlacementAndReplicationFactorSpecified(): Unit = {
    val kafkaConfig: KafkaConfig = mock(classOf[KafkaConfig])
    when(kafkaConfig.isObserverSupportEnabled).thenReturn(true)

    val topic: CreatableTopic = new CreatableTopic()
    topic.setReplicationFactor(3)

    val topicConfigs: Properties = new Properties()
    topicConfigs.setProperty(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, placementJson)

    val caught = intercept[InvalidRequestException] {
      AdminManager.validateAndGetTopicPlacement(kafkaConfig, LogConfig(topicConfigs), topic)
    }

    val expectedErrMsg = "Both replicationFactor and confluent.placement.constraints are set. Both cannot be used at the same time."
    assertEquals(caught.getMessage, expectedErrMsg)
  }
}
