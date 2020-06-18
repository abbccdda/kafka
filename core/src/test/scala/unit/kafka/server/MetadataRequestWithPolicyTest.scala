/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.Properties

import kafka.network.SocketServer
import kafka.utils.{MockTopicCreatePolicy, TestUtils}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{MetadataRequest, MetadataResponse}
import org.junit.Assert.{assertEquals, assertFalse}
import org.junit.{Before, Test}

import scala.jdk.CollectionConverters._
import scala.collection.Seq

class MetadataRequestWithPolicyTest extends BaseRequestTest {

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.setProperty(KafkaConfig.DefaultReplicationFactorProp, "2")
    properties.setProperty(KafkaConfig.RackProp, s"rack/${properties.getProperty(KafkaConfig.BrokerIdProp)}")
    properties.setProperty(KafkaConfig.CreateTopicPolicyClassNameProp, classOf[MockTopicCreatePolicy].getName)
  }

  @Before
  override def setUp(): Unit = {
    doSetup(createOffsetsTopic = false)
  }

  /**
   * When topic-creation-policy is specified, and auto-topic-creation is enabled, a
   * metadata request for a non-existing topic may succeed or fail, depending on the
   * policy. For this test, we have a dummy policy that matches on the name on the topic.
   */
  @Test
  def testMetadataWithAutoCreateRespectsPolicy() : Unit = {
    val testAllowedTopic = "random-auto-created-topic"
    val responseForValidCreationPolicy = sendMetadataRequest(new MetadataRequest.Builder(Seq(testAllowedTopic).asJava, true).build)
    verifySuccessfulAutoCreateTopic(testAllowedTopic, responseForValidCreationPolicy)

    val testDisallowedTopic = "disallowed-topic"
    val responseForInvalidCreationPolicy = sendMetadataRequest(new MetadataRequest.Builder(Seq(testDisallowedTopic).asJava,true).build)
    verifyUnsuccessfulAutoCreateTopic(testDisallowedTopic, responseForInvalidCreationPolicy)
  }

  private def verifySuccessfulAutoCreateTopic(autoCreatedTopic: String, response: MetadataResponse): Unit = {
    assertEquals(Errors.LEADER_NOT_AVAILABLE, response.errors.get(autoCreatedTopic))
    assertEquals(Some(servers.head.config.numPartitions), zkClient.getTopicPartitionCount(autoCreatedTopic))
    for (i <- 0 until servers.head.config.numPartitions)
      TestUtils.waitUntilMetadataIsPropagated(servers, autoCreatedTopic, i)
  }

  private def verifyUnsuccessfulAutoCreateTopic(autoCreatedTopic: String, response: MetadataResponse) = {
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, response.errors.get(autoCreatedTopic))
    assertFalse(zkClient.topicExists(autoCreatedTopic))
  }

  private def sendMetadataRequest(request: MetadataRequest, destination: Option[SocketServer] = None): MetadataResponse = {
    connectAndReceive[MetadataResponse](request, destination = destination.getOrElse(anySocketServer))
  }

}
