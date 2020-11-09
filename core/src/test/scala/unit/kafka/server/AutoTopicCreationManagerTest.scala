/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import kafka.controller.KafkaController
import org.apache.kafka.common.message.CreateTopicsRequestData
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.requests._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.{ArgumentMatchers, Mockito}

import scala.collection.Map

class AutoTopicCreationManagerTest {

  private val requestTimeout = 100
  private val brokerToController = Mockito.mock(classOf[BrokerToControllerChannelManager])
  private val adminManager = Mockito.mock(classOf[ZkAdminManager])
  private val controller = Mockito.mock(classOf[KafkaController])

  @Test
  def testCreateTopics(): Unit = {
    val autoTopicCreationManager = new AutoTopicCreationManagerImpl(
      Some(brokerToController),
      adminManager,
      controller,
      requestTimeout)

    val topicName = "topic"

    val topicsToCreate = Set(getNewTopic(topicName))

    val topicsCollection = new CreateTopicsRequestData.CreatableTopicCollection
    topicsCollection.add(getNewTopic(topicName))
    val requestBody = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData()
        .setTopics(topicsCollection)
        .setTimeoutMs(requestTimeout))

    Mockito.when(controller.isActive).thenReturn(false)
    autoTopicCreationManager.createTopics(topicsToCreate, UnboundedControllerMutationQuota)
    // Calling twice with the same topic will only trigger one forwarding.
    autoTopicCreationManager.createTopics(topicsToCreate, UnboundedControllerMutationQuota)
    Mockito.verify(brokerToController).sendRequest(
      ArgumentMatchers.eq(requestBody),
      any(classOf[ControllerRequestCompletionHandler]))
  }

  // We need to prepare a new CreatableTopic each time to make sure it could be added
  // to the new collection.
  private def getNewTopic(topicName: String): CreatableTopic = {
    new CreatableTopic().setName(topicName)
  }

  @Test
  def testCreateTopicsWithForwardingDisabled(): Unit = {
    val autoTopicCreationManager = new AutoTopicCreationManagerImpl(None,
      adminManager,
      controller,
      requestTimeout)

    val topicName = "topic"
    val creatableTopic = new CreatableTopic().setName(topicName)
    val topicsToCreate = Set(creatableTopic)

    val topicsCollection = new CreateTopicsRequestData.CreatableTopicCollection
    topicsCollection.add(creatableTopic)

    Mockito.when(controller.isActive).thenReturn(false)
    autoTopicCreationManager.createTopics(topicsToCreate, UnboundedControllerMutationQuota)

    Mockito.verify(adminManager).createTopics(
      ArgumentMatchers.eq(requestTimeout),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.eq(Map(topicName -> creatableTopic)),
      ArgumentMatchers.eq(Map.empty),
      any(classOf[ControllerMutationQuota]),
      any(classOf[Map[String, ApiError] => Unit]))
  }
}
