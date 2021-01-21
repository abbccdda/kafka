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

import java.util.concurrent.ConcurrentHashMap

import kafka.controller.KafkaController
import kafka.utils.Logging
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.message.CreateTopicsRequestData
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.requests.CreateTopicsRequest
import org.apache.kafka.common.utils.Time

import scala.collection.Map

trait AutoTopicCreationManager {

  def createTopics(
    topicNames: Set[CreatableTopic],
    controllerMutationQuota: ControllerMutationQuota
  ): Unit

  def start(): Unit = {}

  def shutdown(): Unit = {}
}

object AutoTopicCreationManager {

  def apply(
    config: KafkaConfig,
    metadataCache: MetadataCache,
    time: Time,
    metrics: Metrics,
    threadNamePrefix: Option[String],
    adminManager: ZkAdminManager,
    controller: KafkaController,
    enableForwarding: Boolean
  ): AutoTopicCreationManager = {
    val channelManager =
      if (enableForwarding)
        Some(new BrokerToControllerChannelManager(
          metadataCache = metadataCache,
          time = time,
          metrics = metrics,
          config = config,
          channelName = "autoTopicCreationChannel",
          threadNamePrefix = threadNamePrefix,
          retryTimeoutMs = config.requestTimeoutMs.longValue
        ))
      else
        None
    new AutoTopicCreationManagerImpl(channelManager, adminManager, controller, config.requestTimeoutMs)
  }
}

class AutoTopicCreationManagerImpl(
  channelManager: Option[BrokerToControllerChannelManager],
  adminManager: ZkAdminManager,
  controller: KafkaController,
  requestTimeout: Int
) extends AutoTopicCreationManager with Logging {

  private val inflightTopics = new ConcurrentHashMap[String, CreatableTopic]

  override def start(): Unit = {
    channelManager.foreach(_.start())
  }

  override def shutdown(): Unit = {
    channelManager.foreach(_.shutdown())
  }

  override def createTopics(topics: Set[CreatableTopic],
                            controllerMutationQuota: ControllerMutationQuota): Unit = {
    val topicConfigs = topics
      .filter(topic => !inflightTopics.contains(topic.name()))
      .map(topic => {(topic.name(), topic)}).toMap

    if (topicConfigs.nonEmpty) {
      // Mark the topics as inflight during auto creation.
      topicConfigs.foreach(config => inflightTopics.put(config._1, config._2))

      if (!controller.isActive && channelManager.isDefined) {
        val topicsToCreate = new CreateTopicsRequestData.CreatableTopicCollection
        topicConfigs.foreach(config => topicsToCreate.add(config._2))
        val createTopicsRequest = new CreateTopicsRequest.Builder(
          new CreateTopicsRequestData()
            .setTimeoutMs(requestTimeout)
            .setTopics(topicsToCreate)
        )

        channelManager.get.sendRequest(createTopicsRequest, new ControllerRequestCompletionHandler {
          override def onTimeout(): Unit = {
            clearInflightRequests(topicConfigs)
          }

          override def onComplete(response: ClientResponse): Unit = {
            clearInflightRequests(topicConfigs)
          }
        })
      } else {
        adminManager.createTopics(
          requestTimeout,
          validateOnly = false,
          topicConfigs,
          Map.empty,
          controllerMutationQuota,
          _ => clearInflightRequests(topicConfigs))
      }
    } else {
      debug(s"Topics $topics are under creation already, skip sending additional " +
        s"CreateTopic requests.")
    }
  }

  private def clearInflightRequests(topicConfigs: Map[String, CreatableTopic]): Unit = {
    topicConfigs.foreach(config => inflightTopics.remove(config._1))
  }
}
