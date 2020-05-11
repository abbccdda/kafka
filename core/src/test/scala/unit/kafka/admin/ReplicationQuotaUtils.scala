/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.admin

import kafka.server.{ConfigType, KafkaConfig, KafkaServer}
import kafka.utils.TestUtils
import kafka.zk.AdminZkClient

import scala.collection.Seq

object ReplicationQuotaUtils {

  def checkThrottleConfigRemovedFromZK(adminZkClient: AdminZkClient, topic: String, servers: Seq[KafkaServer]): Unit = {
    TestUtils.waitUntilTrue(() => {
      val hasRateProp = servers.forall { server =>
        val brokerConfig = adminZkClient.fetchEntityConfig(ConfigType.Broker, server.config.brokerId.toString)
        brokerConfig.contains(KafkaConfig.LeaderReplicationThrottledRateProp) ||
          brokerConfig.contains(KafkaConfig.FollowerReplicationThrottledRateProp)
      }
      val topicConfig = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic)
      val hasReplicasProp = topicConfig.contains(KafkaConfig.LeaderReplicationThrottledReplicasProp) ||
        topicConfig.contains(KafkaConfig.FollowerReplicationThrottledReplicasProp)
      !hasRateProp && !hasReplicasProp
    }, "Throttle limit/replicas was not unset")
  }

  def checkThrottleConfigAddedToZK(adminZkClient: AdminZkClient, expectedThrottleRate: Long, servers: Seq[KafkaServer], topic: String, throttledLeaders: Set[String], throttledFollowers: Set[String]): Unit = {
    TestUtils.waitUntilTrue(() => {
      //Check for limit in ZK
      val brokerConfigAvailable = servers.forall { server =>
        val configInZk = adminZkClient.fetchEntityConfig(ConfigType.Broker, server.config.brokerId.toString)
        val zkLeaderRate = configInZk.getProperty(KafkaConfig.LeaderReplicationThrottledRateProp)
        val zkFollowerRate = configInZk.getProperty(KafkaConfig.FollowerReplicationThrottledRateProp)
        zkLeaderRate != null && expectedThrottleRate == zkLeaderRate.toLong &&
          zkFollowerRate != null && expectedThrottleRate == zkFollowerRate.toLong
      }
      //Check replicas assigned
      val topicConfig = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic)
      val leader = topicConfig.getProperty(KafkaConfig.LeaderReplicationThrottledReplicasProp).split(",").toSet
      val follower = topicConfig.getProperty(KafkaConfig.FollowerReplicationThrottledReplicasProp).split(",").toSet
      val topicConfigAvailable = leader == throttledLeaders && follower == throttledFollowers
      brokerConfigAvailable && topicConfigAvailable
    }, "throttle limit/replicas was not set")
  }
}
