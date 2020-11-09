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

package unit.kafka.server

import java.util.Properties

import kafka.server.{KafkaConfig, MetadataRequestTest}
import org.junit.jupiter.api.Test

class MetadataRequestWithForwardingTest extends MetadataRequestTest {

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    super.brokerPropertyOverrides(properties)
    properties.put(KafkaConfig.EnableMetadataQuorumProp, true.toString)
  }

  @Test
  override def testAutoTopicCreation(): Unit = {
    super.testAutoTopicCreation()
  }

  @Test
  override def testAutoCreateOfCollidingTopics(): Unit = {
    super.testAutoCreateOfCollidingTopics()
  }

  @Test
  override def testAutoCreateTopicWithInvalidReplicationFactor(): Unit = {
    super.testAutoCreateTopicWithInvalidReplicationFactor()
  }

  /* the rest of tests are not enabled */

  @Test
  override def testClusterIdWithRequestVersion1(): Unit = {
  }

  @Test
  override def testClusterIdIsValid(): Unit = {
  }

  @Test
  override def testControllerId(): Unit = {
  }

  @Test
  override def testRack(): Unit = {
  }

  @Test
  override def testIsInternal(): Unit = {
  }

  @Test
  override def testNoTopicsRequest(): Unit = {
  }

  @Test
  override def testAllTopicsRequest(): Unit = {
  }

  @Test
  override def testPreferredReplica(): Unit = {
  }

  @Test
  override def testReplicaDownResponse(): Unit = {
  }

  @Test
  override def testIsrAfterBrokerShutDownAndJoinsBack(): Unit = {
  }

  @Test
  override def testAliveBrokersWithNoTopics(): Unit = {
  }
}
