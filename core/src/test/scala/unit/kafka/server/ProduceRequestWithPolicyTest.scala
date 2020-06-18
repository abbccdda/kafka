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

import kafka.utils.{MockTopicCreatePolicy, TestUtils}
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.junit.Test
import org.junit.Assert.assertEquals
import org.scalatest.Assertions.intercept

import scala.concurrent.ExecutionException

class ProduceRequestWithPolicyTest extends BaseRequestTest {

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.setProperty(KafkaConfig.CreateTopicPolicyClassNameProp, classOf[MockTopicCreatePolicy].getName)
  }

  /**
   * Send produce requests to non-existing topics, verify success/failure modes depending on the
   * configured topic-creation policy (which in this case, is a dummy policy that only looks at the
   * topic name).
   */
  @Test
  def testProduceRequestAgainstNonExistingTopic(): Unit = {
    val goodTopic = "random-topic-that-should-be-auto-created"
    TestUtils.generateAndProduceMessages(servers, goodTopic, 1)

    val badTopic = "disallowed-topic"
    val exceptionCause = intercept[ExecutionException](TestUtils.generateAndProduceMessages(servers,
      badTopic, 1)).getCause
    assertEquals(classOf[TopicAuthorizationException], exceptionCause.getClass)
  }

}
