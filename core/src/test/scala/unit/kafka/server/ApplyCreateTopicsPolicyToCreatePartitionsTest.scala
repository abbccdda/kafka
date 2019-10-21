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

import java.util
import java.util.Properties
import java.util.concurrent.ExecutionException

import kafka.api.IntegrationTestHarness
import org.apache.kafka.clients.admin.{AdminClient, NewPartitions}
import org.apache.kafka.common.config.internals.ConfluentConfigs
import org.apache.kafka.common.errors.PolicyViolationException
import org.apache.kafka.server.policy.CreateTopicPolicy
import org.apache.kafka.server.policy.CreateTopicPolicy.RequestMetadata
import org.junit.{Assert, Test}

import scala.collection.JavaConverters._

/**
 * This is a test of a Confluent-specific configuration option,
 * confluent.apply.create.topic.policy.to.create.partitions.
 */
class ApplyCreateTopicsPolicyToCreatePartitionsTest extends IntegrationTestHarness {
  import ApplyCreateTopicsPolicyToCreatePartitionsTest._

  override protected def brokerCount: Int = 2

  override def modifyConfigs(props: Seq[Properties]): Unit = {
    props.foreach { p =>
      p.put(ConfluentConfigs.APPLY_CREATE_TOPIC_POLICY_TO_CREATE_PARTITIONS, "true")
      p.put(KafkaConfig.CreateTopicPolicyClassNameProp, classOf[Policy].getName)
    }
  }

  @Test
  def testSuccessfullyCreatePartitions(): Unit = {
    createTopic(allowedTopic)
    val admin = AdminClient.create(adminClientConfig)
    try {
      admin.createPartitions(Map(allowedTopic -> NewPartitions.increaseTo(2)).asJava).all().get()
    } finally {
      admin.close()
    }
  }

  @Test
  def testUnsuccessfullyCreatePartitions(): Unit = {
    createTopic(disallowedTopic)
    val admin = AdminClient.create(adminClientConfig)
    try {
      admin.createPartitions(Map(disallowedTopic -> NewPartitions.increaseTo(2)).asJava).all().get()
      Assert.fail("Expected invoking createPartitions to fail, but it did not.")
    } catch {
      case e: ExecutionException => {
        Assert.assertEquals(classOf[PolicyViolationException], e.getCause().getClass())
      }
    } finally {
      admin.close()
    }
  }
}

object ApplyCreateTopicsPolicyToCreatePartitionsTest {
  val allowedTopic = "allowed-topic"
  val disallowedTopic = "disallowed-topic"

  class Policy extends CreateTopicPolicy {
    override def validate(requestMetadata: RequestMetadata): Unit = {
      if (!requestMetadata.topic().equals(allowedTopic))
        throw new PolicyViolationException("invalid topic name")
    }

    override def configure(configs: util.Map[String, _]): Unit = {}

    override def close(): Unit = {}
  }
}
