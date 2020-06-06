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

package kafka.utils

import java.util

import org.apache.kafka.common.errors.PolicyViolationException
import org.apache.kafka.server.policy.CreateTopicPolicy
import org.apache.kafka.server.policy.CreateTopicPolicy.RequestMetadata

class MockTopicCreatePolicy extends CreateTopicPolicy {
  val disallowedTopic = "disallowed-topic"

  override def validate(requestMetadata: RequestMetadata): Unit = {
    if (requestMetadata.topic().equals(disallowedTopic))
      throw new PolicyViolationException("I'm sorry Dave, I'm afraid I can't make that topic for you.")
  }

  override def configure(configs: util.Map[String, _]): Unit = {}

  override def close(): Unit = {}
}
