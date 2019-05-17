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

package kafka.security.auth

import java.util.concurrent.CompletableFuture

/**
  * Extended authorizer interface for authorizers that load metadata from Kafka topics.
  * This is used to support scenarios where metadata for authorization is loaded from
  * Kafka topics using the inter-broker listener.
  * Start up sequence for this scenario is:
  *   1a) SocketServer starts up control plane listener and inter-broker listener
  *   1b) SocketServer waits for the authorizer to be initialized
  *
  *   2a) Authorizer loads its metadata from Kafka topic(s) using inter-broker listener
  *       when 1a) completes. Authorizer is now ready to authorize requests from external users.
  *   2b) Authorizer completes the future returned by [[AuthorizerWithKafkaStore#readyFuture]]
  *
  *   3) SocketServer starts the remaining listeners when 2b) completes
  *
  * Limitations:
  *  - Broker principals (and any other users of inter-broker listeners) must be authorized as
  *    super.users or using ACLs without access to metadata from Kafka topics. This makes it safe
  *    to start inter-broker listeners earlier.
  */
trait AuthorizerWithKafkaStore extends Authorizer {

  /**
    * Returns a future that completes when this Authorizer is ready to start authorizing requests
    * from all users. Implementation of this method should complete the future with appropriate
    * exception if initialization fails or times out.
    */
  def start(interBrokerListenerConfigs: java.util.Map[String, _]): CompletableFuture[Void]
}
