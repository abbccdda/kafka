/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.security.authorizer;

import java.util.Collections;
import java.util.Properties;
import kafka.api.SaslEndToEndAuthorizationTest;
import kafka.server.KafkaConfig$;
import kafka.utils.JaasTestUtils;
import kafka.zk.ConfigEntityChangeNotificationZNode$;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.Before;
import scala.collection.JavaConversions;

/**
 * Tests ACL-based authorization with integration tests from AK to ensure that we stay consistent.
 */
public class AclEndToEndAuthorizationTest extends SaslEndToEndAuthorizationTest {

  @Before
  public void setUp() {
    Properties serverConfig = serverConfig();
    serverConfig.put(KafkaConfig$.MODULE$.AuthorizerClassNameProp(), ConfluentServerAuthorizer.class.getName());
    super.setUp();
  }

  @Override
  public String kafkaClientSaslMechanism() {
    return "SCRAM-SHA-256";
  }

  @Override
  public scala.collection.immutable.List<String> kafkaServerSaslMechanisms() {
    return JavaConversions.asScalaBuffer(Collections.singletonList("SCRAM-SHA-256")).toList();
  }

  @Override
  public String kafkaPrincipalType() {
    return KafkaPrincipal.USER_TYPE;
  }

  @Override
  public String clientPrincipal() {
    return JaasTestUtils.KafkaScramUser();
  }

  @Override
  public String kafkaPrincipal() {
    return JaasTestUtils.KafkaScramAdmin();
  }

  @Override
  public void configureSecurityBeforeServersStart() {
    super.configureSecurityBeforeServersStart();
    zkClient().makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode$.MODULE$.path());
    createScramCredentials(zkConnect(), kafkaPrincipal(), JaasTestUtils.KafkaScramAdminPassword());
    createScramCredentials(zkConnect(), JaasTestUtils.KafkaScramUser(),
        JaasTestUtils.KafkaScramPassword());
    createScramCredentials(zkConnect(), JaasTestUtils.KafkaScramUser2(),
        JaasTestUtils.KafkaScramPassword2());
  }
}

