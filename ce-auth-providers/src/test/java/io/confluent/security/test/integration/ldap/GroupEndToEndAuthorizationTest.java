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

package io.confluent.security.test.integration.ldap;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.minikdc.MiniKdcWithLdapService;
import io.confluent.security.test.utils.LdapTestUtils;
import kafka.api.SaslEndToEndAuthorizationTest;
import kafka.utils.JaasTestUtils;
import kafka.zk.ConfigEntityChangeNotificationZNode$;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.After;
import org.junit.Before;
import scala.collection.JavaConversions;

import java.util.Collections;
import java.util.Properties;

/**
 * The tests in io.confluent.kafka.security.test.integration are extensions of the
 * existing tests in Apache Kafka for SimpleAclAuthorizer. Most of this functionality
 * is also tested by the new tests added in io.confluent.kafka.security.test, but these
 * tests have been retained to ensure that we don't break anything when changes are made
 * in SimpleAclAuthorizer or LDAPAuthorizer.
 */
public abstract class GroupEndToEndAuthorizationTest extends SaslEndToEndAuthorizationTest {

  private static final String ADMIN_GROUP = JaasTestUtils.KafkaScramAdmin();
  private static final String TEST_GROUP = "TestGroup";
  private static final String TEST_GROUP2 = "TestGroup2";

  private MiniKdcWithLdapService ldapServer;

  @Before
  public void setUp() {
    try {
      // Create LDAP server and create groups
      ldapServer = createLdapServer();
      if (ldapServer != null) {
        ldapServer.createGroup(TEST_GROUP, JaasTestUtils.KafkaScramUser());
        ldapServer.createGroup(TEST_GROUP2, JaasTestUtils.KafkaScramUser2());
        ldapServer.createGroup(ADMIN_GROUP, JaasTestUtils.KafkaScramAdmin());

        // Add LDAP authorizer properties to server configs
        Properties serverConfig = serverConfig();
        serverConfig.putAll(LdapTestUtils.ldapAuthorizerConfigs(ldapServer, 0));
      }

      super.setUp();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @After
  public void tearDown() {
    try {
      super.tearDown();
    } finally {
      if (ldapServer != null) {
        ldapServer.shutdown();
      }
      KafkaTestUtils.verifyThreadCleanup();
    }
  }

  protected abstract MiniKdcWithLdapService createLdapServer() throws Exception;

  @Override
  public String kafkaClientSaslMechanism() {
    return "SCRAM-SHA-256";
  }

  @Override
  public scala.collection.immutable.List<String> kafkaServerSaslMechanisms() {
    return JavaConversions.asScalaBuffer(Collections.singletonList("SCRAM-SHA-256")).toList();
  }

  @Override
  public KafkaPrincipal clientPrincipal() {
    return new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, TEST_GROUP);
  }

  @Override
  public KafkaPrincipal kafkaPrincipal() {
    return new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, JaasTestUtils.KafkaScramAdmin());
  }

  @Override
  public void configureSecurityBeforeServersStart() {
    super.configureSecurityBeforeServersStart();
    zkClient().makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode$.MODULE$.path());
    createScramCredentials(zkConnect(), kafkaPrincipal().getName(), JaasTestUtils.KafkaScramAdminPassword());
    createScramCredentials(zkConnect(), JaasTestUtils.KafkaScramUser(),
        JaasTestUtils.KafkaScramPassword());
    createScramCredentials(zkConnect(), JaasTestUtils.KafkaScramUser2(),
        JaasTestUtils.KafkaScramPassword2());
    SecurityTestUtils.addLicenseTopicAcl(zkConnect(), kafkaPrincipal());
  }
}

