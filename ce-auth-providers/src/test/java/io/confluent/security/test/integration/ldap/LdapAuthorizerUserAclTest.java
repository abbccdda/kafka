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

import io.confluent.kafka.security.authorizer.ConfluentServerAuthorizerTest;
import io.confluent.kafka.security.ldap.authorizer.LdapAuthorizer;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.minikdc.MiniKdcWithLdapService;
import io.confluent.security.test.utils.LdapTestUtils;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;

// Note: This test has been very useful for early development and testing, especially
// as SimpleAclAuthorizer has been changing frequently in AK as wildcard support was
// adding in a series of conflicting PRs. Long-term, it would be very hard to maintain
// this test, so we should remove it once the code base is stable.
public class LdapAuthorizerUserAclTest extends ConfluentServerAuthorizerTest {

  private MiniKdcWithLdapService miniKdcWithLdapService;

  @Override
  public void setUp() {

    try {
      miniKdcWithLdapService = LdapTestUtils.createMiniKdcWithLdapService(null, null);
      super.setUp();
    } catch (Exception e) {
      throw new RuntimeException("LDAP authorizer set up failed", e);
    }
  }

  @Override
  public void tearDown() {
    super.tearDown();
    if (miniKdcWithLdapService != null) {
      miniKdcWithLdapService.shutdown();
    }
    KafkaTestUtils.verifyThreadCleanup();
  }

  protected Authorizer createAuthorizer() {
    return new TestLdapAuthorizer();
  }

  // Some tests in SimpleAclAuthorizerTest configure the authorizer with just
  // broker configs, so make sure LDAP configs are added.
  private class TestLdapAuthorizer extends LdapAuthorizer {

    volatile AuthorizerServerInfo serverInfo;
    @Override
    public void configure(Map<String, ?> configs) {
      Map<String, Object> authorizerConfigs = new HashMap<>();
      authorizerConfigs.putAll(configs);
      authorizerConfigs.putAll(LdapTestUtils.ldapAuthorizerConfigs(miniKdcWithLdapService, 10));
      authorizerConfigs.putAll(miniKdcWithLdapService.ldapClientConfigs());
      super.configure(authorizerConfigs);
      if (serverInfo != null)
        configureServerInfo(serverInfo);
    }

    @Override
    public void configureServerInfo(AuthorizerServerInfo serverInfo) {
      this.serverInfo = serverInfo;
      super.configureServerInfo(serverInfo);
    }
  }

}

