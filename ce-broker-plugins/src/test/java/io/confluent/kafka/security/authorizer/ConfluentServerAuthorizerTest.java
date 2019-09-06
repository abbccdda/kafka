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

import io.confluent.kafka.test.utils.KafkaTestUtils;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import kafka.security.authorizer.AclAuthorizer;
import kafka.security.authorizer.AclAuthorizerTest;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;

// Note: This test is useful during the early stages of development to ensure consistency
// with Apache Kafka SimpleAclAuthorizer. It can be removed once the code is stable if it
// becomes hard to maintain.
public class ConfluentServerAuthorizerTest extends AclAuthorizerTest {

  @Override
  public void setUp() {
    super.setUp();

    Authorizer authorizer = createAuthorizer();
    Authorizer authorizer2 = createAuthorizer();
    String superUsers = initialize(authorizer, authorizer2);

    try {
      Map<String, Object> authorizerConfigs = authorizerConfigs();
      authorizerConfigs.put(AclAuthorizer.SuperUsersProp(), superUsers);
      authorizer.configure(authorizerConfigs);
      authorizer2.configure(authorizerConfigs);
      AuthorizerServerInfo serverInfo = KafkaTestUtils.serverInfo("clusterA", SecurityProtocol.SSL);
      ((ConfluentServerAuthorizer) authorizer).configureServerInfo(serverInfo);
      ((ConfluentServerAuthorizer) authorizer2).configureServerInfo(serverInfo);
    } catch (Exception e) {
      throw new RuntimeException("Confluent authorizer set up failed", e);
    }
  }

  @Override
  public void tearDown() {
    super.tearDown();
    KafkaTestUtils.verifyThreadCleanup();
  }

  protected Authorizer createAuthorizer() {
    return new TestAuthorizer();
  }

  protected Map<String, Object> authorizerConfigs() {
    Map<String, Object> authorizerConfigs = new HashMap<>();
    authorizerConfigs.put(KafkaConfig$.MODULE$.ZkConnectProp(), zkConnect());
    return authorizerConfigs;
  }

  private String initialize(Authorizer authorizer, Authorizer authorizer2) {
    try {
      String superUsers = KafkaTestUtils.fieldValue(this, AclAuthorizerTest.class, "superUsers");
      AclAuthorizer aclAuthorizer = KafkaTestUtils.fieldValue(this,
          AclAuthorizerTest.class, "aclAuthorizer");
      aclAuthorizer.close();
      AclAuthorizer aclAuthorizer2 = KafkaTestUtils.fieldValue(this,
          AclAuthorizerTest.class, "aclAuthorizer2");
      aclAuthorizer2.close();
      KafkaTestUtils.setFinalField(this, AclAuthorizerTest.class,
          "aclAuthorizer", aclAuthorizer(authorizer));
      KafkaTestUtils.setFinalField(this, AclAuthorizerTest.class,
          "aclAuthorizer2", aclAuthorizer(authorizer2));

      return superUsers;
    } catch (Exception e) {
      throw new RuntimeException("Could not initialize test", e);
    }
  }

  protected AclAuthorizer aclAuthorizer(Authorizer authorizer) {
    return new AclAuthorizer() {
      @Override
      public void configure(Map<String, ?> javaConfigs) {
        authorizer.configure(javaConfigs);
      }

      @Override
      public Map<Endpoint, CompletableFuture<Void>> start(AuthorizerServerInfo serverInfo) {
        return authorizer.start(serverInfo);
      }

      @Override
      public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext,
          List<Action> actions) {
        return authorizer.authorize(requestContext, actions);
      }

      @Override
      public List<AclCreateResult> createAcls(AuthorizableRequestContext requestContext,
          List<AclBinding> aclBindings) {
        return authorizer.createAcls(requestContext, aclBindings);
      }

      @Override
      public List<AclDeleteResult> deleteAcls(AuthorizableRequestContext requestContext,
          List<AclBindingFilter> aclBindingFilters) {
        return authorizer.deleteAcls(requestContext, aclBindingFilters);
      }

      @Override
      public Iterable<AclBinding> acls(AclBindingFilter filter) {
        return authorizer.acls(filter);
      }

      @Override
      public void close() {
        try {
          authorizer.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private static class TestAuthorizer extends ConfluentServerAuthorizer {

    volatile AuthorizerServerInfo serverInfo;

    @Override
    public void configure(Map<String, ?> configs) {
      super.configure(configs);
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

