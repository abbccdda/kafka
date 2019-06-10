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
import java.util.HashMap;
import java.util.Map;
import kafka.network.RequestChannel.Session;
import kafka.security.auth.Acl;
import kafka.security.auth.Authorizer;
import kafka.security.auth.Operation;
import kafka.security.auth.Resource;
import kafka.security.auth.SimpleAclAuthorizer;
import kafka.security.auth.SimpleAclAuthorizerTest;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import scala.collection.immutable.Set;

// Note: This test is useful during the early stages of development to ensure consistency
// with Apache Kafka SimpleAclAuthorizer. It can be removed once the code is stable if it
// becomes hard to maintain.
public class ConfluentServerAuthorizerTest extends SimpleAclAuthorizerTest {

  @Override
  public void setUp() {
    super.setUp();

    Authorizer authorizer = createAuthorizer();
    Authorizer authorizer2 = createAuthorizer();
    String superUsers = initialize(authorizer, authorizer2);

    try {
      Map<String, Object> authorizerConfigs = authorizerConfigs();
      authorizerConfigs.put(SimpleAclAuthorizer.SuperUsersProp(), superUsers);
      authorizer.configure(authorizerConfigs);
      authorizer2.configure(authorizerConfigs);
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
    return new ConfluentServerAuthorizer();
  }

  protected Map<String, Object> authorizerConfigs() {
    Map<String, Object> authorizerConfigs = new HashMap<>();
    authorizerConfigs.put(KafkaConfig$.MODULE$.ZkConnectProp(), zkConnect());
    return authorizerConfigs;
  }

  private String initialize(Authorizer authorizer, Authorizer authorizer2) {
    try {
      String superUsers = KafkaTestUtils.fieldValue(this, SimpleAclAuthorizerTest.class, "superUsers");
      SimpleAclAuthorizer simpleAuthorizer = KafkaTestUtils.fieldValue(this,
          SimpleAclAuthorizerTest.class, "simpleAclAuthorizer");
      simpleAuthorizer.close();
      SimpleAclAuthorizer simpleAuthorizer2 = KafkaTestUtils.fieldValue(this,
          SimpleAclAuthorizerTest.class, "simpleAclAuthorizer2");
      simpleAuthorizer2.close();
      KafkaTestUtils.setFinalField(this, SimpleAclAuthorizerTest.class,
          "simpleAclAuthorizer", simpleAclAuthorizer(authorizer));
      KafkaTestUtils.setFinalField(this, SimpleAclAuthorizerTest.class,
          "simpleAclAuthorizer2", simpleAclAuthorizer(authorizer2));

      return superUsers;
    } catch (Exception e) {
      throw new RuntimeException("Could not initialize test", e);
    }
  }

  protected SimpleAclAuthorizer simpleAclAuthorizer(Authorizer authorizer) throws Exception {
    return new SimpleAclAuthorizer() {
      @Override
      public void configure(Map<String, ?> javaConfigs) {
        authorizer.configure(javaConfigs);
      }

      @Override
      public boolean authorize(Session session, Operation operation, Resource resource) {
        return authorizer.authorize(session, operation, resource);
      }

      @Override
      public void addAcls(Set<Acl> acls, Resource resource) {
        authorizer.addAcls(acls, resource);
      }

      @Override
      public boolean removeAcls(Set<Acl> aclsTobeRemoved, Resource resource) {
        return authorizer.removeAcls(aclsTobeRemoved, resource);
      }

      @Override
      public boolean removeAcls(Resource resource) {
        return authorizer.removeAcls(resource);
      }

      @Override
      public Set<Acl> getAcls(Resource resource) {
        return authorizer.getAcls(resource);
      }

      @Override
      public scala.collection.immutable.Map<Resource, Set<Acl>> getAcls(KafkaPrincipal principal) {
        return authorizer.getAcls(principal);
      }

      @Override
      public scala.collection.immutable.Map<Resource, Set<Acl>> getAcls() {
        return authorizer.getAcls();
      }

      @Override
      public void close() {
        authorizer.close();
      }
    };
  }
}

