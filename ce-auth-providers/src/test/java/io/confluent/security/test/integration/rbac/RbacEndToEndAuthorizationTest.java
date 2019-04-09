// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.test.integration.rbac;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.test.utils.RbacClusters;
import io.confluent.security.test.utils.RbacClusters.Config;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RbacEndToEndAuthorizationTest {

  private static final String BROKER_USER = "kafka";
  private static final String SUPER_USER = "root";
  private static final String DEVELOPER1 = "app1-developer";
  private static final String DEVELOPER2 = "app2-developer";
  private static final String RESOURCE_OWNER = "resourceOwner1";
  private static final String OPERATOR = "operator1";
  private static final String DEVELOPER_GROUP = "app-developers";

  private static final String APP1_TOPIC = "app1-topic";
  private static final String APP1_CONSUMER_GROUP = "app1-consumer-group";
  private static final String APP2_TOPIC = "app2-topic";
  private static final String APP2_CONSUMER_GROUP = "app2-consumer-group";

  private RbacClusters rbacClusters;
  private String clusterId;

  @Before
  public void setUp() throws Throwable {
    List<String> otherUsers = Arrays.asList(
        SUPER_USER,
        DEVELOPER1,
        DEVELOPER2,
        RESOURCE_OWNER,
        OPERATOR
    );
    Config config = new Config()
        .users(BROKER_USER, otherUsers);
    rbacClusters = new RbacClusters(config);

    rbacClusters.kafkaCluster.createTopic(APP1_TOPIC, 2, 1);
    rbacClusters.kafkaCluster.createTopic(APP2_TOPIC, 2, 1);
    clusterId = rbacClusters.kafkaClusterId();

    initializeRoles();
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (rbacClusters != null)
        rbacClusters.shutdown();
    } finally {
      SecurityTestUtils.clearSecurityConfigs();
      KafkaTestUtils.verifyThreadCleanup();
    }
  }

  @Test
  public void testProduceConsumeWithRbac() throws Throwable {
    rbacClusters.produceConsume(DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
    rbacClusters.produceConsume(DEVELOPER2, APP1_TOPIC, APP1_CONSUMER_GROUP, false);
    rbacClusters.produceConsume(DEVELOPER2, APP2_TOPIC, APP2_CONSUMER_GROUP, true);
    rbacClusters.produceConsume(SUPER_USER, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
    rbacClusters.produceConsume(SUPER_USER, APP2_TOPIC, APP1_CONSUMER_GROUP, true);
    rbacClusters.produceConsume(RESOURCE_OWNER, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
    rbacClusters.produceConsume(RESOURCE_OWNER, APP2_TOPIC, APP1_CONSUMER_GROUP, true);
  }

  @Test
  public void testProduceConsumeWithGroupRoles() throws Throwable {
    rbacClusters.updateUserGroup(DEVELOPER2, DEVELOPER_GROUP);
    rbacClusters.assignRole(AccessRule.GROUP_PRINCIPAL_TYPE, DEVELOPER_GROUP, "DeveloperRead", clusterId,
        Utils.mkSet(new ResourcePattern("Group", APP1_CONSUMER_GROUP, PatternType.LITERAL),
            new ResourcePattern("Topic", APP1_TOPIC, PatternType.LITERAL)));
    rbacClusters.assignRole(AccessRule.GROUP_PRINCIPAL_TYPE, DEVELOPER_GROUP, "DeveloperWrite", clusterId,
        Utils.mkSet(new ResourcePattern("Topic", APP1_TOPIC, PatternType.LITERAL)));
    rbacClusters.waitUntilAccessAllowed(DEVELOPER2, APP1_TOPIC);
    rbacClusters.produceConsume(DEVELOPER2, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
  }

  @Test
  public void testAuthorizationWithRolesInOtherScopes() throws Throwable {
    createAdditionalRoles("confluent/core/anotherCluster");
    createAdditionalRoles("confluent/anotherDepartment/testCluster");
    rbacClusters.produceConsume(DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
  }

  private void initializeRoles() throws Exception {
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER1, "DeveloperRead", clusterId,
        Utils.mkSet(new ResourcePattern("Topic", APP1_TOPIC, PatternType.LITERAL),
            new ResourcePattern("Group", APP1_CONSUMER_GROUP, PatternType.LITERAL)));
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER1, "DeveloperWrite", clusterId,
        Utils.mkSet(new ResourcePattern("Topic", APP1_TOPIC, PatternType.LITERAL)));
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER2, "DeveloperRead", clusterId,
        Utils.mkSet(new ResourcePattern("Topic", "app2", PatternType.PREFIXED),
            new ResourcePattern("Group", "app2", PatternType.PREFIXED)));
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER2, "DeveloperWrite", clusterId,
        Utils.mkSet(new ResourcePattern("Topic", "app2", PatternType.PREFIXED)));
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, RESOURCE_OWNER, "ResourceOwner", clusterId,
        Utils.mkSet(new ResourcePattern("Topic", "*", PatternType.LITERAL),
            new ResourcePattern("Group", "*", PatternType.LITERAL)));
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, OPERATOR, "Operator", clusterId, Collections.emptySet());
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, SUPER_USER, "SuperUser", clusterId, Collections.emptySet());

    rbacClusters.waitUntilAccessAllowed(DEVELOPER1, APP1_TOPIC);
    rbacClusters.waitUntilAccessAllowed(DEVELOPER2, APP2_TOPIC);
    rbacClusters.waitUntilAccessAllowed(RESOURCE_OWNER, APP1_TOPIC);
    rbacClusters.waitUntilAccessAllowed(SUPER_USER, APP1_TOPIC);
  }

  private void createAdditionalRoles(String cluster) throws Exception {
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER1, "DeveloperRead", cluster,
        Utils.mkSet(new ResourcePattern("Topic", APP1_TOPIC, PatternType.LITERAL),
            new ResourcePattern("Group", APP1_CONSUMER_GROUP, PatternType.LITERAL)));
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER2, "DeveloperRead", cluster,
        Utils.mkSet(new ResourcePattern("Topic", "app2", PatternType.PREFIXED),
            new ResourcePattern("Group", "app2", PatternType.PREFIXED)));
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, RESOURCE_OWNER, "ResourceOwner", cluster,
        Utils.mkSet(new ResourcePattern("Topic", "*", PatternType.LITERAL),
            new ResourcePattern("Group", "*", PatternType.LITERAL)));
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, OPERATOR, "Operator", cluster,
        Collections.emptySet());
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, SUPER_USER, "SuperUser", cluster,
        Collections.emptySet());
  }
}

