// (Copyright) [2020 - 2020] Confluent, Inc.

package io.confluent.security.auth.provider;



import io.confluent.security.authorizer.Scope;
import io.confluent.security.rbac.RbacRoles;
import java.util.Collections;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This test class tests custom roles not available in the cloud default roles, specifically roles that bind to resources.
 */
public class CloudCustomProviderTest extends CloudBaseProviderTest {


    @Before
    public void setUp() throws Exception {
        RbacRoles rbacRoles = RbacRoles
            .load(this.getClass().getClassLoader(), "test_cloud_rbac_roles.json");
        initializeRbacProvider("clusterA", Scope.ROOT_SCOPE,
            Collections.singletonMap(KafkaConfig.AuthorizerClassNameProp(),
                ConfluentConfigs.MULTITENANT_AUTHORIZER_CLASS_NAME),
            rbacRoles);
    }

    @After
    public void tearDown() {
        if (rbacProvider != null) {
            rbacProvider.close();
        }
    }

    @Test
    public void testInteractionMultipleEnvironments() {
        updateRoleBinding(alice, "DeveloperRead", kafkaClusterA, Collections.singleton(topic));
        updateRoleBinding(alice, "DeveloperRead", kafkaClusterC, Collections.singleton(topic));

        verifyRules(alice, groups, organization123, org, "Describe");
        verifyRules(alice, groups, organization789, org);

        verifyRules(alice, groups, environmentT55, envT55, "Describe");
        verifyRules(alice, groups, environmentT66, envT66, "Describe");

        verifyRules(alice, groups, clusterA, cloudClusterResource, "Describe");
        verifyRules(alice, groups, kafkaClusterA, topic, "Read");
        verifyRules(alice, groups, kafkaClusterA, topicB);

        verifyRules(alice, groups, clusterB, cloudClusterResource);
        verifyRules(alice, groups, kafkaClusterB, topic);
        verifyRules(alice, groups, kafkaClusterB, topicB);

        verifyRules(alice, groups, clusterC, cloudClusterResource, "Describe");
        verifyRules(alice, groups, kafkaClusterC, topic, "Read");
        verifyRules(alice, groups, kafkaClusterC, topicB);

        verifyRules(alice, groups, wrongEnvironmentT55, envT55);
        verifyRules(alice, groups, wrongClusterB, cloudClusterResource);

        deleteRoleBinding(alice, "DeveloperRead", kafkaClusterA);

        // still have our org permissions
        verifyRules(alice, groups, organization123, org, "Describe");

        // still have our env permissions
        verifyRules(alice, groups, environmentT55, envT55);
        verifyRules(alice, groups, environmentT66, envT66, "Describe");

        verifyRules(alice, groups, clusterA, cloudClusterResource);
        verifyRules(alice, groups, kafkaClusterA, topic);

        // still have our resource access in one cluster's topic
        verifyRules(alice, groups, clusterC, cloudClusterResource, "Describe");
        verifyRules(alice, groups, kafkaClusterC, topic, "Read");

        verifyRules(alice, groups, wrongEnvironmentT55, envT55);
        verifyRules(alice, groups, wrongClusterB, cloudClusterResource);

        deleteRoleBinding(alice, "DeveloperRead", kafkaClusterC);

        verifyNoPermissions();
    }

    private void verifyNoPermissions() {
        verifyRules(alice, groups, organization123, org);
        verifyRules(alice, groups, organization789, org);

        verifyRules(alice, groups, environmentT55, envT55);
        verifyRules(alice, groups, environmentT66, envT66);

        verifyRules(alice, groups, clusterA, cloudClusterResource);
        verifyRules(alice, groups, kafkaClusterA, topic);

        verifyRules(alice, groups, clusterB, cloudClusterResource);
        verifyRules(alice, groups, kafkaClusterB, topic);

        verifyRules(alice, groups, wrongEnvironmentT55, envT55);
        verifyRules(alice, groups, wrongClusterB, cloudClusterResource);
    }
}
