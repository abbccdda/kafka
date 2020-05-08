// (Copyright) [2020 - 2020] Confluent, Inc.

package io.confluent.security.test.integration.rbac;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.security.test.utils.RbacClusters;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Category(IntegrationTest.class)
public class RbacSbkIntegrationTest {

    private static final String BROKER_USER = "kafka";
    private static final String SYSTEM_ADMIN_USER = "system-admin";
    private static final String CLUSTER_ADMIN_USER = "cluster-admin";
    private static final String RESOURCE_OWNER_USER = "topic-owner";
    private static final String TOPIC = "test-topic";

    private RbacClusters.Config rbacConfig;
    private RbacClusters rbacClusters;

    @Before
    public void setUp() throws Throwable {
        List<String> otherUsers = Arrays.asList(
            SYSTEM_ADMIN_USER,
            CLUSTER_ADMIN_USER,
            RESOURCE_OWNER_USER
        );
        rbacConfig = new RbacClusters.Config()
            .users(BROKER_USER, otherUsers);
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (rbacClusters != null) {
                rbacClusters.shutdown();
                rbacClusters = null;
            }

        } finally {
            SecurityTestUtils.clearSecurityConfigs();
            KafkaTestUtils.verifyThreadCleanup();
        }
    }

    @Test
    public void testSBKApiAccessWithRbac() throws Throwable {
        rbacClusters = new RbacClusters(rbacConfig);
        initializeRoles();

        verifyRemoveBrokersAPIAccess(BROKER_USER, true);
        verifyRemoveBrokersAPIAccess(SYSTEM_ADMIN_USER, true);
        verifyRemoveBrokersAPIAccess(CLUSTER_ADMIN_USER, true);
        verifyRemoveBrokersAPIAccess(RESOURCE_OWNER_USER, false);
    }

    private void initializeRoles() throws Exception {
        final String clusterId = rbacClusters.kafkaClusterId();
        rbacClusters.kafkaCluster.createTopic(TOPIC, 2, 1);

        rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, RESOURCE_OWNER_USER, "ResourceOwner", clusterId,
            Utils.mkSet(new io.confluent.security.authorizer.ResourcePattern("Topic", "*", PatternType.LITERAL)));

        rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, CLUSTER_ADMIN_USER, "ClusterAdmin", clusterId, Collections.emptySet());
        rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, SYSTEM_ADMIN_USER, "SystemAdmin", clusterId, Collections.emptySet());

        rbacClusters.waitUntilAccessAllowed(RESOURCE_OWNER_USER, TOPIC);
        rbacClusters.waitUntilAccessAllowed(CLUSTER_ADMIN_USER, TOPIC);
        rbacClusters.waitUntilAccessAllowed(SYSTEM_ADMIN_USER, TOPIC);
    }


    private void verifyRemoveBrokersAPIAccess(String user, boolean authorized) throws Throwable {
        KafkaTestUtils.ClientBuilder clientBuilder = rbacClusters.clientBuilder(user);
        try (KafkaAdminClient adminClient = (KafkaAdminClient) clientBuilder.buildAdminClient()) {
            KafkaFuture<List<Integer>> future = adminClient.removeBrokers(Collections.singletonList(0)).all();
            if (!authorized)
                TestUtils.assertFutureError(future, ClusterAuthorizationException.class);
            else
                // This fails as we try to remove broker having partitions with RF = 1.
                TestUtils.assertFutureError(future, InvalidRequestException.class);
        }
    }
}

