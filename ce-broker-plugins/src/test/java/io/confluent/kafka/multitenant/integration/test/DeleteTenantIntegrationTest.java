package io.confluent.kafka.multitenant.integration.test;

import io.confluent.kafka.multitenant.LogicalClusterMetadata;
import io.confluent.kafka.multitenant.PhysicalClusterMetadata;
import io.confluent.kafka.multitenant.Utils;
import io.confluent.kafka.multitenant.authorizer.MultiTenantAuthorizer;
import io.confluent.kafka.multitenant.integration.cluster.LogicalCluster;
import io.confluent.kafka.multitenant.integration.cluster.LogicalClusterUser;
import io.confluent.kafka.multitenant.integration.cluster.PhysicalCluster;
import io.confluent.kafka.server.plugins.policy.TopicPolicyConfig;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import kafka.admin.AclCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.confluent.kafka.multitenant.Utils.LC_META_ABC;
import static io.confluent.kafka.multitenant.Utils.LC_META_XYZ;
import static junit.framework.TestCase.assertTrue;


public class DeleteTenantIntegrationTest {

    private static final Long TEST_CACHE_RELOAD_DELAY_MS = TimeUnit.SECONDS.toMillis(5);
    // logical metadata file creation involves creating dirs, moving files, creating/deleting symlinks
    // so we will use longer timeout than in other tests
    private static final long TEST_MAX_WAIT_MS = TimeUnit.SECONDS.toMillis(60);

    private IntegrationTestHarness testHarness;
    private PhysicalCluster physicalCluster;
    private LogicalCluster lc1;
    private LogicalCluster lc2;
    private PhysicalClusterMetadata metadata;
    private List<NewTopic> sampleTopics = Collections.singletonList(new NewTopic("abcd", 3, (short) 1));
    private String internalBootstrap;
    private int adminUserId = 100;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {

        testHarness = new IntegrationTestHarness();

        physicalCluster = testHarness.start(brokerProps());

        lc1 = physicalCluster.createLogicalCluster(LC_META_ABC.logicalClusterId(), adminUserId, 9, 11, 12);
        lc2 = physicalCluster.createLogicalCluster(LC_META_XYZ.logicalClusterId(), adminUserId, 9, 11, 12);

        metadata = updatePhysicalClusterMetadata(physicalCluster);

        Utils.createLogicalClusterFile(LC_META_ABC, tempFolder);
        Utils.createLogicalClusterFile(LC_META_XYZ, tempFolder);
        TestUtils.waitForCondition(
                () -> metadata.logicalClusterIds().size() == 2,
                "Expected metadata of new logical cluster to be present in metadata cache");
    }

    @After
    public void tearDown() throws Exception {
        testHarness.shutdown();
    }

    private Properties brokerProps() throws IOException {
        Properties props = new Properties();
        props.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG,
                tempFolder.getRoot().getCanonicalPath());
        props.put(ConfluentConfigs.MULTITENANT_METADATA_CLASS_CONFIG,
                "io.confluent.kafka.multitenant.PhysicalClusterMetadata");
        props.put(KafkaConfig$.MODULE$.AuthorizerClassNameProp(), MultiTenantAuthorizer.class.getName());
        props.put(MultiTenantAuthorizer.MAX_ACLS_PER_TENANT_PROP, "100"); // this enables ACLs
        props.put(ConfluentConfigs.MULTITENANT_METADATA_RELOAD_DELAY_MS_CONFIG,
                TEST_CACHE_RELOAD_DELAY_MS);
        // for most tests, we want to delete tenants immediately
        props.put(ConfluentConfigs.MULTITENANT_TENANT_DELETE_DELAY_MS_CONFIG, "0");
        return props;
    }


    @Test
    public void testDeleteSingleTenantWithOneTopic() throws InterruptedException, IOException, ExecutionException {

        // Create topic with same name in two logical clusters
        AdminClient adminClient1 = testHarness.createAdminClient(lc1.adminUser());
        AdminClient adminClient2 = testHarness.createAdminClient(lc2.adminUser());

        adminClient1.createTopics(sampleTopics).all().get();
        adminClient2.createTopics(sampleTopics).all().get();

        // Check that both clusters have the topics
        List<String> expectedTopics = sampleTopics.stream().map(NewTopic::name)
                .collect(Collectors.toList());
        assertTrue(adminClient1.listTopics().names().get().containsAll(expectedTopics));
        assertTrue(adminClient2.listTopics().names().get().containsAll(expectedTopics));

        LogicalClusterMetadata deleted = deleteLogicalCluster(LC_META_ABC);

        // Wait for tenant to disappear
        TestUtils.waitForCondition(
               () -> !metadata.logicalClusterIds().contains(deleted.logicalClusterId()),
                TEST_MAX_WAIT_MS,
                "Expect that the tenant is gone");


        // And make sure the topics are gone
        TestUtils.waitForCondition(
                () -> {
                    try {
                        return adminClient1.listTopics().names().get().size() == 0;
                    } catch (Exception e) {
                        return false;
                    }
                },
                TEST_MAX_WAIT_MS,
                "Expecting that the tenant topics were deleted");

        // Make sure the other cluster still has topics!
       assertTrue(adminClient2.listTopics().names().get().containsAll(expectedTopics));
    }

    @Test
    public void testDeleteSingleTenantWithACLs() throws InterruptedException, IOException,
            ExecutionException {

        // Check that ACL was created
        LogicalClusterUser user = lc1.user(9);
        LogicalClusterUser user2 = lc2.user(11);

        AclCommand.main(SecurityTestUtils.produceAclArgs(testHarness.zkConnect(),
                user.prefixedKafkaPrincipal(), user.withPrefix("topic1"), PatternType.LITERAL));
        AclCommand.main(SecurityTestUtils.consumeAclArgs(testHarness.zkConnect(),
                user.prefixedKafkaPrincipal(), user.withPrefix("topic2."), user.withPrefix("group"),
                PatternType.PREFIXED));
        // Also creating ACL for a user on the second logical cluster. Lets not delete this one
        AclCommand.main(SecurityTestUtils.produceAclArgs(testHarness.zkConnect(),
                user2.prefixedKafkaPrincipal(), user2.withPrefix("topic1"), PatternType.LITERAL));

        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, internalBootstrap);

        AdminClient superClient = physicalCluster.superAdminClient();
        Collection<AclBinding> describedAcls = superClient.describeAcls(new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY),
                new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY)
        )).values().get();

        assertTrue("ACLs should exist", describedAcls.size() > 0);

        LogicalClusterMetadata deleted = deleteLogicalCluster(LC_META_ABC);

        // Wait for tenant to disappear
        TestUtils.waitForCondition(
                () -> !metadata.logicalClusterIds().contains(deleted.logicalClusterId()),
                TEST_MAX_WAIT_MS,
                "Expect that the tenant is gone");

        // And make sure the ACLs for first tenant are gone
        TestUtils.waitForCondition(
                () -> {
                    try {
                        return superClient.describeAcls(new AclBindingFilter(
                                new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY),
                                new AccessControlEntryFilter(user.prefixedKafkaPrincipal().toString(),
                                        null, AclOperation.ANY, AclPermissionType.ANY)
                        )).values().get().size() == 0;
                    } catch (Exception e) {
                        return false;
                    }
                },
                TEST_MAX_WAIT_MS,
                "Expecting that the tenant ACLs were deleted");

        // Check that we didn't delete ACLs for the other tenant
        describedAcls.clear();
        describedAcls = superClient.describeAcls(new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY),
                new AccessControlEntryFilter(user2.prefixedKafkaPrincipal().toString(), null,
                        AclOperation.ANY, AclPermissionType.ANY)
        )).values().get();

        assertTrue("ACLs should exist", describedAcls.size() > 0);
    }

    @Test
    public void testHandleACLsDisabledCase() throws Exception {

        // we need a new physical cluster where ACLs are disabled

        Properties props = new Properties();
        props.put(KafkaConfig.BrokerIdProp(), 100);
        props.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG,
                tempFolder.getRoot().getCanonicalPath());
        props.put(ConfluentConfigs.MULTITENANT_METADATA_CLASS_CONFIG,
                "io.confluent.kafka.multitenant.PhysicalClusterMetadata");
        props.put(KafkaConfig$.MODULE$.AuthorizerClassNameProp(), MultiTenantAuthorizer.class.getName());
        props.put(ConfluentConfigs.MULTITENANT_METADATA_RELOAD_DELAY_MS_CONFIG,
                TEST_CACHE_RELOAD_DELAY_MS);
        props.put(ConfluentConfigs.MULTITENANT_TENANT_DELETE_DELAY_MS_CONFIG, "0");

        testHarness.shutdown();

        PhysicalCluster physicalCluster = testHarness.start(props);
        PhysicalClusterMetadata metadata = updatePhysicalClusterMetadata(physicalCluster);

        Utils.createLogicalClusterFile(LC_META_ABC, tempFolder);

        // delete the clusters
        LogicalClusterMetadata deleted1 = deleteLogicalCluster(LC_META_ABC);

        // make sure it is completely deleted
        TestUtils.waitForCondition(
                () ->   metadata.tenantLifecycleManager.fullyDeletedClusters().contains(deleted1.logicalClusterId()),
                TEST_MAX_WAIT_MS,
                "Expect that the tenants are gone");
    }

    @Test
    public void testDelayedDelete() throws Exception {

        int delaySec = 10;

        // create physical cluster with delay, wait for tenant to get created and then delete it

        Properties props = new Properties();
        props.put(KafkaConfig.BrokerIdProp(), 100);
        props.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG,
                tempFolder.getRoot().getCanonicalPath());
        props.put(ConfluentConfigs.MULTITENANT_METADATA_CLASS_CONFIG,
                "io.confluent.kafka.multitenant.PhysicalClusterMetadata");
        props.put(KafkaConfig$.MODULE$.AuthorizerClassNameProp(), MultiTenantAuthorizer.class.getName());
        props.put(ConfluentConfigs.MULTITENANT_METADATA_RELOAD_DELAY_MS_CONFIG,
                TEST_CACHE_RELOAD_DELAY_MS);
        props.put(ConfluentConfigs.MULTITENANT_TENANT_DELETE_DELAY_MS_CONFIG,
                TimeUnit.SECONDS.toMillis(delaySec));

        testHarness.shutdown();

        PhysicalCluster physicalCluster = testHarness.start(props);
        PhysicalClusterMetadata metadata = updatePhysicalClusterMetadata(physicalCluster);

        Utils.createLogicalClusterFile(LC_META_ABC, tempFolder);

        TestUtils.waitForCondition(
                () -> metadata.logicalClusterIds().contains(LC_META_ABC.logicalClusterId()),
                TEST_MAX_WAIT_MS,
                "Tenant wasn't created on time");

        LogicalClusterMetadata deleted = deleteLogicalCluster(LC_META_ABC);

        // wait for tenant to be around and marked for deletion

        TestUtils.waitForCondition(
                () -> !metadata.logicalClusterIds().contains(deleted.logicalClusterId()) &&
                        !metadata.tenantLifecycleManager.deletedClusters().contains(deleted.logicalClusterId()),
                TEST_MAX_WAIT_MS,
                "Tenant should not be part of the cache but not deleted either");


        // wait a bit more and check that it is really gone
        TestUtils.waitForCondition(
                () -> metadata.tenantLifecycleManager.fullyDeletedClusters().contains(deleted.logicalClusterId()),
                TEST_MAX_WAIT_MS,
                "Tenant was not deleted as expected");
    }

    // We "delete" tenants by generating new metadata with a delete date
    private LogicalClusterMetadata deleteLogicalCluster(LogicalClusterMetadata lkc) throws IOException {
        LogicalClusterMetadata deleted =
                new LogicalClusterMetadata(lkc.logicalClusterId(), lkc.physicalClusterId(), lkc.logicalClusterName(),
                        lkc.accountId(), lkc.k8sClusterId(), lkc.logicalClusterType(),
                        lkc.storageBytes(), lkc.producerByteRate(), lkc.consumerByteRate(),
                        lkc.brokerRequestPercentage().longValue(), lkc.networkQuotaOverhead(),
                        new LogicalClusterMetadata.LifecycleMetadata(lkc.lifecycleMetadata().logicalClusterName(),
                                lkc.lifecycleMetadata().physicalK8sNamespace(),
                                lkc.lifecycleMetadata().creationDate(),
                                new Date()));
        Utils.updateLogicalClusterFile(deleted, tempFolder);
        return deleted;
    }

    // Update the metadata plugin with a valid internal listener configuration
    private PhysicalClusterMetadata updatePhysicalClusterMetadata(PhysicalCluster physicalCluster) {
        PhysicalClusterMetadata metadata =
                (PhysicalClusterMetadata) physicalCluster.kafkaCluster().brokers().get(0).multitenantMetadata();

        Map<String, Object> configs = new HashMap<>();
        internalBootstrap =
                "INTERNAL://" + physicalCluster.kafkaCluster().kafkas().get(0).brokerConnect("INTERNAL");
        configs.put(KafkaConfig.AdvertisedListenersProp(), internalBootstrap);
        configs.put(TopicPolicyConfig.INTERNAL_LISTENER_CONFIG,
                TopicPolicyConfig.DEFAULT_INTERNAL_LISTENER);
        metadata.tenantLifecycleManager.updateAdminClient(configs);

        return metadata;
    }

}
