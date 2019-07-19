package io.confluent.kafka.multitenant;

import io.confluent.kafka.multitenant.schema.TenantContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.confluent.kafka.multitenant.Utils.LC_META_ABC;
import static io.confluent.kafka.multitenant.Utils.LC_META_DED;
import static io.confluent.kafka.multitenant.Utils.LC_META_MEH;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TenantLifecycleManagerTest {

    private static final Long TEST_DELETE_DELAY_MS = 0L;
    private AdminClient mockAdminClient;
    private TenantLifecycleManager lifecycleManagerWithDeleteDelay;
    private TenantLifecycleManager lifecycleManager;

    @Before
    public void setUp() throws Exception {
        // Using mock AdminClient for the tests
        Node node = new Node(1, "localhost", 9092);
        mockAdminClient = spy(new MockAdminClient(singletonList(node), node));
        DescribeAclsResult emptyAcls = mock(DescribeAclsResult.class);
        doReturn(KafkaFuture.completedFuture(Collections.emptySet())).when(emptyAcls).values();
        doReturn(emptyAcls).when(mockAdminClient).describeAcls(any(), any());
        doReturn(null).when(mockAdminClient).deleteAcls(any(), any()); // return value is ignored

        lifecycleManager = new TenantLifecycleManager(TEST_DELETE_DELAY_MS, mockAdminClient);
        lifecycleManagerWithDeleteDelay = new TenantLifecycleManager(ConfluentConfigs.MULTITENANT_TENANT_DELETE_DELAY_MS_DEFAULT,
                mockAdminClient);

        // Add few tenants to TenantLifecycleManager
        lifecycleManagerWithDeleteDelay.updateTenantState(LC_META_ABC);
        lifecycleManagerWithDeleteDelay.updateTenantState(LC_META_DED);
        lifecycleManagerWithDeleteDelay.updateTenantState(LC_META_MEH);
    }

    @Test
    public void testStateInitialization() {
        // Make sure our tenants are at the right phase of their lifecycle
        assertEquals("Expecting tenant LC_META_ABC to be active",
                TenantLifecycleManager.State.ACTIVE,
                lifecycleManagerWithDeleteDelay.tenantLifecycleState.get(LC_META_ABC.logicalClusterId()));
        assertEquals("Expecting tenant LC_META_DED to be deactivated",
                TenantLifecycleManager.State.DEACTIVATED,
                lifecycleManagerWithDeleteDelay.tenantLifecycleState.get(LC_META_DED.logicalClusterId()));
        assertEquals("Expecting tenant LC_META_MEH to be on the way to deletion",
                TenantLifecycleManager.State.DELETE_IN_PROGRESS,
                lifecycleManagerWithDeleteDelay.tenantLifecycleState.get(LC_META_MEH.logicalClusterId()));
    }

    @Test
    public void testInactiveClusters() {
        List<String> clusters = Arrays.asList(LC_META_DED.logicalClusterId(),
                LC_META_MEH.logicalClusterId());
        assertTrue("LC_META_DED and LC_META_MEH should be inactive",
                lifecycleManagerWithDeleteDelay.inactiveClusters().containsAll(clusters));
        assertFalse("LC_META_ABC should be active",
                lifecycleManagerWithDeleteDelay.inactiveClusters().contains(LC_META_ABC.logicalClusterId()));
    }

    @Test
    public void testOnlyDeleteTenantsOnce() throws ExecutionException, InterruptedException {
        // count how many times we are trying to delete a topic

        TenantContext tc = new TenantContext(new MultiTenantPrincipal("",
                new TenantMetadata(LC_META_DED.logicalClusterId(), LC_META_DED.logicalClusterId())));
        List<NewTopic> sampleTopics =
                Collections.singletonList(new NewTopic(tc.addTenantPrefix("topic"), 3, (short) 1));
        mockAdminClient.createTopics(sampleTopics).all().get();

        // load deleted tenant to state store and trigger delete
        lifecycleManager.updateTenantState(LC_META_DED);
        lifecycleManager.deleteTenants();

        // wait for async delete task started by `deleteTenants` to complete
        lifecycleManager.topicDeletionExecutor().submit(() -> { }).get();

        // trigger deletion again so the deletion will be finalized
        lifecycleManager.deleteTenants();

        // assert that our mocked admin client ran listTopics once to delete the tenant and one
        // to validate the topics are gone and the tenant is deleted
        verify(mockAdminClient, times(2)).listTopics(any());

        // verify that acls are described and then deleted
        verify(mockAdminClient, times(2)).describeAcls(any(), any());
        verify(mockAdminClient, times(2)).deleteAcls(any(), any());

        // Try updating metadata and deleting tenants again and check that we are not calling the
        // admin client again because tenant was already deleted
        reset(mockAdminClient);
        lifecycleManager.updateTenantState(LC_META_DED);
        lifecycleManager.deleteTenants();
        verify(mockAdminClient, never()).listTopics(any());
    }

    @Test
    public void testUndelete() throws IOException {
        // check that a cluster is inactive, remove its delete date, update and check that it is
        // active again
        assertEquals("Expecting tenant LC_META_DED to be deactivated",
                TenantLifecycleManager.State.DEACTIVATED,
                lifecycleManagerWithDeleteDelay.tenantLifecycleState.get(LC_META_DED.logicalClusterId()));

        LogicalClusterMetadata active = reactivateLogicalCluster(LC_META_DED);
        lifecycleManagerWithDeleteDelay.updateTenantState(active);

        assertEquals("Expecting tenant to be deactivated",
                TenantLifecycleManager.State.ACTIVE,
                lifecycleManagerWithDeleteDelay.tenantLifecycleState.get(active.logicalClusterId()));
    }

    private LogicalClusterMetadata reactivateLogicalCluster(LogicalClusterMetadata lkc) throws IOException {
        LogicalClusterMetadata deleted =
                new LogicalClusterMetadata(lkc.logicalClusterId(), lkc.physicalClusterId(), lkc.logicalClusterName(),
                        lkc.accountId(), lkc.k8sClusterId(), lkc.logicalClusterType(),
                        lkc.storageBytes(), lkc.producerByteRate(), lkc.consumerByteRate(),
                        lkc.brokerRequestPercentage().longValue(), lkc.networkQuotaOverhead(),
                        new LogicalClusterMetadata.LifecycleMetadata(lkc.lifecycleMetadata().logicalClusterName(),
                                lkc.lifecycleMetadata().physicalK8sNamespace(),
                                lkc.lifecycleMetadata().creationDate(),
                                null));
        return deleted;
    }
}
