// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant;

import com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.apache.kafka.test.TestUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static io.confluent.kafka.multitenant.Utils.LC_META_DED;
import static io.confluent.kafka.multitenant.Utils.LC_META_ABC;
import static io.confluent.kafka.multitenant.Utils.LC_META_XYZ;
import static io.confluent.kafka.multitenant.Utils.LC_META_HEALTHCHECK;
import static java.util.Collections.singletonList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.confluent.kafka.multitenant.quota.QuotaConfig;
import io.confluent.kafka.multitenant.quota.TenantQuotaCallback;
import io.confluent.kafka.multitenant.quota.TestCluster;

public class PhysicalClusterMetadataTest {

  private static final Long TEST_CACHE_RELOAD_DELAY_MS = TimeUnit.SECONDS.toMillis(5);

  // logical metadata file creation involves creating dirs, moving files, creating/deleting symlinks
  // so we will use longer timeout than in other tests
  private static final long TEST_MAX_WAIT_MS = TimeUnit.SECONDS.toMillis(60);
  private static final String SSL_CERTS_DIR = "mnt/sslcerts/";
  private static final String BROKER_ID = "1";
  private static final String BROKER_UUID = "test-uuid-3";
  private static final URL TEST_SSL_CERTS_AUG = PhysicalClusterMetadataTest.class.getClass().getResource("/cert_exp_aug");
  private static final URL TEST_SSL_CERTS_MAY = PhysicalClusterMetadataTest.class.getClass().getResource("/cert_exp_may");
  private static final URL TEST_ROOT = PhysicalClusterMetadataTest.class.getClass().getResource("/");

  private AdminClient mockAdminClient;
  private PhysicalClusterMetadata lcCache;
  private String sslCertsPath;
  private String endpoint;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    lcCache = new PhysicalClusterMetadata();
    Node node = new Node(1, "localhost", 9092);
    endpoint = node.host() + ":" + node.port();
    mockAdminClient = spy(new MockAdminClient(singletonList(node), node));
    sslCertsPath = tempFolder.getRoot().getCanonicalPath() + "/" + SSL_CERTS_DIR + "spec.json";
    String logicalClustersDir = tempFolder.getRoot().getCanonicalPath();
    lcCache.configure(logicalClustersDir, TEST_CACHE_RELOAD_DELAY_MS, mockAdminClient, BROKER_ID, sslCertsPath);
  }

  @After
  public void tearDown() {
    lcCache.shutdown();
  }

  @Test
  public void testCreateAndRemoveInstance() throws Exception {
    final String brokerUUID = "test-uuid";
    Map<String, Object> configs = new HashMap<>();
    configs.put("broker.session.uuid", String.valueOf(brokerUUID));
    configs.put("broker.id", "0");
    // will create directory if it does not exist
    configs.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG,
                tempFolder.getRoot().getCanonicalPath() + "/subdir/anotherdir/");

    // get instance does not create instance
    assertNull(PhysicalClusterMetadata.getInstance(brokerUUID));

    final PhysicalClusterMetadata metadata = new PhysicalClusterMetadata();
    metadata.configure(configs);
    assertTrue("Expected cache to be initialized", metadata.isUp());
    assertEquals(metadata, PhysicalClusterMetadata.getInstance(brokerUUID));
    metadata.close(brokerUUID);
    assertNull(PhysicalClusterMetadata.getInstance(brokerUUID));
  }

  @Test(expected = ConfigException.class)
  public void testConfigureInstanceWithoutDirConfigThrowsException() {
    Map<String, Object> configs = new HashMap<>();
    configs.put("broker.session.uuid", "test-uuid-1");
    final PhysicalClusterMetadata metadata = new PhysicalClusterMetadata();
    metadata.configure(configs);
  }

  @Test
  public void testConfigureInstanceWithSameBrokerUuid() throws IOException {
    final String brokerUUID = "test-uuid-2";
    Map<String, Object> configs = new HashMap<>();
    configs.put("broker.session.uuid", brokerUUID);
    configs.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG,
                tempFolder.getRoot().getCanonicalPath());

    final PhysicalClusterMetadata meta1 = new PhysicalClusterMetadata();
    meta1.configure(configs);
    assertEquals(meta1, PhysicalClusterMetadata.getInstance(brokerUUID));
    // configure() on the same instance and broker UUId does nothing
    meta1.configure(configs);
    assertEquals(meta1, PhysicalClusterMetadata.getInstance(brokerUUID));

    final PhysicalClusterMetadata meta2 = new PhysicalClusterMetadata();
    // configuring another instance with the same broker uuid should fail
    try {
      meta2.configure(configs);
      fail("Exception not thrown when configuring another instance with the same broker UUID");
    } catch (UnsupportedOperationException e) {
      // success, but verify that we can still get an original instance for this broker UUID
      assertEquals(meta1, PhysicalClusterMetadata.getInstance(brokerUUID));
    }

    // close() on second instance which we failed to configure should not shutdown and remove the
    // original instance with the same broker UUID
    meta2.close(brokerUUID);
    assertEquals(meta1, PhysicalClusterMetadata.getInstance(brokerUUID));
    meta1.close(brokerUUID);
    assertNull(PhysicalClusterMetadata.getInstance(brokerUUID));
  }

  @Test
  public void testStartWithInaccessibleDirShouldThrowException() throws IOException {
    //tempFolder.newFolder("logical_clusters");
    //File logicalCluster = new File(logicalClustersDir);
    assertTrue(tempFolder.getRoot().setReadable(false));
    try {
      lcCache.start();
      fail("IOException not thrown when starting with inaccessible directory.");
    } catch (IOException ioe) {
      // success
      assertFalse(lcCache.isUp());
      // make sure we don't leak watcher service
      assertFalse(lcCache.dirWatcher.isRegistered());
    }

    // should be able to start() once directory is readable
    assertTrue(tempFolder.getRoot().setReadable(true));
    lcCache.start();
    assertTrue(lcCache.isUp());
  }

  @Test
  public void testExistingFilesLoaded() throws IOException, InterruptedException {
    Utils.createLogicalClusterFile(LC_META_ABC, tempFolder);

    lcCache.start();
    assertTrue("Expected cache to be initialized", lcCache.isUp());
    assertEquals(LC_META_ABC, lcCache.metadata(LC_META_ABC.logicalClusterId()));
    assertEquals(ImmutableSet.of(LC_META_ABC.logicalClusterId()), lcCache.logicalClusterIds());
  }

  @Test
  public void testHandlesFileEvents() throws IOException, InterruptedException {
    lcCache.start();
    assertTrue(lcCache.isUp());
    assertEquals(ImmutableSet.of(), lcCache.logicalClusterIds());
    assertEquals(ImmutableSet.of(), lcCache.logicalClusterIdsIncludingStale());

    // create new file and ensure cache gets updated
    final String lcXyzId = LC_META_XYZ.logicalClusterId();
    Utils.createLogicalClusterFile(LC_META_XYZ, tempFolder);
    Utils.createLogicalClusterFile(LC_META_ABC, tempFolder);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(lcXyzId) != null,
        TEST_MAX_WAIT_MS,
        "Expected metadata of new logical cluster to be present in metadata cache");
    assertEquals(LC_META_XYZ, lcCache.metadata(LC_META_XYZ.logicalClusterId()));
    TestUtils.waitForCondition(
        () -> lcCache.metadata(LC_META_ABC.logicalClusterId()) != null,
        TEST_MAX_WAIT_MS,
        "Expected metadata of new logical cluster to be present in metadata cache");
    assertEquals(LC_META_ABC, lcCache.metadata(LC_META_ABC.logicalClusterId()));
    assertEquals(lcCache.logicalClusterIds(), lcCache.logicalClusterIdsIncludingStale());

    // update logical cluster
    LogicalClusterMetadata updatedLcMeta = new LogicalClusterMetadata(
        LC_META_XYZ.logicalClusterId(), LC_META_XYZ.physicalClusterId(),
        "new-name", "new-account", LC_META_XYZ.k8sClusterId(),
        LC_META_XYZ.logicalClusterType(), LC_META_XYZ.storageBytes(),
        LC_META_XYZ.producerByteRate(), LC_META_XYZ.consumerByteRate(),
        LC_META_XYZ.brokerRequestPercentage().longValue(), LC_META_XYZ.networkQuotaOverhead(), null
    );
    Utils.updateLogicalClusterFile(updatedLcMeta, tempFolder);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(lcXyzId).logicalClusterName().equals("new-name"),
        TEST_MAX_WAIT_MS,
        "Expected metadata to be updated");

    // delete logical cluster
    Utils.deleteLogicalClusterFile(updatedLcMeta, tempFolder);
    Utils.deleteLogicalClusterFile(LC_META_ABC, tempFolder);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(lcXyzId) == null,
        TEST_MAX_WAIT_MS,
        "Expected metadata to be removed from the cache");
  }

  @Test
  public void testEventsForJsonFileWithInvalidContentDoNotImpactValidLogicalClusters()
      throws IOException, InterruptedException {
    Utils.createInvalidLogicalClusterFile(LC_META_ABC, tempFolder);
    Utils.createLogicalClusterFile(LC_META_XYZ, tempFolder);

    lcCache.start();
    assertEquals(ImmutableSet.of(LC_META_XYZ.logicalClusterId()), lcCache.logicalClusterIds());
    assertEquals(ImmutableSet.of(LC_META_XYZ.logicalClusterId(), LC_META_ABC.logicalClusterId()),
                 lcCache.logicalClusterIdsIncludingStale());

    // update file with invalid content with another invalid content
    Utils.updateInvalidLogicalClusterFile(LC_META_ABC, tempFolder);

    // we cannot verify that an update event was handled for already invalid file, so create
    // another valid file which should be an event after a file update event
    final LogicalClusterMetadata anotherMeta = new LogicalClusterMetadata(
        "lkc-123", "pkc-123", "123", "my-account", "k8s-123",
        LogicalClusterMetadata.KAFKA_LOGICAL_CLUSTER_TYPE,
        10485760L, 102400L, 204800L,
        LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE_PER_BROKER.longValue(),
        LogicalClusterMetadata.DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE, null);
    Utils.createLogicalClusterFile(anotherMeta, tempFolder);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(anotherMeta.logicalClusterId()) != null,
        TEST_MAX_WAIT_MS,
        "Expected metadata of new logical cluster to be present in metadata cache");
    assertEquals(ImmutableSet.of(LC_META_XYZ.logicalClusterId(), anotherMeta.logicalClusterId()),
                 lcCache.logicalClusterIds());

    // deleting file with invalid content should result in no stale clusters
    Utils.deleteLogicalClusterFile(LC_META_ABC, tempFolder);
    TestUtils.waitForCondition(
        () -> lcCache.logicalClusterIds().size() == lcCache.logicalClusterIdsIncludingStale().size(),
        TEST_MAX_WAIT_MS,
        "Deleting file with bad content should remove corresponding logical cluster from stale list."
    );
    assertEquals(ImmutableSet.of(LC_META_XYZ.logicalClusterId(), anotherMeta.logicalClusterId()),
                 lcCache.logicalClusterIds());
    assertEquals(ImmutableSet.of(LC_META_XYZ.logicalClusterId(), anotherMeta.logicalClusterId()),
                 lcCache.logicalClusterIdsIncludingStale());

    // ensure we can re-create file with the same name but with good content
    Utils.createLogicalClusterFile(LC_META_ABC, tempFolder);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(LC_META_ABC.logicalClusterId()) != null,
        TEST_MAX_WAIT_MS,
        "Expected metadata of new logical cluster to be present in metadata cache");
    assertEquals(
        ImmutableSet.of(LC_META_XYZ.logicalClusterId(),
                        anotherMeta.logicalClusterId(),
                        LC_META_ABC.logicalClusterId()),
        lcCache.logicalClusterIds());
  }

  @Test
  public void testWatcherIsClosedAfterShutdown() throws IOException, InterruptedException {
    assertFalse(lcCache.dirWatcher.isRegistered());
    lcCache.start();
    // wait until WatchService is started, which happens on a separate thread
    TestUtils.waitForCondition(
        lcCache.dirWatcher::isRegistered, "Timed out waiting for WatchService to start.");
    lcCache.shutdown();
    assertFalse(lcCache.dirWatcher.isRegistered());
  }

  @Test(expected = IllegalStateException.class)
  public void testStartAfterShutdownShouldThrowException() throws IOException {
    lcCache.shutdown();
    lcCache.start();
  }

  @Test(expected = IllegalStateException.class)
  public void testLogicalClusterIdsAfterShutdownShouldThrowException() throws IOException {
    lcCache.start();
    lcCache.shutdown();
    lcCache.logicalClusterIds();
  }

  @Test(expected = IllegalStateException.class)
  public void testLogicalClusterIdsBeforeStartShouldThrowException() {
    lcCache.logicalClusterIds();
  }

  @Test(expected = IllegalStateException.class)
  public void testGetMetadataAfterShutdownShouldThrowException() throws IOException {
    lcCache.start();
    lcCache.shutdown();
    lcCache.metadata("some-cluster-id");
  }

  @Test(expected = IllegalStateException.class)
  public void testGetMetadataBeforeStartShouldThrowException() {
    assertFalse(lcCache.isUp());
    lcCache.metadata("some-cluster-id");
  }

  @Test
  public void testShouldSilentlySkipSubdirectoryEvents() throws IOException, InterruptedException {
    lcCache.start();
    assertTrue(lcCache.isUp());

    final File subdir = tempFolder.newFolder("lkc-hjf");
    assertTrue(subdir.exists() && subdir.isDirectory());

    // we need to wait until the cache handles new subdir event, but there is no way to check
    // that it happened. Will create a new file as well, and hopefully that event will be behind
    // the `new dir` event
    Utils.createLogicalClusterFile(LC_META_ABC, tempFolder);
    Utils.createLogicalClusterFile(LC_META_XYZ, tempFolder);
    TestUtils.waitForCondition(
        () -> lcCache.logicalClusterIds().size() >= 2,
        TEST_MAX_WAIT_MS,
        "Expected two new logical clusters to be added to the cache.");

    assertEquals(LC_META_XYZ, lcCache.metadata(LC_META_XYZ.logicalClusterId()));
    assertEquals(LC_META_ABC, lcCache.metadata(LC_META_ABC.logicalClusterId()));
    assertEquals(ImmutableSet.of(LC_META_ABC.logicalClusterId(), LC_META_XYZ.logicalClusterId()),
                 lcCache.logicalClusterIds());
    assertEquals(ImmutableSet.of(LC_META_ABC.logicalClusterId(), LC_META_XYZ.logicalClusterId()),
                 lcCache.logicalClusterIdsIncludingStale());
  }

  @Test
  public void testShouldRetryOnFailureToReadFile() throws IOException, InterruptedException {
    // create one file in logical clusters dir, but not readable
    Utils.createLogicalClusterFile(LC_META_ABC, tempFolder);
    Utils.setPosixFilePermissions(LC_META_ABC, "-wx-wx-wx", tempFolder);

    // we should be able to start the cache, but with scheduled retry
    lcCache.start();
    assertEquals(ImmutableSet.of(), lcCache.logicalClusterIds());
    assertEquals(ImmutableSet.of(LC_META_ABC.logicalClusterId()),
                 lcCache.logicalClusterIdsIncludingStale());

    // should be still able to update cache from valid files
    Utils.createLogicalClusterFile(LC_META_XYZ, tempFolder);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(LC_META_XYZ.logicalClusterId()) != null,
        TEST_MAX_WAIT_MS,
        "Expected metadata of 'lkc-xyz' logical cluster to be present in metadata cache");
    assertEquals(ImmutableSet.of(LC_META_XYZ.logicalClusterId()), lcCache.logicalClusterIds());

    // "fix" abc metadata file
    Utils.setPosixFilePermissions(LC_META_ABC, "rwxrwxrwx", tempFolder);

    TestUtils.waitForCondition(
        () -> lcCache.logicalClusterIds().size() == lcCache.logicalClusterIdsIncludingStale().size(),
        TEST_MAX_WAIT_MS,
        "Expected cache to recover");
    assertEquals(LC_META_ABC, lcCache.metadata(LC_META_ABC.logicalClusterId()));
  }

  @Test
  public void testListenerThreadShouldNotDieOnUnsetQuotas() throws IOException, InterruptedException {
    final LogicalClusterMetadata tenantWithUnsetQuota =
        new LogicalClusterMetadata(
            "lkc-inv", "pkc-a7sjfe", "my-poc-cluster", "my-account", "k8s-abc",
            LogicalClusterMetadata.KAFKA_LOGICAL_CLUSTER_TYPE,
            5242880000L, 104857600L, null, 0L,
            LogicalClusterMetadata.DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE,
            new LogicalClusterMetadata.LifecycleMetadata("my-poc-cluster", "pkc-a7sjfe", null, null));
    final int numBrokers = 8;
    TenantQuotaCallback quotaCallback = setupCallbackAndTenant(
        LC_META_ABC.logicalClusterId(), numBrokers);

    lcCache.configure(tempFolder.getRoot().getCanonicalPath(), TimeUnit.MINUTES.toMillis(60),
            mockAdminClient, BROKER_ID, sslCertsPath);
    lcCache.start();
    assertEquals(ImmutableSet.of(), lcCache.logicalClusterIds());

    // create logical cluster with unset request quota should still load logical cluster
    Utils.createLogicalClusterFile(tenantWithUnsetQuota, tempFolder);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(tenantWithUnsetQuota.logicalClusterId()) != null,
        TEST_MAX_WAIT_MS,
        "Expected metadata of new logical cluster to be present in metadata cache");
    Map<String, String> tags1 = Collections.singletonMap("tenant", tenantWithUnsetQuota.logicalClusterId());
    // no partitions on this broker --> min producer quota
    TestUtils.waitForCondition(
        () -> quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, tags1).longValue() ==
              TenantQuotaCallback.DEFAULT_MIN_BROKER_TENANT_PRODUCER_BYTE_RATE,
        TEST_MAX_WAIT_MS,
        "Expected min producer quota for tenant with no topics and limited quota");
    assertEquals(QuotaConfig.UNLIMITED_QUOTA.quota(ClientQuotaType.FETCH),
                 quotaCallback.quotaLimit(ClientQuotaType.FETCH, tags1), 0.001);
    assertEquals(LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE_PER_BROKER,
                 quotaCallback.quotaLimit(ClientQuotaType.REQUEST, tags1), 0.001);

    // add another logical cluster to verify that listener thread is still alive
    Utils.createLogicalClusterFile(LC_META_ABC, tempFolder);
    // will timeout if loaded on retry thread
    TestUtils.waitForCondition(
        () -> lcCache.metadata(LC_META_ABC.logicalClusterId()) != null,
        TEST_MAX_WAIT_MS,
        "Expected metadata of 'lkc-abc' logical cluster to be present in metadata cache");
    quotaCallback.updateClusterMetadata(quotaCallback.cluster());

    // verify quotas are also updated
    Map<String, String> tags = Collections.singletonMap("tenant", LC_META_ABC.logicalClusterId());
    TestUtils.waitForCondition(
        () -> quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, tags) >
              TenantQuotaCallback.DEFAULT_MIN_BROKER_TENANT_PRODUCER_BYTE_RATE,
        TEST_MAX_WAIT_MS,
        "Expected unlimited quota for tenant with no quota");

    assertEquals(10240000L / numBrokers,
                 quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, tags), 0.001);
    assertEquals(204800L / numBrokers,
                 quotaCallback.quotaLimit(ClientQuotaType.FETCH, tags), 0.001);
    assertEquals(LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE_PER_BROKER,
                 quotaCallback.quotaLimit(ClientQuotaType.REQUEST, tags), 0.001);
  }

  @Test
  public void testListenerThreadShouldNotDieOnException() throws IOException, InterruptedException {
    // long retry period, to make sure we test the listener thread
    TenantLifecycleManager lifecycleManager = spy(new TenantLifecycleManager(0, null));
    SslCertificateManager sslCertificateManager = new SslCertificateManager(BROKER_ID, sslCertsPath, mockAdminClient);
    lcCache.configure(tempFolder.getRoot().getCanonicalPath(), TimeUnit.MINUTES.toMillis(60),
            lifecycleManager, sslCertificateManager);
    lcCache.start();

    doThrow(new RuntimeException()).when(lifecycleManager).deleteTenants();

    Utils.createLogicalClusterFile(LC_META_ABC, tempFolder);
    // will timeout if loaded on retry thread
    TestUtils.waitForCondition(
        () -> lcCache.metadata(LC_META_ABC.logicalClusterId()) != null,
        TEST_MAX_WAIT_MS,
        "Expected metadata of 'lkc-abc' logical cluster to be present in metadata cache");

    // add another logical cluster to verify that listener thread is still alive
    Utils.createLogicalClusterFile(LC_META_XYZ, tempFolder);
    // will timeout if loaded on retry thread
    TestUtils.waitForCondition(
        () -> lcCache.metadata(LC_META_XYZ.logicalClusterId()) != null,
        TEST_MAX_WAIT_MS,
        "Expected metadata of 'lkc-xyz' logical cluster to be present in metadata cache");
  }

  @Test
  public void testShouldSkipInvalidJsonButUpdateCacheWhenJsonGetsFixed()
      throws IOException, InterruptedException {
    lcCache.start();
    assertTrue(lcCache.isUp());

    // initially create json file with invalid content
    Utils.createInvalidLogicalClusterFile(LC_META_ABC, tempFolder);
    // and create one valid file so that we know when update event gets handled
    Utils.createLogicalClusterFile(LC_META_XYZ, tempFolder);

    TestUtils.waitForCondition(
        () -> lcCache.metadata(LC_META_XYZ.logicalClusterId()) != null,
        TEST_MAX_WAIT_MS,
        "Expected metadata of 'lkc-xyz' logical cluster to be present in metadata cache");
    TestUtils.waitForCondition(
        () -> lcCache.logicalClusterIds().size() < lcCache.logicalClusterIdsIncludingStale().size(),
        TEST_MAX_WAIT_MS,
        "Expected inaccessible logical cluster file to cause stale cluster in cache.");
    assertEquals(ImmutableSet.of(LC_META_XYZ.logicalClusterId()), lcCache.logicalClusterIds());
    assertEquals(ImmutableSet.of(LC_META_XYZ.logicalClusterId(), LC_META_ABC.logicalClusterId()),
                 lcCache.logicalClusterIdsIncludingStale());

    // "fix" abc cluster meta, which should cause cache update
    Utils.updateLogicalClusterFile(LC_META_ABC, tempFolder);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(LC_META_ABC.logicalClusterId()) != null,
        TEST_MAX_WAIT_MS,
        "Expected metadata to be updated");
    assertEquals(ImmutableSet.of(LC_META_ABC.logicalClusterId(), LC_META_XYZ.logicalClusterId()),
                 lcCache.logicalClusterIds());
    assertEquals(ImmutableSet.of(LC_META_ABC.logicalClusterId(), LC_META_XYZ.logicalClusterId()),
                 lcCache.logicalClusterIdsIncludingStale());
  }

  @Test
  public void testShouldSkipInvalidMetadataButUpdateCacheWhenFixed()
      throws IOException, InterruptedException {
    final LogicalClusterMetadata lcMeta =
        new LogicalClusterMetadata("lkc-qwr", "pkc-qwr", "xyz", "my-account", "k8s-abc",
                                   LogicalClusterMetadata.KAFKA_LOGICAL_CLUSTER_TYPE,
                                   104857600L, 1024L, null,
                                   LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE_PER_BROKER.longValue(),
                                   LogicalClusterMetadata.DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE, null);

    lcCache.start();
    assertTrue(lcCache.isUp());

    // initially create json file with valid json content, but invalid metadata
    Utils.createLogicalClusterFile(lcMeta, tempFolder);
    TestUtils.waitForCondition(
        () -> lcCache.logicalClusterIdsIncludingStale().size() > 0,
        TEST_MAX_WAIT_MS,
        "Expected logical cluster with invalid metadata to be in the stale list");

    // "fix" cluster meta, which should cause cache update
    final LogicalClusterMetadata lcValidMeta = new LogicalClusterMetadata(
        "lkc-qwr", "pkc-qwr", "xyz", "my-account", "k8s-abc",
        LogicalClusterMetadata.KAFKA_LOGICAL_CLUSTER_TYPE, 104857600L, 1024L, 2048L,
        LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE_PER_BROKER.longValue(),
        LogicalClusterMetadata.DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE, null);
    Utils.updateLogicalClusterFile(lcValidMeta, tempFolder);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(lcValidMeta.logicalClusterId()) != null,
        TEST_MAX_WAIT_MS,
        "Expected metadata to be updated");
    assertEquals(ImmutableSet.of(lcValidMeta.logicalClusterId()), lcCache.logicalClusterIds());
    assertEquals("Expect no stale logical clusters anymore",
                 ImmutableSet.of(lcValidMeta.logicalClusterId()),
                 lcCache.logicalClusterIdsIncludingStale());
  }

  @Test
  public void testLoadAndRemoveMetadataCallsQuotaCallback()
      throws IOException, InterruptedException {
    TenantQuotaCallback quotaCallback = new TenantQuotaCallback();
    quotaCallback.configure(Collections.singletonMap("broker.id", "1"));
    Utils.createLogicalClusterFile(LC_META_ABC, tempFolder);

    lcCache.start();
    assertTrue("Expected cache to be initialized", lcCache.isUp());
    assertEquals(LC_META_ABC, lcCache.metadata(LC_META_ABC.logicalClusterId()));

    Map<String, String> tags = Collections.singletonMap("tenant", LC_META_ABC.logicalClusterId());
    // expect minimum bandwidth quotas because we did not simulate creating partitions
    assertEquals(TenantQuotaCallback.DEFAULT_MIN_BROKER_TENANT_PRODUCER_BYTE_RATE,
                 quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, tags), 0.001);
    assertEquals(TenantQuotaCallback.DEFAULT_MIN_BROKER_TENANT_CONSUMER_BYTE_RATE,
                 quotaCallback.quotaLimit(ClientQuotaType.FETCH, tags), 0.001);
    assertEquals(LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE_PER_BROKER,
                 quotaCallback.quotaLimit(ClientQuotaType.REQUEST, tags), 0.001);

    Thread.sleep(1000);

    Utils.deleteLogicalClusterFile(LC_META_ABC, tempFolder);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(LC_META_ABC.logicalClusterId()) == null,
        TEST_MAX_WAIT_MS,
        "Expected metadata to be removed from the cache");
    TestUtils.waitForCondition(
        () -> quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, tags) ==
              QuotaConfig.UNLIMITED_QUOTA.quota(ClientQuotaType.PRODUCE),
        TEST_MAX_WAIT_MS,
        "Expected unlimited quota for tenant with no quota");
  }

  @Test
  public void testHealthCheckAndKafkaTenant() throws IOException, InterruptedException {
    TenantQuotaCallback quotaCallback = new TenantQuotaCallback();
    quotaCallback.configure(Collections.singletonMap("broker.id", "1"));

    lcCache.start();
    assertTrue(lcCache.isUp());
    assertEquals(ImmutableSet.of(), lcCache.logicalClusterIds());

    // create new file and ensure cache gets updated
    Utils.createLogicalClusterFile(LC_META_XYZ, tempFolder);
    Utils.createLogicalClusterFile(LC_META_HEALTHCHECK, tempFolder);

    TestUtils.waitForCondition(
        () -> lcCache.metadata(LC_META_XYZ.logicalClusterId()) != null,
        TEST_MAX_WAIT_MS,
        "Expected metadata of new logical cluster to be present in metadata cache");
    TestUtils.waitForCondition(
        () -> lcCache.metadata(LC_META_HEALTHCHECK.logicalClusterId()) != null,
        TEST_MAX_WAIT_MS,
        "Expected healthcheck tenant metadata to be present in metadata cache");
    assertEquals(ImmutableSet.of(LC_META_XYZ.logicalClusterId(), LC_META_HEALTHCHECK.logicalClusterId()),
                 lcCache.logicalClusterIds());

    Map<String, String> tags =
        Collections.singletonMap("tenant", LC_META_HEALTHCHECK.logicalClusterId());
    // expect minimum bandwidth quotas because we did not simulate creating partitions
    assertEquals(
        TenantQuotaCallback.DEFAULT_MIN_BROKER_TENANT_PRODUCER_BYTE_RATE,
        quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, tags), 0.001);
    assertEquals(
        TenantQuotaCallback.DEFAULT_MIN_BROKER_TENANT_CONSUMER_BYTE_RATE,
        quotaCallback.quotaLimit(ClientQuotaType.FETCH, tags), 0.001);
    assertEquals(LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE_PER_BROKER,
                 quotaCallback.quotaLimit(ClientQuotaType.REQUEST, tags), 0.001);
  }

  /**
   * We do not expect any json files other than logical cluster metadata in the logical cluster
   * directory. We do not want to make an assumption about logical cluster ID, so other json
   * files will fail to load, but would mark them as "potential state logical cluster metadata"
   */
  @Test
  public void testShouldFailToLoadApiKeysAndHealthcheckFiles()
      throws IOException, InterruptedException {
    final String apikeysJson = "{\"keys\": {\"key1\": {" +
                               "\"user_id\": \"user1\"," +
                               "\"logical_cluster_id\": \"myCluster\"," +
                               "\"sasl_mechanism\": \"PLAIN\"," +
                               "\"hashed_secret\": \"no hash\"," +
                               "\"hash_function\": \"none\"" +
                               "}}}";
    Utils.updateJsonFile("apikeys.json", apikeysJson, false, tempFolder);

    final String hcJson = "{\"kafka_key\":\"Q4L43O\",\"kafka_secret\":\"J\",\"dd_api_key\":\"\"}";
    Utils.updateJsonFile("kafka-healthcheck-external.json", hcJson, false, tempFolder);

    lcCache.start();
    assertEquals(ImmutableSet.of(), lcCache.logicalClusterIds());
    assertEquals(ImmutableSet.of("apikeys", "kafka-healthcheck-external"),
                 lcCache.logicalClusterIdsIncludingStale());

    Utils.createLogicalClusterFile(LC_META_ABC, tempFolder);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(LC_META_ABC.logicalClusterId()) != null,
        TEST_MAX_WAIT_MS,
        "Expected new logical cluster to be added to the cache.");
    assertEquals(LC_META_ABC, lcCache.metadata(LC_META_ABC.logicalClusterId()));
    assertEquals(ImmutableSet.of(LC_META_ABC.logicalClusterId()), lcCache.logicalClusterIds());

    // writing the same content will still trigger file update event
    Utils.updateJsonFile("apikeys.json", apikeysJson, false, tempFolder);

    // since the file update happens async, remove the valid metadata file and hopefully that
    // update will happen after the previous update
    Utils.deleteLogicalClusterFile(LC_META_ABC, tempFolder);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(LC_META_ABC.logicalClusterId()) == null,
        TEST_MAX_WAIT_MS,
        "Expected metadata to be removed from the cache");
    assertEquals(ImmutableSet.of(), lcCache.logicalClusterIds());
  }

  @Test
  public void testShouldNotReturnDeletedLogicalClusters() throws IOException, InterruptedException {

    lcCache.start();

    // create two tenants, one is deleted
    Utils.createLogicalClusterFile(LC_META_ABC, tempFolder);
    Utils.createLogicalClusterFile(LC_META_DED, tempFolder);

    // Wait until the cache is updated. We are checking that the non-deleted one is in the cache
    // and the deleted one is not
    TestUtils.waitForCondition(
            () -> lcCache.metadata(LC_META_ABC.logicalClusterId()) != null &&
                  !lcCache.logicalClusterIds().contains(LC_META_DED.logicalClusterId()),
            TEST_MAX_WAIT_MS,
            "Expected new logical cluster to be added to the cache and the deleted to not be.");

    // it's possible that the previous 'waitForCondition' returned true while DED logical cluster
    // was not loaded yet, so we should wait with timeout in case DED cluster is still being
    // loaded (and not deactivated yet)
    TestUtils.waitForCondition(
        () -> !lcCache.logicalClusterIdsIncludingStale().contains(LC_META_DED.logicalClusterId()),
        TEST_MAX_WAIT_MS,
        "We expect that deactivated clusters will not be in cache, even as stale.");

    assertFalse("We expect that the deactivated cluster will be in process of getting deleted",
                lcCache.tenantLifecycleManager.deletedClusters().contains(LC_META_DED));
  }

  @Test
  public void testTenantQuotaUpdateUpdatesQuotaLimitAndUpdatesQuotaResetRequired()
      throws IOException, InterruptedException {
    final LogicalClusterMetadata lowIngressEgressTenant =
        new LogicalClusterMetadata(
            "lkc-leg", "pkc-a7sjfe", "my-poc-cluster", "my-account", "k8s-abc",
            LogicalClusterMetadata.KAFKA_LOGICAL_CLUSTER_TYPE,
            5242880000L, 5242880L, 5242880L, 50L,
            LogicalClusterMetadata.DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE,
            new LogicalClusterMetadata.LifecycleMetadata("my-poc-cluster", "pkc-a7sjfe", null, null));
    final LogicalClusterMetadata upgradedTenant =
        Utils.updateQuotas(lowIngressEgressTenant, 104857600L, 104857600L, 250L);

    // tenant
    Utils.createLogicalClusterFile(lowIngressEgressTenant, tempFolder);

    // cluster
    final int numBrokers = 8;
    TenantQuotaCallback quotaCallback = setupCallbackAndTenant(lowIngressEgressTenant.logicalClusterId(), numBrokers);

    // since quota update came from metadata update, quota reset flags should not be set
    assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE));
    assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.FETCH));
    assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.REQUEST));

    // start the broker
    lcCache.start();
    assertTrue("Expected cache to be initialized", lcCache.isUp());

    Map<String, String> tags = Collections.singletonMap("tenant", lowIngressEgressTenant.logicalClusterId());
    // we don't have special headroom handling anymore; if some tenant sets <= 5MB/s ingress &
    // egress, they are still going to have 0 headroom
    assertEquals(5242880.0 / numBrokers,
                 quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, tags), 0.001);
    assertEquals(5242880.0 / numBrokers,
                 quotaCallback.quotaLimit(ClientQuotaType.FETCH, tags), 0.001);
    assertEquals(50.0, quotaCallback.quotaLimit(ClientQuotaType.REQUEST, tags), 0.001);

    // since we loaded tenant metadata, quota changed from default (unlimited) to tenant's quotas
    // verify that TenantQuotaCallback#quotaResetRequired returns true for all types of quotas
    assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE));
    assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.FETCH));
    assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.REQUEST));

    // upgrade tenant to 100MB/s quotas
    Utils.updateLogicalClusterFile(upgradedTenant, tempFolder);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(upgradedTenant.logicalClusterId()).producerByteRate() == 104857600L,
        TEST_MAX_WAIT_MS,
        "Expected metadata to be updated");
    assertEquals(104857600L / numBrokers,
                 quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, tags), 0.001);
    assertEquals(104857600L / numBrokers,
                 quotaCallback.quotaLimit(ClientQuotaType.FETCH, tags), 0.001);
    assertEquals(250.0, quotaCallback.quotaLimit(ClientQuotaType.REQUEST, tags), 0.001);

    // verify that TenantQuotaCallback#quotaResetRequired returns true for all types of quotas,
    // which will ensure that next produce/consume will pick up new quotas for throttling
    assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE));
    assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.FETCH));
    assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.REQUEST));
  }

  private TenantQuotaCallback setupCallbackAndTenant(String lcId, int numBrokers) {
    final int brokerId = 1;

    TenantQuotaCallback quotaCallback = new TenantQuotaCallback();
    quotaCallback.configure(Collections.singletonMap("broker.id", String.valueOf(brokerId)));
    TestCluster testCluster = new TestCluster();
    for (int i = 1; i <= numBrokers; i++) {
      testCluster.addNode(i, "rack0");
    }
    Cluster cluster = testCluster.cluster();
    quotaCallback.updateClusterMetadata(cluster);

    // partitions
    final String topic = lcId + "_topic1";
    for (int i = 1; i <= numBrokers; i++) {
      testCluster.setPartitionLeaders(topic, i, 1, i);
    }
    quotaCallback.updateClusterMetadata(testCluster.cluster());
    return quotaCallback;
  }


  private String fullchain1 = "-----BEGIN CERTIFICATE-----\n" +
          "MIIFfDCCBGSgAwIBAgISBFmAQQIS/v9qbmrDF5BxHGeWMA0GCSqGSIb3DQEBCwUA\n" +
          "MEoxCzAJBgNVBAYTAlVTMRYwFAYDVQQKEw1MZXQncyBFbmNyeXB0MSMwIQYDVQQD\n" +
          "ExpMZXQncyBFbmNyeXB0IEF1dGhvcml0eSBYMzAeFw0xOTAzMDIwNDMwNDhaFw0x\n" +
          "OTA1MzEwNDMwNDhaMC0xKzApBgNVBAMMIioudXMtY2VudHJhbDEuZ2NwLnByaXYu\n" +
          "Y3BkZXYuY2xvdWQwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQD7fcdS\n" +
          "HYnMBb7zL2Nez+Mfy0g2FEBAT+duDDPGlP9AVyw3D7mZxIhaIy/kz3oWguXrtvZA\n" +
          "bvmLoTvnYm4suUD/iWpfr5OBOunnzSb772TbCTMjVrrVjjaKR+hHMscGMeV7O8yv\n" +
          "G3urz8KA7ms66COIXWFE1cKTIfhTqoIj0sgQ0J+WuLHvqrn5V0F5P0oZ2rRBMcUS\n" +
          "mO2/FGUgGMs/Tnh+6DdE1GuBeQxztLRMWTlKWKY6zKd5H305ef1uvGhMgDkBnR97\n" +
          "0nFi6rjvbL+OXfQuYXf/rhR1MtjHTNprmGBVhqHci3oZdE1vFvnxUQ98nY/ibOgZ\n" +
          "WrEXuXc9QN5MgZeJAgMBAAGjggJ3MIICczAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0l\n" +
          "BBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHQYDVR0OBBYE\n" +
          "FL+UaLPv4/rIx38v2b2kw1YvJIOwMB8GA1UdIwQYMBaAFKhKamMEfd265tE5t6ZF\n" +
          "Ze/zqOyhMG8GCCsGAQUFBwEBBGMwYTAuBggrBgEFBQcwAYYiaHR0cDovL29jc3Au\n" +
          "aW50LXgzLmxldHNlbmNyeXB0Lm9yZzAvBggrBgEFBQcwAoYjaHR0cDovL2NlcnQu\n" +
          "aW50LXgzLmxldHNlbmNyeXB0Lm9yZy8wLQYDVR0RBCYwJIIiKi51cy1jZW50cmFs\n" +
          "MS5nY3AucHJpdi5jcGRldi5jbG91ZDBMBgNVHSAERTBDMAgGBmeBDAECATA3Bgsr\n" +
          "BgEEAYLfEwEBATAoMCYGCCsGAQUFBwIBFhpodHRwOi8vY3BzLmxldHNlbmNyeXB0\n" +
          "Lm9yZzCCAQQGCisGAQQB1nkCBAIEgfUEgfIA8AB2AHR+2oMxrTMQkSGcziVPQnDC\n" +
          "v/1eQiAIxjc1eeYQe8xWAAABaTziKRUAAAQDAEcwRQIhAJ8fxSwNiqLbtz/pCCPY\n" +
          "tA+yyTJ5jhwbxIxeDg7x1q6FAiA3ClZpy5EVG/ng997wyZjWW8n6dH9Owee8wjVV\n" +
          "OXK1VAB2ACk8UZZUyDlluqpQ/FgH1Ldvv1h6KXLcpMMM9OVFR/R4AAABaTziKSoA\n" +
          "AAQDAEcwRQIhAMPvT2P/PY6xux8Jf7T8ZhiluaSa8PUQDXqJNrd3I5rMAiBiaaAX\n" +
          "JsWP2RZrUXGLECy8s5L8kUPD5ZpBmFCaxUnl6zANBgkqhkiG9w0BAQsFAAOCAQEA\n" +
          "MChPeoJ15MwxFuVc2FuHFGyBWuKrOSMcQ+1l8T86IOV5xh8grnZO55hlO2jbfXkF\n" +
          "sWQpSMyfi4QRyX3U5o9R4HzsnC2zmFoPZ6SkYM/SUJX6qY0asW5+WmQk920EXuuZ\n" +
          "aP1lGAIbZXOI4OmJDBeVYzSFQOh8o5Mbsa9geGxUSLQoRf1KAjGIbsFlGwe+/7gN\n" +
          "zpmPfciV+hwM9QGO3BXOV3MWdi8UvHppr9YpjuPbCNnVlm6Cqq8KxoKSa/DS1MrK\n" +
          "1YMkFSGZJoDJFi5AAhBgUW1i5KEi1bfwleDQfJtoGcXH/0CpdnVfMWdYejHEyn2b\n" +
          "IjD2mMLVhq9uEr1O6l9C5g==\n" +
          "-----END CERTIFICATE-----\n" +
          "\n" +
          "-----BEGIN CERTIFICATE-----\n" +
          "MIIEkjCCA3qgAwIBAgIQCgFBQgAAAVOFc2oLheynCDANBgkqhkiG9w0BAQsFADA/\n" +
          "MSQwIgYDVQQKExtEaWdpdGFsIFNpZ25hdHVyZSBUcnVzdCBDby4xFzAVBgNVBAMT\n" +
          "DkRTVCBSb290IENBIFgzMB4XDTE2MDMxNzE2NDA0NloXDTIxMDMxNzE2NDA0Nlow\n" +
          "SjELMAkGA1UEBhMCVVMxFjAUBgNVBAoTDUxldCdzIEVuY3J5cHQxIzAhBgNVBAMT\n" +
          "GkxldCdzIEVuY3J5cHQgQXV0aG9yaXR5IFgzMIIBIjANBgkqhkiG9w0BAQEFAAOC\n" +
          "AQ8AMIIBCgKCAQEAnNMM8FrlLke3cl03g7NoYzDq1zUmGSXhvb418XCSL7e4S0EF\n" +
          "q6meNQhY7LEqxGiHC6PjdeTm86dicbp5gWAf15Gan/PQeGdxyGkOlZHP/uaZ6WA8\n" +
          "SMx+yk13EiSdRxta67nsHjcAHJyse6cF6s5K671B5TaYucv9bTyWaN8jKkKQDIZ0\n" +
          "Z8h/pZq4UmEUEz9l6YKHy9v6Dlb2honzhT+Xhq+w3Brvaw2VFn3EK6BlspkENnWA\n" +
          "a6xK8xuQSXgvopZPKiAlKQTGdMDQMc2PMTiVFrqoM7hD8bEfwzB/onkxEz0tNvjj\n" +
          "/PIzark5McWvxI0NHWQWM6r6hCm21AvA2H3DkwIDAQABo4IBfTCCAXkwEgYDVR0T\n" +
          "AQH/BAgwBgEB/wIBADAOBgNVHQ8BAf8EBAMCAYYwfwYIKwYBBQUHAQEEczBxMDIG\n" +
          "CCsGAQUFBzABhiZodHRwOi8vaXNyZy50cnVzdGlkLm9jc3AuaWRlbnRydXN0LmNv\n" +
          "bTA7BggrBgEFBQcwAoYvaHR0cDovL2FwcHMuaWRlbnRydXN0LmNvbS9yb290cy9k\n" +
          "c3Ryb290Y2F4My5wN2MwHwYDVR0jBBgwFoAUxKexpHsscfrb4UuQdf/EFWCFiRAw\n" +
          "VAYDVR0gBE0wSzAIBgZngQwBAgEwPwYLKwYBBAGC3xMBAQEwMDAuBggrBgEFBQcC\n" +
          "ARYiaHR0cDovL2Nwcy5yb290LXgxLmxldHNlbmNyeXB0Lm9yZzA8BgNVHR8ENTAz\n" +
          "MDGgL6AthitodHRwOi8vY3JsLmlkZW50cnVzdC5jb20vRFNUUk9PVENBWDNDUkwu\n" +
          "Y3JsMB0GA1UdDgQWBBSoSmpjBH3duubRObemRWXv86jsoTANBgkqhkiG9w0BAQsF\n" +
          "AAOCAQEA3TPXEfNjWDjdGBX7CVW+dla5cEilaUcne8IkCJLxWh9KEik3JHRRHGJo\n" +
          "uM2VcGfl96S8TihRzZvoroed6ti6WqEBmtzw3Wodatg+VyOeph4EYpr/1wXKtx8/\n" +
          "wApIvJSwtmVi4MFU5aMqrSDE6ea73Mj2tcMyo5jMd6jmeWUHK8so/joWUoHOUgwu\n" +
          "X4Po1QYz+3dszkDqMp4fklxBwXRsW10KXzPMTZ+sOPAveyxindmjkW8lGy+QsRlG\n" +
          "PfZ+G6Z6h7mjem0Y+iWlkYcV4PIWL1iwBi8saCbGS5jN2p8M+X+Q7UNKEkROb3N6\n" +
          "KOqkqm57TH2H3eDJAkSnh6/DNFu0Qg==\n" +
          "-----END CERTIFICATE-----\n";

  private String privkey1 = "-----BEGIN RSA PRIVATE KEY-----\n" +
          "MIIEowIBAAKCAQEA+33HUh2JzAW+8y9jXs/jH8tINhRAQE/nbgwzxpT/QFcsNw+5\n" +
          "mcSIWiMv5M96FoLl67b2QG75i6E752JuLLlA/4lqX6+TgTrp580m++9k2wkzI1a6\n" +
          "1Y42ikfoRzLHBjHlezvMrxt7q8/CgO5rOugjiF1hRNXCkyH4U6qCI9LIENCflrix\n" +
          "76q5+VdBeT9KGdq0QTHFEpjtvxRlIBjLP054fug3RNRrgXkMc7S0TFk5SlimOsyn\n" +
          "eR99OXn9brxoTIA5AZ0fe9JxYuq472y/jl30LmF3/64UdTLYx0zaa5hgVYah3It6\n" +
          "GXRNbxb58VEPfJ2P4mzoGVqxF7l3PUDeTIGXiQIDAQABAoIBAH8DkEY1quGCyWSy\n" +
          "u0IoRjJJjafaZHTWpjCbMw8JMz0AidEpPPifHKpBeS/bZXK3G34HwqjaI2hUvxdm\n" +
          "S/SEf4JPmYzH9Pxgj7/FifnVdx90rwIbDHNMxtjh5jsHNyM20gqCMicB/1zPqhFJ\n" +
          "2JhAo6l8V+LW/tUmY++FfwKusuJiKtq8gTAhaf+qr9ARnNwRQ0hq+BwEr6XD9ful\n" +
          "rfLEIl3ecvaKXjj6b1inrG2yVN3coibNuDkLXY2EgJG64n1vTu6aBMj0P6HeiXiH\n" +
          "m04OrTP43nQrKV/+7IzrBlqEMzHtj09OgWvXkV6NhIEWswkkY+HkzLj9hgoDdV4Z\n" +
          "FmmmbV0CgYEA/O6cPK4K+ElrGvyJRlkRuD3WzZByaqJyeR6Jdm3qNCQ/9bI5PH/X\n" +
          "gpPqHaFAA0RVlUiV4dZ5BksxW6Lvokbv/i1xlAs8TFobdr5+qcO518e5C8V+CCut\n" +
          "y01LOj0I9ZvFdAHJP514QgHLHhG8/Ua5/kgFWtnZWi4mhak6T04NOk8CgYEA/oqx\n" +
          "zzHHhpaWS4tfQEL0Ibxx1VKT8M4R3QkOnngLJonA1lfqSujIeodLZWm6uzcy30u7\n" +
          "ikCuoKCxK0vE8dOltopwTtlTOuYI2JfdvmFaanOG/857VoFIQZmI8bakaQPr4PXe\n" +
          "ffDZ22k4n4B7k05fZ1MqaCHnZPI1niamfGkJEqcCgYEA0G35BfAOTiiCQIzWusfv\n" +
          "WDptZpygDMutNa46bQOKukkdA+VIUViwSYSGqsAUthx7wjc8fAx3Uv5nwDH2820t\n" +
          "m/Hq5KqVl/2xIBs+2brWzMBi9xZaE3WbFCuv0GA3n94ryrsmEmw7i3la3n6TlMvR\n" +
          "vX+wGfvnpu7dA8w+pteVAvUCgYAO//FWemJ9peYZcY8dZFSqoEY9Ae7B5ALdeako\n" +
          "4X4WuUtp1ihyXaFixxJEWaStX6VZz0av8PvZb17BZGeosIY1aZcQrnHfKKsgyGJC\n" +
          "083WNBSignJ2OIwfgYK2a8LohVijGxoPZeAQs/SoQZQGrDmnBxmapVTTeAp81V4+\n" +
          "OppURQKBgFBqIIXH06SH4FVxTbo+cq6gOHuRIXyLR++AyzJyn8wVfHVJ01r6zhGd\n" +
          "azTCm+fyfw/6jsJfK3b8mLjiz39GAM7vyvkVV5FKYC2wQldxPyow/MhmKBjdqp2p\n" +
          "5gq8MSFaXoZOtRgMWdT4JPLTi7xp8429yRM8JK8E6A9+5aTkUM7H\n" +
          "-----END RSA PRIVATE KEY-----\n";

  private String fullchain2 = "-----BEGIN CERTIFICATE-----\n" +
          "MIIFfDCCBGSgAwIBAgISA+SIWPXUZozkvzhi5xKWfne+MA0GCSqGSIb3DQEBCwUA\n" +
          "MEoxCzAJBgNVBAYTAlVTMRYwFAYDVQQKEw1MZXQncyBFbmNyeXB0MSMwIQYDVQQD\n" +
          "ExpMZXQncyBFbmNyeXB0IEF1dGhvcml0eSBYMzAeFw0xOTA1MTcwMzU5MTdaFw0x\n" +
          "OTA4MTUwMzU5MTdaMC0xKzApBgNVBAMMIioudXMtY2VudHJhbDEuZ2NwLnByaXYu\n" +
          "Y3BkZXYuY2xvdWQwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQD7fcdS\n" +
          "HYnMBb7zL2Nez+Mfy0g2FEBAT+duDDPGlP9AVyw3D7mZxIhaIy/kz3oWguXrtvZA\n" +
          "bvmLoTvnYm4suUD/iWpfr5OBOunnzSb772TbCTMjVrrVjjaKR+hHMscGMeV7O8yv\n" +
          "G3urz8KA7ms66COIXWFE1cKTIfhTqoIj0sgQ0J+WuLHvqrn5V0F5P0oZ2rRBMcUS\n" +
          "mO2/FGUgGMs/Tnh+6DdE1GuBeQxztLRMWTlKWKY6zKd5H305ef1uvGhMgDkBnR97\n" +
          "0nFi6rjvbL+OXfQuYXf/rhR1MtjHTNprmGBVhqHci3oZdE1vFvnxUQ98nY/ibOgZ\n" +
          "WrEXuXc9QN5MgZeJAgMBAAGjggJ3MIICczAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0l\n" +
          "BBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHQYDVR0OBBYE\n" +
          "FL+UaLPv4/rIx38v2b2kw1YvJIOwMB8GA1UdIwQYMBaAFKhKamMEfd265tE5t6ZF\n" +
          "Ze/zqOyhMG8GCCsGAQUFBwEBBGMwYTAuBggrBgEFBQcwAYYiaHR0cDovL29jc3Au\n" +
          "aW50LXgzLmxldHNlbmNyeXB0Lm9yZzAvBggrBgEFBQcwAoYjaHR0cDovL2NlcnQu\n" +
          "aW50LXgzLmxldHNlbmNyeXB0Lm9yZy8wLQYDVR0RBCYwJIIiKi51cy1jZW50cmFs\n" +
          "MS5nY3AucHJpdi5jcGRldi5jbG91ZDBMBgNVHSAERTBDMAgGBmeBDAECATA3Bgsr\n" +
          "BgEEAYLfEwEBATAoMCYGCCsGAQUFBwIBFhpodHRwOi8vY3BzLmxldHNlbmNyeXB0\n" +
          "Lm9yZzCCAQQGCisGAQQB1nkCBAIEgfUEgfIA8AB3AHR+2oMxrTMQkSGcziVPQnDC\n" +
          "v/1eQiAIxjc1eeYQe8xWAAABasQonMoAAAQDAEgwRgIhAJCoN9WGHlqLCDD6cBg2\n" +
          "QLlwETH+I8J0n/k8NYnrBpGTAiEAvjwyafxyws7p6Ay2NosLJ9elSvqMepPfuGIO\n" +
          "G7wis9gAdQBj8tvN6DvMLM8LcoQnV2szpI1hd4+9daY4scdoVEvYjQAAAWrEKJzr\n" +
          "AAAEAwBGMEQCIE397fFPAFeDRaI7ByAE/hqwhQKeTf8sPc4nKB6f98QLAiAon1kz\n" +
          "O99aKvICl8N7z63Y5rIAqV1jEde2Ie58KPzVgDANBgkqhkiG9w0BAQsFAAOCAQEA\n" +
          "KQdMKH9f2twvcVcYX+nkyzwhc11sf8n5dt100DHnnU0sD3R6LvaYpGqy6F/52rMl\n" +
          "DFC/Lj98Xp+aATEGv31CYfZBdd8yxJ1XKs3xm0avjhPW+amWnNz5T9MENCvTss6x\n" +
          "hZKc18Xwj8ZQH8zw9+xTp5wi1x6kYyIfL6s2L76ogf3FgrqksVh8mHGkJbYs6hdj\n" +
          "d4NbXhTwyVrimVLaFRX1ijULG2YX/E9uQXLqMorl0P+Sy+XywR5X+Y9I/You0wko\n" +
          "YLMT+6MCieNjV2wR97Q+J2G8Hfg4gavUgd/SGiPvBtrmx51SANNzjA6GJzODfbjm\n" +
          "Q6UBvzOlbr3oBx5+/yjtkg==\n" +
          "-----END CERTIFICATE-----\n" +
          "\n" +
          "-----BEGIN CERTIFICATE-----\n" +
          "MIIEkjCCA3qgAwIBAgIQCgFBQgAAAVOFc2oLheynCDANBgkqhkiG9w0BAQsFADA/\n" +
          "MSQwIgYDVQQKExtEaWdpdGFsIFNpZ25hdHVyZSBUcnVzdCBDby4xFzAVBgNVBAMT\n" +
          "DkRTVCBSb290IENBIFgzMB4XDTE2MDMxNzE2NDA0NloXDTIxMDMxNzE2NDA0Nlow\n" +
          "SjELMAkGA1UEBhMCVVMxFjAUBgNVBAoTDUxldCdzIEVuY3J5cHQxIzAhBgNVBAMT\n" +
          "GkxldCdzIEVuY3J5cHQgQXV0aG9yaXR5IFgzMIIBIjANBgkqhkiG9w0BAQEFAAOC\n" +
          "AQ8AMIIBCgKCAQEAnNMM8FrlLke3cl03g7NoYzDq1zUmGSXhvb418XCSL7e4S0EF\n" +
          "q6meNQhY7LEqxGiHC6PjdeTm86dicbp5gWAf15Gan/PQeGdxyGkOlZHP/uaZ6WA8\n" +
          "SMx+yk13EiSdRxta67nsHjcAHJyse6cF6s5K671B5TaYucv9bTyWaN8jKkKQDIZ0\n" +
          "Z8h/pZq4UmEUEz9l6YKHy9v6Dlb2honzhT+Xhq+w3Brvaw2VFn3EK6BlspkENnWA\n" +
          "a6xK8xuQSXgvopZPKiAlKQTGdMDQMc2PMTiVFrqoM7hD8bEfwzB/onkxEz0tNvjj\n" +
          "/PIzark5McWvxI0NHWQWM6r6hCm21AvA2H3DkwIDAQABo4IBfTCCAXkwEgYDVR0T\n" +
          "AQH/BAgwBgEB/wIBADAOBgNVHQ8BAf8EBAMCAYYwfwYIKwYBBQUHAQEEczBxMDIG\n" +
          "CCsGAQUFBzABhiZodHRwOi8vaXNyZy50cnVzdGlkLm9jc3AuaWRlbnRydXN0LmNv\n" +
          "bTA7BggrBgEFBQcwAoYvaHR0cDovL2FwcHMuaWRlbnRydXN0LmNvbS9yb290cy9k\n" +
          "c3Ryb290Y2F4My5wN2MwHwYDVR0jBBgwFoAUxKexpHsscfrb4UuQdf/EFWCFiRAw\n" +
          "VAYDVR0gBE0wSzAIBgZngQwBAgEwPwYLKwYBBAGC3xMBAQEwMDAuBggrBgEFBQcC\n" +
          "ARYiaHR0cDovL2Nwcy5yb290LXgxLmxldHNlbmNyeXB0Lm9yZzA8BgNVHR8ENTAz\n" +
          "MDGgL6AthitodHRwOi8vY3JsLmlkZW50cnVzdC5jb20vRFNUUk9PVENBWDNDUkwu\n" +
          "Y3JsMB0GA1UdDgQWBBSoSmpjBH3duubRObemRWXv86jsoTANBgkqhkiG9w0BAQsF\n" +
          "AAOCAQEA3TPXEfNjWDjdGBX7CVW+dla5cEilaUcne8IkCJLxWh9KEik3JHRRHGJo\n" +
          "uM2VcGfl96S8TihRzZvoroed6ti6WqEBmtzw3Wodatg+VyOeph4EYpr/1wXKtx8/\n" +
          "wApIvJSwtmVi4MFU5aMqrSDE6ea73Mj2tcMyo5jMd6jmeWUHK8so/joWUoHOUgwu\n" +
          "X4Po1QYz+3dszkDqMp4fklxBwXRsW10KXzPMTZ+sOPAveyxindmjkW8lGy+QsRlG\n" +
          "PfZ+G6Z6h7mjem0Y+iWlkYcV4PIWL1iwBi8saCbGS5jN2p8M+X+Q7UNKEkROb3N6\n" +
          "KOqkqm57TH2H3eDJAkSnh6/DNFu0Qg==\n" +
          "-----END CERTIFICATE-----";

  private String privkey2 = "-----BEGIN RSA PRIVATE KEY-----\n" +
          "MIIEowIBAAKCAQEA+33HUh2JzAW+8y9jXs/jH8tINhRAQE/nbgwzxpT/QFcsNw+5\n" +
          "mcSIWiMv5M96FoLl67b2QG75i6E752JuLLlA/4lqX6+TgTrp580m++9k2wkzI1a6\n" +
          "1Y42ikfoRzLHBjHlezvMrxt7q8/CgO5rOugjiF1hRNXCkyH4U6qCI9LIENCflrix\n" +
          "76q5+VdBeT9KGdq0QTHFEpjtvxRlIBjLP054fug3RNRrgXkMc7S0TFk5SlimOsyn\n" +
          "eR99OXn9brxoTIA5AZ0fe9JxYuq472y/jl30LmF3/64UdTLYx0zaa5hgVYah3It6\n" +
          "GXRNbxb58VEPfJ2P4mzoGVqxF7l3PUDeTIGXiQIDAQABAoIBAH8DkEY1quGCyWSy\n" +
          "u0IoRjJJjafaZHTWpjCbMw8JMz0AidEpPPifHKpBeS/bZXK3G34HwqjaI2hUvxdm\n" +
          "S/SEf4JPmYzH9Pxgj7/FifnVdx90rwIbDHNMxtjh5jsHNyM20gqCMicB/1zPqhFJ\n" +
          "2JhAo6l8V+LW/tUmY++FfwKusuJiKtq8gTAhaf+qr9ARnNwRQ0hq+BwEr6XD9ful\n" +
          "rfLEIl3ecvaKXjj6b1inrG2yVN3coibNuDkLXY2EgJG64n1vTu6aBMj0P6HeiXiH\n" +
          "m04OrTP43nQrKV/+7IzrBlqEMzHtj09OgWvXkV6NhIEWswkkY+HkzLj9hgoDdV4Z\n" +
          "FmmmbV0CgYEA/O6cPK4K+ElrGvyJRlkRuD3WzZByaqJyeR6Jdm3qNCQ/9bI5PH/X\n" +
          "gpPqHaFAA0RVlUiV4dZ5BksxW6Lvokbv/i1xlAs8TFobdr5+qcO518e5C8V+CCut\n" +
          "y01LOj0I9ZvFdAHJP514QgHLHhG8/Ua5/kgFWtnZWi4mhak6T04NOk8CgYEA/oqx\n" +
          "zzHHhpaWS4tfQEL0Ibxx1VKT8M4R3QkOnngLJonA1lfqSujIeodLZWm6uzcy30u7\n" +
          "ikCuoKCxK0vE8dOltopwTtlTOuYI2JfdvmFaanOG/857VoFIQZmI8bakaQPr4PXe\n" +
          "ffDZ22k4n4B7k05fZ1MqaCHnZPI1niamfGkJEqcCgYEA0G35BfAOTiiCQIzWusfv\n" +
          "WDptZpygDMutNa46bQOKukkdA+VIUViwSYSGqsAUthx7wjc8fAx3Uv5nwDH2820t\n" +
          "m/Hq5KqVl/2xIBs+2brWzMBi9xZaE3WbFCuv0GA3n94ryrsmEmw7i3la3n6TlMvR\n" +
          "vX+wGfvnpu7dA8w+pteVAvUCgYAO//FWemJ9peYZcY8dZFSqoEY9Ae7B5ALdeako\n" +
          "4X4WuUtp1ihyXaFixxJEWaStX6VZz0av8PvZb17BZGeosIY1aZcQrnHfKKsgyGJC\n" +
          "083WNBSignJ2OIwfgYK2a8LohVijGxoPZeAQs/SoQZQGrDmnBxmapVTTeAp81V4+\n" +
          "OppURQKBgFBqIIXH06SH4FVxTbo+cq6gOHuRIXyLR++AyzJyn8wVfHVJ01r6zhGd\n" +
          "azTCm+fyfw/6jsJfK3b8mLjiz39GAM7vyvkVV5FKYC2wQldxPyow/MhmKBjdqp2p\n" +
          "5gq8MSFaXoZOtRgMWdT4JPLTi7xp8429yRM8JK8E6A9+5aTkUM7H\n" +
          "-----END RSA PRIVATE KEY-----\n";

  private String fullchain3 = "-----BEGIN CERTIFICATE-----\n" +
          "MIIFfDCCBGSgAwIBAgISA+SIWPXUZozkvzhi5xKWfne+MA0GCSqGSIb3DQEBCwUA\n" +
          "MEoxCzAJBgNVBAYTAlVTMRYwFAYDVQQKEw1MZXQncyBFbmNyeXB0MSMwIQYDVQQD\n" +
          "ExpMZXQncyBFbmNyeXB0IEF1dGhvcml0eSBYMzAeFw0xOTA1MTcwMzU5MTdaFw0x\n" +
          "OTA4MTUwMzU5MTdaMC0xKzApBgNVBAMMIioudXMtY2VudHJhbDEuZ2NwLnByaXYu\n" +
          "Y3BkZXYuY2xvdWQwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQD7fcdS\n" +
          "HYnMBb7zL2Nez+Mfy0g2FEBAT+duDDPGlP9AVyw3D7mZxIhaIy/kz3oWguXrtvZA\n" +
          "bvmLoTvnYm4suUD/iWpfr5OBOunnzSb772TbCTMjVrrVjjaKR+hHMscGMeV7O8yv\n" +
          "G3urz8KA7ms66COIXWFE1cKTIfhTqoIj0sgQ0J+WuLHvqrn5V0F5P0oZ2rRBMcUS\n" +
          "mO2/FGUgGMs/Tnh+6DdE1GuBeQxztLRMWTlKWKY6zKd5H305ef1uvGhMgDkBnR97\n" +
          "0nFi6rjvbL+OXfQuYXf/rhR1MtjHTNprmGBVhqHci3oZdE1vFvnxUQ98nY/ibOgZ\n" +
          "WrEXuXc9QN5MgZeJAgMBAAGjggJ3MIICczAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0l\n" +
          "BBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHQYDVR0OBBYE\n" +
          "FL+UaLPv4/rIx38v2b2kw1YvJIOwMB8GA1UdIwQYMBaAFKhKamMEfd265tE5t6ZF\n" +
          "Ze/zqOyhMG8GCCsGAQUFBwEBBGMwYTAuBggrBgEFBQcwAYYiaHR0cDovL29jc3Au\n" +
          "aW50LXgzLmxldHNlbmNyeXB0Lm9yZzAvBggrBgEFBQcwAoYjaHR0cDovL2NlcnQu\n" +
          "aW50LXgzLmxldHNlbmNyeXB0Lm9yZy8wLQYDVR0RBCYwJIIiKi51cy1jZW50cmFs\n" +
          "MS5nY3AucHJpdi5jcGRldi5jbG91ZDBMBgNVHSAERTBDMAgGBmeBDAECATA3Bgsr\n" +
          "BgEEAYLfEwEBATAoMCYGCCsGAQUFBwIBFhpodHRwOi8vY3BzLmxldHNlbmNyeXB0\n" +
          "Lm9yZzCCAQQGCisGAQQB1nkCBAIEgfUEgfIA8AB3AHR+2oMxrTMQkSGcziVPQnDC\n" +
          "v/1eQiAIxjc1eeYQe8xWAAABasQonMoAAAQDAEgwRgIhAJCoN9WGHlqLCDD6cBg2\n" +
          "QLlwETH+I8J0n/k8NYnrBpGTAiEAvjwyafxyws7p6Ay2NosLJ9elSvqMepPfuGIO\n" +
          "G7wis9gAdQBj8tvN6DvMLM8LcoQnV2szpI1hd4+9daY4scdoVEvYjQAAAWrEKJzr\n" +
          "AAAEAwBGMEQCIE397fFPAFeDRaI7ByAE/hqwhQKeTf8sPc4nKB6f98QLAiAon1kz\n" +
          "O99aKvICl8N7z63Y5rIAqV1jEde2Ie58KPzVgDANBgkqhkiG9w0BAQsFAAOCAQEA\n" +
          "KQdMKH9f2twvcVcYX+nkyzwhc11sf8n5dt100DHnnU0sD3R6LvaYpGqy6F/52rMl\n" +
          "DFC/Lj98Xp+aATEGv31CYfZBdd8yxJ1XKs3xm0avjhPW+amWnNz5T9MENCvTss6x\n" +
          "hZKc18Xwj8ZQH8zw9+xTp5wi1x6kYyIfL6s2L76ogf3FgrqksVh8mHGkJbYs6hdj\n" +
          "d4NbXhTwyVrimVLaFRX1ijULG2YX/E9uQXLqMorl0P+Sy+XywR5X+Y9I/You0wko\n" +
          "YLMT+6MCieNjV2wR97Q+J2G8Hfg4gavUgd/SGiPvBtrmx51SANNzjA6GJzODfbjm\n" +
          "Q6UBvzOlbr3oBx5+/yjtkg==\n" +
          "-----END CERTIFICATE-----\n" +
          "\n" +
          "-----BEGIN CERTIFICATE-----\n" +
          "MIIEkjCCA3qgAwIBAgIQCgFBQgAAAVOFc2oLheynCDANBgkqhkiG9w0BAQsFADA/\n" +
          "MSQwIgYDVQQKExtEaWdpdGFsIFNpZ25hdHVyZSBUcnVzdCBDby4xFzAVBgNVBAMT\n" +
          "DkRTVCBSb290IENBIFgzMB4XDTE2MDMxNzE2NDA0NloXDTIxMDMxNzE2NDA0Nlow\n" +
          "SjELMAkGA1UEBhMCVVMxFjAUBgNVBAoTDUxldCdzIEVuY3J5cHQxIzAhBgNVBAMT\n" +
          "GkxldCdzIEVuY3J5cHQgQXV0aG9yaXR5IFgzMIIBIjANBgkqhkiG9w0BAQEFAAOC\n" +
          "AQ8AMIIBCgKCAQEAnNMM8FrlLke3cl03g7NoYzDq1zUmGSXhvb418XCSL7e4S0EF\n" +
          "q6meNQhY7LEqxGiHC6PjdeTm86dicbp5gWAf15Gan/PQeGdxyGkOlZHP/uaZ6WA8\n" +
          "SMx+yk13EiSdRxta67nsHjcAHJyse6cF6s5K671B5TaYucv9bTyWaN8jKkKQDIZ0\n" +
          "Z8h/pZq4UmEUEz9l6YKHy9v6Dlb2honzhT+Xhq+w3Brvaw2VFn3EK6BlspkENnWA\n" +
          "a6xK8xuQSXgvopZPKiAlKQTGdMDQMc2PMTiVFrqoM7hD8bEfwzB/onkxEz0tNvjj\n" +
          "/PIzark5McWvxI0NHWQWM6r6hCm21AvA2H3DkwIDAQABo4IBfTCCAXkwEgYDVR0T\n" +
          "AQH/BAgwBgEB/wIBADAOBgNVHQ8BAf8EBAMCAYYwfwYIKwYBBQUHAQEEczBxMDIG\n" +
          "CCsGAQUFBzABhiZodHRwOi8vaXNyZy50cnVzdGlkLm9jc3AuaWRlbnRydXN0LmNv\n" +
          "bTA7BggrBgEFBQcwAoYvaHR0cDovL2FwcHMuaWRlbnRydXN0LmNvbS9yb290cy9k\n" +
          "c3Ryb290Y2F4My5wN2MwHwYDVR0jBBgwFoAUxKexpHsscfrb4UuQdf/EFWCFiRAw\n" +
          "VAYDVR0gBE0wSzAIBgZngQwBAgEwPwYLKwYBBAGC3xMBAQEwMDAuBggrBgEFBQcC\n" +
          "ARYiaHR0cDovL2Nwcy5yb290LXgxLmxldHNlbmNyeXB0Lm9yZzA8BgNVHR8ENTAz\n" +
          "MDGgL6AthitodHRwOi8vY3JsLmlkZW50cnVzdC5jb20vRFNUUk9PVENBWDNDUkwu\n" +
          "Y3JsMB0GA1UdDgQWBBSoSmpjBH3duubRObemRWXv86jsoTANBgkqhkiG9w0BAQsF\n" +
          "AAOCAQEA3TPXEfNjWDjdGBX7CVW+dla5cEilaUcne8IkCJLxWh9KEik3JHRRHGJo\n" +
          "uM2VcGfl96S8TihRzZvoroed6ti6WqEBmtzw3Wodatg+VyOeph4EYpr/1wXKtx8/\n" +
          "wApIvJSwtmVi4MFU5aMqrSDE6ea73Mj2tcMyo5jMd6jmeWUHK8so/joWUoHOUgwu\n" +
          "X4Po1QYz+3dszkDqMp4fklxBwXRsW10KXzPMTZ+sOPAveyxindmjkW8lGy+QsRlG\n" +
          "PfZ+G6Z6h7mjem0Y+iWlkYcV4PIWL1iwBi8saCbGS5jN2p8M+X+Q7UNKEkROb3N6\n" +
          "KOqkqm57TH2H3eDJAkSnh6/DNFu0Qg==\n" +
          "-----END CERTIFICATE-----";

  private String privkey3 = "-----BEGIN RSA PRIVATE KEY-----\n" +
          "MIIEowIBAAKCAQEA+33HUh2JzAW+8y9jXs/jH8tINhRAQE/nbgwzxpT/QFcsNw+5\n" +
          "mcSIWiMv5M96FoLl67b2QG75i6E752JuLLlA/4lqX6+TgTrp580m++9k2wkzI1a6\n" +
          "1Y42ikfoRzLHBjHlezvMrxt7q8/CgO5rOugjiF1hRNXCkyH4U6qCI9LIENCflrix\n" +
          "76q5+VdBeT9KGdq0QTHFEpjtvxRlIBjLP054fug3RNRrgXkMc7S0TFk5SlimOsyn\n" +
          "eR99OXn9brxoTIA5AZ0fe9JxYuq472y/jl30LmF3/64UdTLYx0zaa5hgVYah3It6\n" +
          "GXRNbxb58VEPfJ2P4mzoGVqxF7l3PUDeTIGXiQIDAQABAoIBAH8DkEY1quGCyWSy\n" +
          "u0IoRjJJjafaZHTWpjCbMw8JMz0AidEpPPifHKpBeS/bZXK3G34HwqjaI2hUvxdm\n" +
          "S/SEf4JPmYzH9Pxgj7/FifnVdx90rwIbDHNMxtjh5jsHNyM20gqCMicB/1zPqhFJ\n" +
          "2JhAo6l8V+LW/tUmY++FfwKusuJiKtq8gTAhaf+qr9ARnNwRQ0hq+BwEr6XD9ful\n" +
          "rfLEIl3ecvaKXjj6b1inrG2yVN3coibNuDkLXY2EgJG64n1vTu6aBMj0P6HeiXiH\n" +
          "m04OrTP43nQrKV/+7IzrBlqEMzHtj09OgWvXkV6NhIEWswkkY+HkzLj9hgoDdV4Z\n" +
          "FmmmbV0CgYEA/O6cPK4K+ElrGvyJRlkRuD3WzZByaqJyeR6Jdm3qNCQ/9bI5PH/X\n" +
          "gpPqHaFAA0RVlUiV4dZ5BksxW6Lvokbv/i1xlAs8TFobdr5+qcO518e5C8V+CCut\n" +
          "y01LOj0I9ZvFdAHJP514QgHLHhG8/Ua5/kgFWtnZWi4mhak6T04NOk8CgYEA/oqx\n" +
          "zzHHhpaWS4tfQEL0Ibxx1VKT8M4R3QkOnngLJonA1lfqSujIeodLZWm6uzcy30u7\n" +
          "ikCuoKCxK0vE8dOltopwTtlTOuYI2JfdvmFaanOG/857VoFIQZmI8bakaQPr4PXe\n" +
          "ffDZ22k4n4B7k05fZ1MqaCHnZPI1niamfGkJEqcCgYEA0G35BfAOTiiCQIzWusfv\n" +
          "WDptZpygDMutNa46bQOKukkdA+VIUViwSYSGqsAUthx7wjc8fAx3Uv5nwDH2820t\n" +
          "m/Hq5KqVl/2xIBs+2brWzMBi9xZaE3WbFCuv0GA3n94ryrsmEmw7i3la3n6TlMvR\n" +
          "vX+wGfvnpu7dA8w+pteVAvUCgYAO//FWemJ9peYZcY8dZFSqoEY9Ae7B5ALdeako\n" +
          "4X4WuUtp1ihyXaFixxJEWaStX6VZz0av8PvZb17BZGeosIY1aZcQrnHfKKsgyGJC\n" +
          "083WNBSignJ2OIwfgYK2a8LohVijGxoPZeAQs/SoQZQGrDmnBxmapVTTeAp81V4+\n" +
          "OppURQKBgFBqIIXH06SH4FVxTbo+cq6gOHuRIXyLR++AyzJyn8wVfHVJ01r6zhGd\n" +
          "azTCm+fyfw/6jsJfK3b8mLjiz39GAM7vyvkVV5FKYC2wQldxPyow/MhmKBjdqp2p\n" +
          "5gq8MSFaXoZOtRgMWdT4JPLTi7xp8429yRM8JK8E6A9+5aTkUM7H\n" +
          "-----END RSA PRIVATE KEY-----\n";

  @Test
  public void testByteComparisonForPEMCertificateFiles() {

    byte[] fullchain1Bytes = fullchain1.getBytes();
    byte[] privkey1Bytes = privkey1.getBytes();

    byte[] fullchain2Bytes = fullchain2.getBytes();
    byte[] privkey2Bytes = privkey2.getBytes();

    // certificates 1 and 2 are not same
    assertFalse(Arrays.equals(fullchain1Bytes, fullchain2Bytes) && Arrays.equals(privkey1Bytes, privkey2Bytes));

    byte[] fullchain3Bytes = fullchain3.getBytes();
    byte[] privkey3Bytes = privkey3.getBytes();

    // certificates 2 and 3 are same
    assertTrue(Arrays.equals(fullchain2Bytes, fullchain3Bytes) && Arrays.equals(privkey3Bytes, privkey2Bytes));
  }

  @Test
  public void testAdminClientCreatedWithRequiredConfigs() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put("broker.id", "0");
    configs.put("broker.session.uuid", BROKER_UUID);
    configs.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG,
            tempFolder.getRoot().getCanonicalPath());
    configs.put(ConfluentConfigs.MULTITENANT_METADATA_SSL_CERTS_SPEC_CONFIG,
            tempFolder.getRoot().getCanonicalPath() + "/mnt/sslcerts/");
    PhysicalClusterMetadata metadata = new PhysicalClusterMetadata();
    metadata.configure(configs);
    metadata.handleSocketServerInitialized(endpoint);
    assertNotNull(metadata.sslCertificateManager.getAdminClient());
    metadata.close(BROKER_UUID);
  }

  @Test
  public void testAdminClientNotCreatedWithoutBrokerId() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put("broker.session.uuid", BROKER_UUID);
    configs.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG,
            tempFolder.getRoot().getCanonicalPath());
    configs.put(ConfluentConfigs.MULTITENANT_METADATA_SSL_CERTS_SPEC_CONFIG,
            tempFolder.getRoot().getCanonicalPath() + "/mnt/sslcerts/");
    PhysicalClusterMetadata metadata = new PhysicalClusterMetadata();
    metadata.configure(configs);
    metadata.handleSocketServerInitialized(endpoint);
    assertNull(metadata.sslCertificateManager.getAdminClient());
    metadata.close(BROKER_UUID);
  }

  @Test
  public void testAdminClientNotCreatedWithoutSpecConfig() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put("broker.id", "0");
    configs.put("broker.session.uuid", BROKER_UUID);
    configs.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG,
            tempFolder.getRoot().getCanonicalPath());
    PhysicalClusterMetadata metadata = new PhysicalClusterMetadata();
    metadata.configure(configs);
    metadata.handleSocketServerInitialized(endpoint);
    assertNull(metadata.sslCertificateManager.getAdminClient());
    metadata.close(BROKER_UUID);
  }

  @Test
  public void testAdminClientNotCreatedWithMalformedSpecConfig() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put("broker.id", "0");
    configs.put("broker.session.uuid", BROKER_UUID);
    configs.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG,
            tempFolder.getRoot().getCanonicalPath());
    configs.put(ConfluentConfigs.MULTITENANT_METADATA_SSL_CERTS_SPEC_CONFIG, "tempfolderpathmntsslcerts");
    PhysicalClusterMetadata metadata = new PhysicalClusterMetadata();
    metadata.configure(configs);
    metadata.handleSocketServerInitialized(endpoint);
    assertNull(metadata.sslCertificateManager.getAdminClient());
    metadata.close(BROKER_UUID);
  }

  @Test
  public void testAdminClientInvocationOnIdenticalSslCertsSync() throws Exception {
    Utils.syncCerts(tempFolder, TEST_SSL_CERTS_MAY, SSL_CERTS_DIR);
    lcCache.start();
    lcCache.sslCertificateManager.loadSslCertFiles(); // mock call from handleSocketServerInitialized
    verify(mockAdminClient, times(1)).incrementalAlterConfigs(any(), any());
    Utils.deleteFiles(tempFolder, SSL_CERTS_DIR);
    Utils.syncCerts(tempFolder, TEST_SSL_CERTS_MAY, SSL_CERTS_DIR);
    verify(mockAdminClient, timeout(TEST_MAX_WAIT_MS).times(1)).incrementalAlterConfigs(any(), any());
  }

  @Test
  public void testAdminClientInvocationOnDifferentSslCertsSync() throws Exception {
    Utils.syncCerts(tempFolder, TEST_SSL_CERTS_MAY, SSL_CERTS_DIR);
    lcCache.start();
    lcCache.sslCertificateManager.loadSslCertFiles(); // mock call from handleSocketServerInitialized
    verify(mockAdminClient, timeout(TEST_MAX_WAIT_MS).times(1)).incrementalAlterConfigs(any(), any());
    Utils.deleteFiles(tempFolder, SSL_CERTS_DIR);
    Utils.syncCerts(tempFolder, TEST_SSL_CERTS_AUG, SSL_CERTS_DIR);
    verify(mockAdminClient, timeout(TEST_MAX_WAIT_MS).times(2)).incrementalAlterConfigs(any(), any());
    Utils.deleteFiles(tempFolder, SSL_CERTS_DIR);
    Utils.syncCerts(tempFolder, TEST_SSL_CERTS_MAY, SSL_CERTS_DIR);
    verify(mockAdminClient, timeout(TEST_MAX_WAIT_MS).times(3)).incrementalAlterConfigs(any(), any());
  }

  @Test
  public void testWatchServiceDoesNotTerminateOnDirectoryDeletion() throws Exception {
    String logicalClustersDir = tempFolder.getRoot().getCanonicalPath() + "logical_clusters";
    lcCache.configure(logicalClustersDir, TEST_CACHE_RELOAD_DELAY_MS, mockAdminClient,
            BROKER_ID, sslCertsPath);
    Utils.syncCerts(tempFolder, TEST_SSL_CERTS_MAY, SSL_CERTS_DIR);
    lcCache.start();
    lcCache.sslCertificateManager.loadSslCertFiles();
    assertTrue(Files.exists(Paths.get(logicalClustersDir)));
    verify(mockAdminClient, timeout(TEST_MAX_WAIT_MS).times(1)).incrementalAlterConfigs(any(), any());
    Files.delete(Paths.get(logicalClustersDir));
    Utils.deleteFiles(tempFolder, SSL_CERTS_DIR);
    Utils.syncCerts(tempFolder, TEST_SSL_CERTS_AUG, SSL_CERTS_DIR);
    assertFalse(Files.exists(Paths.get(logicalClustersDir)));
    verify(mockAdminClient, timeout(TEST_MAX_WAIT_MS).times(2)).incrementalAlterConfigs(any(), any());
  }
}
