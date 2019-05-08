// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant;

import com.google.common.collect.ImmutableSet;

import org.apache.kafka.common.Cluster;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
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

  private PhysicalClusterMetadata lcCache;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    lcCache = new PhysicalClusterMetadata();
    lcCache.configure(tempFolder.getRoot().getCanonicalPath(), TEST_CACHE_RELOAD_DELAY_MS);
    // but not started, so we can test different initial state of the directory
  }

  @After
  public void tearDown() throws Exception {
    lcCache.shutdown();
  }

  @Test
  public void testCreateAndRemoveInstance() throws Exception {
    final String brokerUUID = "test-uuid";
    Map<String, Object> configs = new HashMap<>();
    configs.put("broker.session.uuid", String.valueOf(brokerUUID));
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


    assertFalse("We expect that the deactivated cluster will be in process of getting deleted",
            lcCache.tenantLifecycleManager.deletedClusters().contains(LC_META_DED));
    assertFalse("We expect that deactivated clusters will not be in cache, even as stale",
            lcCache.logicalClusterIdsIncludingStale().contains(LC_META_DED.logicalClusterId()));
  }

  @Test
  public void testTenantQuotaUpdateUpdatesQuotaLimitAndUpdatesQuotaResetRequired()
      throws IOException, InterruptedException {
    final LogicalClusterMetadata legacyTenant =
        new LogicalClusterMetadata(
            "lkc-leg", "pkc-a7sjfe", "my-poc-cluster", "my-account", "k8s-abc",
            LogicalClusterMetadata.KAFKA_LOGICAL_CLUSTER_TYPE,
            5242880000L, 5242880L, 5242880L, 50L,
            LogicalClusterMetadata.DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE,
            new LogicalClusterMetadata.LifecycleMetadata("my-poc-cluster", "pkc-a7sjfe", null, null));
    final LogicalClusterMetadata upgradedTenant =
        Utils.updateQuotas(legacyTenant, 104857600L, 104857600L, 250L);

    // tenant
    Utils.createLogicalClusterFile(legacyTenant, tempFolder);

    // cluster
    final int brokerId = 1;
    final int numBrokers = 8;
    TenantQuotaCallback quotaCallback = new TenantQuotaCallback();
    quotaCallback.configure(Collections.singletonMap("broker.id", String.valueOf(brokerId)));
    TestCluster testCluster = new TestCluster();
    for (int i = 1; i <= numBrokers; i++) {
      testCluster.addNode(i, "rack0");
    }
    Cluster cluster = testCluster.cluster();
    quotaCallback.updateClusterMetadata(cluster);

    // partitions
    final String topic = legacyTenant.logicalClusterId() + "_topic1";
    for (int i = 1; i <= numBrokers; i++) {
      testCluster.setPartitionLeaders(topic, i, 1, i);
    }
    quotaCallback.updateClusterMetadata(testCluster.cluster());

    // since quota update came from metadata update, quota reset flags should not be set
    assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE));
    assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.FETCH));
    assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.REQUEST));

    // start the broker
    lcCache.start();
    assertTrue("Expected cache to be initialized", lcCache.isUp());

    Map<String, String> tags = Collections.singletonMap("tenant", legacyTenant.logicalClusterId());
    assertEquals(2.0 * 5242880.0 / numBrokers,
                 quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, tags), 0.001);
    assertEquals(2.0 * 5242880.0 / numBrokers,
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
        () -> lcCache.metadata(legacyTenant.logicalClusterId()).producerByteRate() == 104857600L,
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
}
