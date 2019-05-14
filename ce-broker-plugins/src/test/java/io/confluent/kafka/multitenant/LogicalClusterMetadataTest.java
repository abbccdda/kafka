// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.server.quota.ClientQuotaType;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static io.confluent.kafka.multitenant.Utils.LC_META_ABC;
import static io.confluent.kafka.multitenant.Utils.LC_META_DED;
import static io.confluent.kafka.multitenant.Utils.LC_META_XYZ;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;

import io.confluent.kafka.multitenant.quota.QuotaConfig;

public class LogicalClusterMetadataTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testLoadMetadataFromFile() throws IOException {
    final Path metaFile = tempFolder.newFile("lkc-xyz.json").toPath();
    Files.write(metaFile, Utils.logicalClusterJsonString(LC_META_XYZ).getBytes());

    // load metadata and verify
    LogicalClusterMetadata meta = loadFromFile(metaFile);
    assertEquals(LC_META_XYZ, meta);
    assertTrue(meta.isValid());
  }

  @Test
  public void testLoadMetadataWithNonDefaultOverheadAndRequestRate() throws IOException {
    final Path metaFile = tempFolder.newFile("lkc-abc.json").toPath();
    Files.write(metaFile, Utils.logicalClusterJsonString(LC_META_ABC).getBytes());

    // load metadata and verify
    LogicalClusterMetadata meta = loadFromFile(metaFile);
    assertEquals(LC_META_ABC, meta);
    assertTrue(meta.isValid());
  }

  @Test
  public void testDefaultHeadroom() throws IOException {
    // ensure we set default headroom even for "legacy" (<=5MB/s) ingress and egress
    final LogicalClusterMetadata legacyQuotaMeta = new LogicalClusterMetadata(
        LC_META_ABC.logicalClusterId(), LC_META_ABC.physicalClusterId(),
        "new-name", "new-account", LC_META_XYZ.k8sClusterId(),
        LC_META_ABC.logicalClusterType(), LC_META_ABC.storageBytes(),
        5L * 1024L * 1024L, 5L * 1024L * 1024L,
        LC_META_ABC.brokerRequestPercentage().longValue(), null, null
    );
    assertEquals(LogicalClusterMetadata.DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE,
                 legacyQuotaMeta.networkQuotaOverhead());
    QuotaConfig quotaConfig = legacyQuotaMeta.quotaConfig();
    assertEquals(5L * 1024L * 1024L, quotaConfig.quota(ClientQuotaType.PRODUCE), 0.001);
    assertEquals(5L * 1024L * 1024L, quotaConfig.quota(ClientQuotaType.FETCH), 0.001);
    assertEquals(LC_META_ABC.brokerRequestPercentage(),
                 quotaConfig.quota(ClientQuotaType.REQUEST), 0.001);

    // low ingress, 100MB/s egress -- same default headroom
    final LogicalClusterMetadata largeConsumeQuotaMeta = new LogicalClusterMetadata(
        LC_META_ABC.logicalClusterId(), LC_META_ABC.physicalClusterId(),
        "new-name", "new-account", LC_META_XYZ.k8sClusterId(),
        LC_META_ABC.logicalClusterType(), LC_META_ABC.storageBytes(),
        1024L * 1024L, 100L * 1024L * 1024L,
        LC_META_ABC.brokerRequestPercentage().longValue(), null, null
    );
    assertEquals(LogicalClusterMetadata.DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE,
                 largeConsumeQuotaMeta.networkQuotaOverhead());
    quotaConfig = largeConsumeQuotaMeta.quotaConfig();
    assertEquals(1024L * 1024L,
                 quotaConfig.quota(ClientQuotaType.PRODUCE), 0.001);
    assertEquals(100L * 1024L * 1024L,
                 quotaConfig.quota(ClientQuotaType.FETCH), 0.001);
    assertEquals(LC_META_ABC.brokerRequestPercentage(),
                 quotaConfig.quota(ClientQuotaType.REQUEST), 0.001);

    // high ingress and egress -- same default headroom
    final LogicalClusterMetadata largeQuotaMeta = new LogicalClusterMetadata(
        LC_META_ABC.logicalClusterId(), LC_META_ABC.physicalClusterId(),
        "new-name", "new-account", LC_META_XYZ.k8sClusterId(),
        LC_META_ABC.logicalClusterType(), LC_META_ABC.storageBytes(),
        100L * 1024L * 1024L, 100L * 1024L * 1024L,
        LC_META_ABC.brokerRequestPercentage().longValue(), null, null
    );
    assertEquals(LogicalClusterMetadata.DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE,
                 largeQuotaMeta.networkQuotaOverhead());
    quotaConfig = largeQuotaMeta.quotaConfig();
    assertEquals(100L * 1024L * 1024L,
                 quotaConfig.quota(ClientQuotaType.PRODUCE), 0.001);
    assertEquals(100L * 1024L * 1024L,
                 quotaConfig.quota(ClientQuotaType.FETCH), 0.001);
    assertEquals(LC_META_ABC.brokerRequestPercentage(),
                 quotaConfig.quota(ClientQuotaType.REQUEST), 0.001);
  }

  @Test
  public void testZeroQuotas() throws IOException {
    final LogicalClusterMetadata zeroQuotaMeta = new LogicalClusterMetadata(
        LC_META_ABC.logicalClusterId(), LC_META_ABC.physicalClusterId(),
        "new-name", "new-account", LC_META_XYZ.k8sClusterId(),
        LC_META_ABC.logicalClusterType(), LC_META_ABC.storageBytes(),
        0L, 0L, LC_META_ABC.brokerRequestPercentage().longValue(), null, null
    );
    assertEquals(LogicalClusterMetadata.DEFAULT_MIN_NETWORK_BYTE_RATE,
                 zeroQuotaMeta.producerByteRate());
    assertEquals(LogicalClusterMetadata.DEFAULT_MIN_NETWORK_BYTE_RATE,
                 zeroQuotaMeta.consumerByteRate());

    // test we can independently set one quota to zero and another higher
    final LogicalClusterMetadata produceZeroQuotaMeta = new LogicalClusterMetadata(
        LC_META_ABC.logicalClusterId(), LC_META_ABC.physicalClusterId(),
        "new-name", "new-account", LC_META_XYZ.k8sClusterId(),
        LC_META_ABC.logicalClusterType(), LC_META_ABC.storageBytes(),
        0L, 100L * 1024L * 1024L, LC_META_ABC.brokerRequestPercentage().longValue(), null, null
    );
    assertEquals(LogicalClusterMetadata.DEFAULT_MIN_NETWORK_BYTE_RATE,
                 produceZeroQuotaMeta.producerByteRate());
    assertEquals(100L * 1024L * 1024L,
                 produceZeroQuotaMeta.consumerByteRate().longValue());

    QuotaConfig quotaConfig = produceZeroQuotaMeta.quotaConfig();
    assertEquals(LogicalClusterMetadata.DEFAULT_MIN_NETWORK_BYTE_RATE,
                 quotaConfig.quota(ClientQuotaType.PRODUCE), 0.001);
    assertEquals(100L * 1024L * 1024L,
                 quotaConfig.quota(ClientQuotaType.FETCH), 0.001);
    assertEquals(LC_META_ABC.brokerRequestPercentage(),
                 quotaConfig.quota(ClientQuotaType.REQUEST), 0.001);
  }

  @Test
  public void testLifeCycleMetadataOfLiveCluster() throws IOException {
    final Path metaFile = tempFolder.newFile("lkc-xyz.json").toPath();
    Files.write(metaFile, Utils.logicalClusterJsonString(LC_META_XYZ).getBytes());

    // load metadata and verify that we have lifecycle metadata
    LogicalClusterMetadata meta = loadFromFile(metaFile);
    assertEquals(LC_META_XYZ, meta);
    assertEquals(meta.lifecycleMetadata().logicalClusterName(), "xyz");
  }

  @Test
  public void testLifeCycleMetadataOfDeadCluster() throws IOException {

    final Path metaFile = tempFolder.newFile("lkc-abs.json").toPath();
    Files.write(metaFile, Utils.logicalClusterJsonString(LC_META_DED).getBytes());

    // load metadata and verify that we have lifecycle metadata
    LogicalClusterMetadata meta = loadFromFile(metaFile);
    assertEquals(LC_META_DED, meta);
    assertTrue(meta.lifecycleMetadata().deletionDate().before(new Date()));
  }

  @Test
  public void testMetadataWithNoByteRatesIsValid() throws IOException {
    final String lcId = "lkc-fhg";
    final String invalidMeta = "{" +
                                "\"logical_cluster_id\": \"" + lcId + "\"," +
                                "\"physical_cluster_id\": \"pkc-fhg\"," +
                                "\"logical_cluster_name\": \"name\"," +
                                "\"account_id\": \"account\"," +
                                "\"k8s_cluster_id\": \"k8s-cluster\"," +
                                "\"logical_cluster_type\": \"kafka\"" +
                                "}";
    final Path metaFile = tempFolder.newFile(lcId + ".json").toPath();
    Files.write(metaFile, invalidMeta.getBytes());

    // should be able to load valid json
    LogicalClusterMetadata meta = loadFromFile(metaFile);
    assertNotNull(meta);
    // valid metadata
    assertTrue(meta.isValid());

    // unset (null) produce/consume rate is valid
    assertNull(meta.producerByteRate());
    assertNull(meta.consumerByteRate());
    // unset request rate will result in default request rate (since currently all logical
    // clusters have it unset and we assume default)
    assertEquals(LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE_PER_BROKER,
                 meta.brokerRequestPercentage(), 0.001);

    QuotaConfig quotaConfig = meta.quotaConfig();
    // unset produce/consume rate will result in unlimited produce/consume bandwidth quota
    assertEquals(QuotaConfig.UNLIMITED_QUOTA.quota(ClientQuotaType.PRODUCE),
                 quotaConfig.quota(ClientQuotaType.PRODUCE), 0.001);
    assertEquals(QuotaConfig.UNLIMITED_QUOTA.quota(ClientQuotaType.FETCH),
                 quotaConfig.quota(ClientQuotaType.FETCH), 0.001);
    assertEquals(LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE_PER_BROKER,
                 quotaConfig.quota(ClientQuotaType.REQUEST), 0.001);
  }

  @Test
  public void testLoadMetadataWithInvalidClusterType() throws IOException {
    final String lcId = "lkc-fhg";
    final String invalidMeta = "{" +
                                "\"logical_cluster_id\": \"" + lcId + "\"," +
                                "\"physical_cluster_id\": \"pkc-fhg\"," +
                                "\"logical_cluster_name\": \"name\"," +
                                "\"account_id\": \"account\"," +
                                "\"k8s_cluster_id\": \"k8s-cluster\"," +
                                "\"logical_cluster_type\": \"not-kafka\"," +
                                "\"storage_bytes\": 100," +
                                "\"network_ingress_byte_rate\": 1024," +
                                "\"network_egress_byte_rate\": 1024" +
                                "}";
    final Path metaFile = tempFolder.newFile(lcId + ".json").toPath();
    Files.write(metaFile, invalidMeta.getBytes());

    // should be able to load valid json
    LogicalClusterMetadata meta = loadFromFile(metaFile);
    assertNotNull(meta);
    // but not valid metadata
    assertFalse(meta.isValid());
    assertEquals(lcId, meta.logicalClusterId());
    assertEquals((Long) 1024L, meta.producerByteRate());
    assertEquals((Long) 1024L, meta.consumerByteRate());
  }

  private LogicalClusterMetadata loadFromFile(Path metaFile) {
    LogicalClusterMetadata retMeta = null;
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      retMeta = objectMapper.readValue(metaFile.toFile(), LogicalClusterMetadata.class);
    } catch (IOException ioe) {
      fail("Failed to read logical cluster metadata from file " + metaFile);
    }
    return retMeta;
  }

}
