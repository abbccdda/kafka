// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.multitenant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Date;
import java.util.Map;
import java.util.stream.Stream;

public class Utils {
  public static final LogicalClusterMetadata LC_META_XYZ =
      new LogicalClusterMetadata("lkc-xyz", "pkc-xyz", "xyz",
          "my-account", "k8s-abc", LogicalClusterMetadata.KAFKA_LOGICAL_CLUSTER_TYPE,
          104857600L, 10240000L, 2048L,
          LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE_PER_BROKER.longValue(),
          LogicalClusterMetadata.DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE,
          new LogicalClusterMetadata.LifecycleMetadata("xyz", "pkc-xyz", null, null));
  public static final LogicalClusterMetadata LC_META_ABC =
      new LogicalClusterMetadata("lkc-abc", "pkc-abc", "abc",
          "my-account", "k8s-abc", LogicalClusterMetadata.KAFKA_LOGICAL_CLUSTER_TYPE,
          10485760L, 10240000L, 204800L,
          LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE_PER_BROKER.longValue(),
          LogicalClusterMetadata.DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE,
          new LogicalClusterMetadata.LifecycleMetadata("abc", "pkc-abc", null, null));
  // Note that this cluster will be deactivated on arrival, but maybe not deleted yet
  public static final LogicalClusterMetadata LC_META_DED =
          new LogicalClusterMetadata("lkc-ded", "pkc-ded", "ded",
          "my-account", "k8s-abc", LogicalClusterMetadata.KAFKA_LOGICAL_CLUSTER_TYPE,
          10485760L, 10240000L, 204800L,
          LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE_PER_BROKER.longValue(),
          LogicalClusterMetadata.DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE,
          new LogicalClusterMetadata.LifecycleMetadata("ded", "pkc-ded", null, new Date()));
  // Cluster that was deactivated some time ago and is ready for deletion according to our
  // default policy
  public static final LogicalClusterMetadata LC_META_MEH =
          new LogicalClusterMetadata("lkc-meh", "pkc-meh", "meh",
                  "my-account", "k8s-abc", LogicalClusterMetadata.KAFKA_LOGICAL_CLUSTER_TYPE,
                  10485760L, 10240000L, 204800L,
                  LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE_PER_BROKER.longValue(),
                  LogicalClusterMetadata.DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE,
                  new LogicalClusterMetadata.LifecycleMetadata("meh", "pkc-meh", null,
                          new Date(System.currentTimeMillis() - ConfluentConfigs.MULTITENANT_TENANT_DELETE_DELAY_MS_DEFAULT)));

  public static final LogicalClusterMetadata LC_META_HEALTHCHECK =
      new LogicalClusterMetadata("lkc-htc", "pkc-xyz", "external-healthcheck-pkc-xyz", "my-account",
                                 "k8s-abc", LogicalClusterMetadata.HEALTHCHECK_LOGICAL_CLUSTER_TYPE,
                                 null, null, null, null, null, null);

  public static PhysicalClusterMetadata initiatePhysicalClusterMetadata(Map<String, Object> configs) throws IOException {
    return initiatePhysicalClusterMetadata(configs, ConfluentConfigs.MULTITENANT_METADATA_RELOAD_DELAY_MS_DEFAULT);
  }

  public static PhysicalClusterMetadata initiatePhysicalClusterMetadata(Map<String, Object> configs, long reloadDelay) throws IOException {
    configs.put(ConfluentConfigs.MULTITENANT_METADATA_RELOAD_DELAY_MS_CONFIG, reloadDelay);
    PhysicalClusterMetadata metadata = new PhysicalClusterMetadata();
    metadata.configure(configs);

    return metadata;
  }

  public static void createLogicalClusterFile(LogicalClusterMetadata lcMeta, TemporaryFolder
      tempFolder)
      throws IOException {
    updateLogicalClusterFile(lcMeta, false, true, tempFolder);
  }

  public static void createInvalidLogicalClusterFile(LogicalClusterMetadata lcMeta, TemporaryFolder
      tempFolder)
      throws IOException {
    updateLogicalClusterFile(lcMeta, false, false, tempFolder);
  }

  /**
   * This currently has the same implementation as createLogicalClusterFile, since the underlying
   * method is the same for both, but having a separate method is useful if we need to make
   * changes to the behavior.
   */
  public static void updateLogicalClusterFile(LogicalClusterMetadata lcMeta, TemporaryFolder
      tempFolder)
      throws IOException {
    updateLogicalClusterFile(lcMeta, false, true, tempFolder);
  }

  public static void updateInvalidLogicalClusterFile(LogicalClusterMetadata lcMeta, TemporaryFolder
      tempFolder)
      throws IOException {
    updateLogicalClusterFile(lcMeta, false, false, tempFolder);
  }


  public static void deleteLogicalClusterFile(LogicalClusterMetadata lcMeta, TemporaryFolder tempFolder)
      throws IOException {
    updateLogicalClusterFile(lcMeta, true, true, tempFolder);
  }

  public static void setPosixFilePermissions(LogicalClusterMetadata lcMeta,
                                             String posixFilePermissionsStr,
                                             TemporaryFolder tempFolder) throws IOException {
    final String lcFilename = lcMeta.logicalClusterId() + ".json";
    final Path metaPath = Paths.get(
        tempFolder.getRoot().toString(), PhysicalClusterMetadata.DATA_DIR_NAME, lcFilename);
    Files.setPosixFilePermissions(metaPath, PosixFilePermissions.fromString(posixFilePermissionsStr));
  }

  private static void updateLogicalClusterFile(LogicalClusterMetadata lcMeta,
                                               boolean remove,
                                               boolean valid,
                                               TemporaryFolder tempFolder) throws IOException {
    final String lcFilename = lcMeta.logicalClusterId() + ".json";
    updateJsonFile(lcFilename, logicalClusterJsonString(lcMeta, valid), remove, tempFolder);
  }

  public static Path updateJsonFile(String jsonFilename,
                                    String jsonString,
                                    boolean remove,
                                    TemporaryFolder tempFolder)
      throws IOException {
    // create logical cluster file in tempFolder/<newDir>
    final Path newDir = tempFolder.newFolder().toPath();
    Path lcFile = null;
    if (!remove) {
      lcFile = Paths.get(newDir.toString(), jsonFilename);
      Files.write(lcFile, jsonString.getBytes());
    }

    // this is ..data symbolic link
    final Path dataDir = Paths.get(
        tempFolder.getRoot().toString(), PhysicalClusterMetadata.DATA_DIR_NAME);

    // if ..data dir already exists, move all files from old dir (target of ..data) to new dir
    if (Files.exists(dataDir)) {
      Path oldDir = Files.readSymbolicLink(dataDir);
      // move all existing files to newDir, except the file that represents the same logical
      // cluster, so that we can use the same method for updating files)
      try (Stream<Path> fileStream = Files.list(oldDir)) {
        fileStream.forEach(filePath -> {
          try {
            if (!filePath.getFileName().toString().equals(jsonFilename)) {
              Files.move(filePath, Paths.get(newDir.toString(), filePath.getFileName().toString()));
            } else {
              Files.delete(filePath);
            }
          } catch (IOException ioe) {
            throw new RuntimeException("Test failed to simulate logical cluster file creation.", ioe);
          }
        });
      }

      Files.delete(dataDir);   // symbolic link
      Files.delete(oldDir);    // target dir (which is already empty)
    }

    // point ..data to new dir
    Files.createSymbolicLink(dataDir, newDir);
    return lcFile;
  }

  static String logicalClusterJsonString(LogicalClusterMetadata lcMeta, boolean setQuotaOverhead, boolean setReqPercent) {
    String json = baseLogicalClusterJsonString(lcMeta);
    if (setQuotaOverhead) {
      json += ", \"network_quota_overhead\": " + lcMeta.networkQuotaOverhead();
    }
    if (setReqPercent) {
      json += ", \"broker_request_percentage\": " + lcMeta.brokerRequestPercentage();
    }
    json += "}";

    return json;
  }

  static String logicalClusterJsonString(LogicalClusterMetadata lcMeta, boolean valid) {
    String json = baseLogicalClusterJsonString(lcMeta);
    if (valid) {
      json += "}";
    }
    return json;
  }

  private static String baseLogicalClusterJsonString(LogicalClusterMetadata lcMeta) {
    String json = "{" +
        "\"logical_cluster_id\": \"" + lcMeta.logicalClusterId() + "\"," +
        "\"physical_cluster_id\": \"" + lcMeta.physicalClusterId() + "\"," +
        "\"logical_cluster_name\": \"" + lcMeta.logicalClusterName() + "\"," +
        "\"account_id\": \"" + lcMeta.accountId() + "\"," +
        "\"k8s_cluster_id\": \"" + lcMeta.k8sClusterId() + "\"," +
        "\"logical_cluster_type\": \"" + lcMeta.logicalClusterType() + "\"";
    try {
      json += ", \"metadata\": " + lifecycleJsonString(lcMeta);
    } catch (JsonProcessingException e) {
      // if we can't get the json string for the lifecycleMetadata, just skip it
    }
    if (lcMeta.storageBytes() != null) {
      json += ", \"storage_bytes\": " + lcMeta.storageBytes();
    }
    if (lcMeta.producerByteRate() != null) {
      json += ", \"network_ingress_byte_rate\": " + lcMeta.producerByteRate();
    }
    if (lcMeta.consumerByteRate() != null) {
      json += ", \"network_egress_byte_rate\": " + lcMeta.consumerByteRate();
    }

    return json;
  }

  private static String lifecycleJsonString(LogicalClusterMetadata lcMeta) throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();

    return mapper.writeValueAsString(lcMeta.lifecycleMetadata());
  }

}
