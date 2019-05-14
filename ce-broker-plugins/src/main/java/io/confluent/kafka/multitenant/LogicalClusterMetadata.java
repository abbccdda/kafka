// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.multitenant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Date;
import java.util.Objects;

import io.confluent.kafka.multitenant.quota.QuotaConfig;

/**
 * Represents logical cluster metadata
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LogicalClusterMetadata {

  public static final String KAFKA_LOGICAL_CLUSTER_TYPE = "kafka";
  public static final String HEALTHCHECK_LOGICAL_CLUSTER_TYPE = "healthcheck";
  public static final Double DEFAULT_REQUEST_PERCENTAGE_PER_BROKER = 250.0;

  // 100% overhead means that bandwidth quota will be set to byte_rate + 100% of byte_rate
  // default is 0%, since default tenant read and write quotas are much higher now (50 - 100
  // MB/sec), and our message to customers is now "up to" quota, not provisioned.
  public static final Integer DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE = 0;
  public static final Long DEFAULT_HEALTHCHECK_MAX_PRODUCER_RATE = 10L * 1024L * 1024L;
  public static final Long DEFAULT_HEALTHCHECK_MAX_CONSUMER_RATE = 10L * 1024L * 1024L;
  // some very small number so that we do not have zero quotas, the actual minimum per broker
  // will be set by quota assignor
  public static final Long DEFAULT_MIN_NETWORK_BYTE_RATE = 1024L;

  private final String logicalClusterId;
  private final String physicalClusterId;
  private final String logicalClusterName;
  private final String accountId;
  private final String k8sClusterId;
  private final String logicalClusterType;
  private final Long storageBytes;
  private final Long producerByteRate;
  private final Long consumerByteRate;
  private final Double brokerRequestPercentage;
  private final Integer networkQuotaOverhead;
  private final LifecycleMetadata lifecycleMetadata;


  @JsonCreator
  public LogicalClusterMetadata(
      @JsonProperty("logical_cluster_id") String logicalClusterId,
      @JsonProperty("physical_cluster_id") String physicalClusterId,
      @JsonProperty("logical_cluster_name") String logicalClusterName,
      @JsonProperty("account_id") String accountId,
      @JsonProperty("k8s_cluster_id") String k8sClusterId,
      @JsonProperty("logical_cluster_type") String logicalClusterType,
      @JsonProperty("storage_bytes") Long storageBytes,
      @JsonProperty("network_ingress_byte_rate") Long producerByteRate,
      @JsonProperty("network_egress_byte_rate") Long consumerByteRate,
      @JsonProperty("broker_request_percentage") Long brokerRequestPercentage,
      @JsonProperty("network_quota_overhead") Integer networkQuotaOverhead,
      @JsonProperty("metadata") LifecycleMetadata lifecycleMetadata
  ) {
    this.logicalClusterId = logicalClusterId;
    this.physicalClusterId = physicalClusterId;
    this.logicalClusterName = logicalClusterName;
    this.accountId = accountId;
    this.k8sClusterId = k8sClusterId;
    this.logicalClusterType = logicalClusterType;
    this.storageBytes = storageBytes;

    // handle 0 values for ingress/egress by setting 1KB total quota. The quota distribution
    // algorithm will set appropriate per-broker minimums (see defaults and configs in
    // TenantQuotaCallback). The reason why we need to handle 0 quotas, is that CCloud side is
    // not able to set any quotas that are below 1 MB/sec, so it is currently impossible to set
    // small non-zero quotas. We are handling 0-quota case as "a very small quota", which
    // will be defined by per-broker minimums.
    Long validProducerByteRate = producerByteRate;
    if (validProducerByteRate != null) {
      validProducerByteRate = Math.max(DEFAULT_MIN_NETWORK_BYTE_RATE, producerByteRate);
    } else if (HEALTHCHECK_LOGICAL_CLUSTER_TYPE.equals(logicalClusterType)) {
      validProducerByteRate = DEFAULT_HEALTHCHECK_MAX_PRODUCER_RATE;
    }
    Long validConsumerByteRate = consumerByteRate;
    if (validConsumerByteRate != null) {
      validConsumerByteRate = Math.max(DEFAULT_MIN_NETWORK_BYTE_RATE, consumerByteRate);
    } else if (HEALTHCHECK_LOGICAL_CLUSTER_TYPE.equals(logicalClusterType)) {
      validConsumerByteRate = DEFAULT_HEALTHCHECK_MAX_CONSUMER_RATE;
    }
    this.producerByteRate = validProducerByteRate;
    this.consumerByteRate = validConsumerByteRate;
    this.brokerRequestPercentage = brokerRequestPercentage == null || brokerRequestPercentage == 0 ?
                                   DEFAULT_REQUEST_PERCENTAGE_PER_BROKER : brokerRequestPercentage;
    this.networkQuotaOverhead = networkQuotaOverhead == null ?
                                DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE : networkQuotaOverhead;
    this.lifecycleMetadata = lifecycleMetadata;
  }

  @JsonProperty
  public String logicalClusterId() {
    return logicalClusterId;
  }

  @JsonProperty
  public String physicalClusterId() {
    return physicalClusterId;
  }

  @JsonProperty
  public String logicalClusterName() {
    return logicalClusterName;
  }

  @JsonProperty
  public String accountId() {
    return accountId;
  }

  @JsonProperty
  public String k8sClusterId() {
    return k8sClusterId;
  }

  @JsonProperty
  public String logicalClusterType() {
    return logicalClusterType;
  }

  @JsonProperty
  public Long storageBytes() {
    return storageBytes;
  }

  @JsonProperty
  public Long producerByteRate() {
    return producerByteRate;
  }

  @JsonProperty
  public Long consumerByteRate() {
    return consumerByteRate;
  }

  @JsonProperty
  public Double brokerRequestPercentage() {
    return brokerRequestPercentage;
  }

  @JsonProperty
  public Integer networkQuotaOverhead() {
    return networkQuotaOverhead;
  }

  @JsonProperty
  public LifecycleMetadata lifecycleMetadata() {
     return lifecycleMetadata;
  }

  /**
   * Returns true if metadata values are valid
   */
  boolean isValid() {
    return KAFKA_LOGICAL_CLUSTER_TYPE.equals(logicalClusterType) ||
           HEALTHCHECK_LOGICAL_CLUSTER_TYPE.equals(logicalClusterType);
  }

  public QuotaConfig quotaConfig() {
    double multiplier = 1 + networkQuotaOverhead() / 100.0;
    Long producerByteRate = producerByteRate() == null ?
                            null : (long) (multiplier * producerByteRate());
    Long consumerByteRate = consumerByteRate() == null ?
                            null : (long) (multiplier * consumerByteRate());
    // will use default (unlimited) for null quotas
    return new QuotaConfig(producerByteRate, consumerByteRate,
                           brokerRequestPercentage(), QuotaConfig.UNLIMITED_QUOTA);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LogicalClusterMetadata that = (LogicalClusterMetadata) o;
    return Objects.equals(logicalClusterId, that.logicalClusterId) &&
           Objects.equals(physicalClusterId, that.physicalClusterId) &&
           Objects.equals(logicalClusterName, that.logicalClusterName) &&
           Objects.equals(accountId, that.accountId) &&
           Objects.equals(k8sClusterId, that.k8sClusterId) &&
           Objects.equals(logicalClusterType, that.logicalClusterType) &&
           Objects.equals(storageBytes, that.storageBytes) &&
           Objects.equals(producerByteRate, that.producerByteRate) &&
           Objects.equals(consumerByteRate, that.consumerByteRate) &&
           Objects.equals(brokerRequestPercentage, that.brokerRequestPercentage) &&
           Objects.equals(networkQuotaOverhead, that.networkQuotaOverhead) &&
           Objects.equals(lifecycleMetadata, that.lifecycleMetadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        logicalClusterId, physicalClusterId, logicalClusterName, accountId, k8sClusterId,
        logicalClusterType, storageBytes, producerByteRate, consumerByteRate,
        brokerRequestPercentage, networkQuotaOverhead, lifecycleMetadata
    );
  }

  @Override
  public String toString() {
    return "LogicalClusterMetadata(logicalClusterId=" + logicalClusterId +
           ", physicalClusterId=" + physicalClusterId +
           ", logicalClusterName=" + logicalClusterName +
           ", accountId=" + accountId + ", k8sClusterId=" + k8sClusterId +
           ", logicalClusterType=" + logicalClusterType + ", storageBytes=" + storageBytes +
           ", producerByteRate=" + producerByteRate + ", consumerByteRate=" + consumerByteRate +
           ", brokerRequestPercentage=" + brokerRequestPercentage +
           ", networkQuotaOverhead=" + networkQuotaOverhead +
           ", lifecycleMetadata=" + lifecycleMetadata +
           ')';
  }

  public static class LifecycleMetadata {

    private final String logicalClusterName;
    private final String physicalK8sNamespace;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    private final Date creationDate;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    private final Date deletionDate;

    @JsonCreator
    public LifecycleMetadata(
            @JsonProperty("name") String logicalClusterName,
            @JsonProperty("namespace") String physicalK8sNamespace,
            @JsonProperty("creationTimestamp") Date creationDate,
            @JsonProperty("deletionTimestamp") Date deletionDate
    ) {
      this.logicalClusterName = logicalClusterName;
      this.physicalK8sNamespace = physicalK8sNamespace;
      this.creationDate = creationDate;
      this.deletionDate = deletionDate;
    }

    @JsonProperty
    public String logicalClusterName() {
      return logicalClusterName;
    }

    @JsonProperty
    public String physicalK8sNamespace() {
      return physicalK8sNamespace;
    }

    @JsonProperty
    public Date creationDate() {
      return creationDate;
    }

    @JsonProperty
    public Date deletionDate() {
      return deletionDate;
    }


    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LifecycleMetadata that = (LifecycleMetadata) o;
      return  Objects.equals(logicalClusterName, that.logicalClusterName) &&
              Objects.equals(physicalK8sNamespace, that.physicalK8sNamespace) &&
              Objects.equals(creationDate, that.creationDate) &&
              Objects.equals(deletionDate, that.deletionDate);
    }

    @Override
    public int hashCode() {
      return Objects.hash(logicalClusterName, physicalK8sNamespace, creationDate, deletionDate);
    }

    @Override
    public String toString() {
      return "LifecycleMetadata{" +
              "logicalClusterName='" + logicalClusterName + '\'' +
              ", physicalK8sNamespace='" + physicalK8sNamespace + '\'' +
              ", creationDate=" + creationDate +
              ", deletionDate=" + deletionDate +
              '}';
    }
  }
}
