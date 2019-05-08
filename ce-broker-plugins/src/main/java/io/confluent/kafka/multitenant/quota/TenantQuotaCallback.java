// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.multitenant.quota;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.TenantMetadata;
import io.confluent.kafka.multitenant.metrics.TenantMetrics;
import io.confluent.kafka.multitenant.schema.TenantContext;

import java.util.HashSet;
import java.util.Objects;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.quota.ClientQuotaCallback;
import org.apache.kafka.server.quota.ClientQuotaEntity;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TenantQuotaCallback implements ClientQuotaCallback {
  private static final Logger log = LoggerFactory.getLogger(TenantQuotaCallback.class);

  static final String MAX_BROKER_TENANT_PRODUCER_BYTE_RATE_CONFIG =
      "confluent.quota.tenant.broker.max.producer.rate";
  static final String MAX_BROKER_TENANT_CONSUMER_BYTE_RATE_CONFIG =
      "confluent.quota.tenant.broker.max.consumer.rate";
  static final String MIN_BROKER_TENANT_PRODUCER_BYTE_RATE_CONFIG =
      "confluent.quota.tenant.broker.min.producer.rate";
  static final String MIN_BROKER_TENANT_CONSUMER_BYTE_RATE_CONFIG =
      "confluent.quota.tenant.broker.min.consumer.rate";

  // Default cap on tenant quota that can be assigned to a single broker for produce and consume
  // quotas: 12.5MB/sec each. With 100MB/sec cluster-wide tenant quota, a tenant needs to send
  // load to at least 8 partitions (on 8 brokers) to get the full produce and consume quota.
  public static final long DEFAULT_MAX_BROKER_TENANT_PRODUCER_BYTE_RATE = 13107200;
  public static final long DEFAULT_MAX_BROKER_TENANT_CONSUMER_BYTE_RATE = 13107200;

  // Default minimum quota that can be assigned to a single broker: 32KB/sec (2x default batch size)
  // Per-broker tenant quota is always greater than zero to avoid excessive throttling of
  // requests received before cluster metadata or quota configs are refreshed.
  public static final long DEFAULT_MIN_BROKER_TENANT_PRODUCER_BYTE_RATE = 32768;
  public static final long DEFAULT_MIN_BROKER_TENANT_CONSUMER_BYTE_RATE = 32768;

  // TODO: This is a temporary workaround to track TenantQuotaCallbacks.
  // This is used by interceptors to find a partition assignor that has access to
  // the cluster metadata from the configured quota callback. This is also used
  // by cloud secret file loader to notify quota callback of updates to tenant's
  // quotas whenever the file is loaded.
  private static final Map<Integer, TenantQuotaCallback> INSTANCES = new HashMap<>();

  private final EnumMap<ClientQuotaType, AtomicBoolean> quotaResetPending =
      new EnumMap<>(ClientQuotaType.class);
  private final ConcurrentHashMap<String, TenantQuota> tenantQuotas;
  private final TenantPartitionAssignor partitionAssignor;

  private volatile int brokerId;
  private volatile long maxPerTenantBrokerProducerRate;
  private volatile long maxPerTenantBrokerConsumerRate;
  private volatile long minPerTenantBrokerProducerRate;
  private volatile long minPerTenantBrokerConsumerRate;
  private volatile Cluster cluster;
  private volatile QuotaConfig defaultTenantQuota;

  public TenantQuotaCallback() {
    for (ClientQuotaType quotaType : ClientQuotaType.values()) {
      quotaResetPending.put(quotaType, new AtomicBoolean());
    }
    tenantQuotas = new ConcurrentHashMap<>();
    this.defaultTenantQuota = QuotaConfig.UNLIMITED_QUOTA;
    this.partitionAssignor = new TenantPartitionAssignor();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.brokerId = intConfig(configs, KafkaConfig$.MODULE$.BrokerIdProp());
    synchronized (INSTANCES) {
      INSTANCES.put(brokerId, this);
    }

    minPerTenantBrokerProducerRate = loadPerTenantBrokerByteRateConfig(
        configs, MIN_BROKER_TENANT_PRODUCER_BYTE_RATE_CONFIG,
        DEFAULT_MIN_BROKER_TENANT_PRODUCER_BYTE_RATE, 1L);
    minPerTenantBrokerConsumerRate = loadPerTenantBrokerByteRateConfig(
        configs, MIN_BROKER_TENANT_CONSUMER_BYTE_RATE_CONFIG,
        DEFAULT_MIN_BROKER_TENANT_CONSUMER_BYTE_RATE, 1L);
    maxPerTenantBrokerProducerRate = loadPerTenantBrokerByteRateConfig(
        configs, MAX_BROKER_TENANT_PRODUCER_BYTE_RATE_CONFIG,
        DEFAULT_MAX_BROKER_TENANT_PRODUCER_BYTE_RATE, minPerTenantBrokerProducerRate);
    maxPerTenantBrokerConsumerRate = loadPerTenantBrokerByteRateConfig(
        configs, MAX_BROKER_TENANT_CONSUMER_BYTE_RATE_CONFIG,
        DEFAULT_MAX_BROKER_TENANT_CONSUMER_BYTE_RATE, minPerTenantBrokerConsumerRate);

    log.info("Configured tenant quota callback for broker {} with {}={}, {}={}, {}={}, {}={}",
             brokerId,
             MIN_BROKER_TENANT_PRODUCER_BYTE_RATE_CONFIG, minPerTenantBrokerProducerRate,
             MIN_BROKER_TENANT_CONSUMER_BYTE_RATE_CONFIG, minPerTenantBrokerConsumerRate,
             MAX_BROKER_TENANT_PRODUCER_BYTE_RATE_CONFIG, maxPerTenantBrokerProducerRate,
             MAX_BROKER_TENANT_CONSUMER_BYTE_RATE_CONFIG, maxPerTenantBrokerConsumerRate);
  }

  @Override
  public Map<String, String> quotaMetricTags(ClientQuotaType quotaType, KafkaPrincipal principal,
                                             String clientId) {
    if (principal instanceof MultiTenantPrincipal) {
      TenantMetadata tenantMetadata = ((MultiTenantPrincipal) principal).tenantMetadata();
      String tenant = tenantMetadata.tenantName;
      TenantQuota tenantQuota = getOrCreateTenantQuota(tenant, defaultTenantQuota, false);
      if (!tenantQuota.hasQuotaLimit(quotaType)) {
        // Unlimited quota configured for tenant/default, so not adding any tags
        return Collections.emptyMap();
      } else {
        // We currently have only one-level of quotas for tenants, so it is safe to return
        // just the tenant tags.
        return tenantMetricTags(((MultiTenantPrincipal) principal).tenantMetadata().tenantName);
      }
    } else {
      // For internal listeners, the principal will not be a tenant principal and
      // we don't currently configure quotas for clients on these listeners.
      return Collections.emptyMap();
    }
  }

  @Override
  public Double quotaLimit(ClientQuotaType quotaType, Map<String, String> metricTags) {
    String tenant = metricTags.get(TenantMetrics.TENANT_TAG);
    if (tenant == null || tenant.isEmpty()) {
      return QuotaConfig.UNLIMITED_QUOTA.quota(quotaType);
    } else {
      TenantQuota tenantQuota = tenantQuotas.get(tenant);
      if (tenantQuota != null) {
        return tenantQuota.quotaLimit(quotaType);
      } else {
        log.warn("Quota not found for tenant {}, using default quota", tenant);
        return defaultTenantQuota.quota(quotaType);
      }
    }
  }

  @Override
  public void updateQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity,
                          double newValue) {
    // We currently don't use quotas configured in ZooKeeper
  }

  @Override
  public void removeQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity) {
    // We currently don't use quotas configured in ZooKeeper
  }

  @Override
  public boolean quotaResetRequired(ClientQuotaType quotaType) {
    return quotaResetPending.get(quotaType).getAndSet(false);
  }

  /**
   * Handle metadata update. This method is invoked whenever the broker receives
   * UpdateMetadata request from the controller. Recompute all quotas to take
   * the current partition allocation into account.
   */
  @Override
  public synchronized boolean updateClusterMetadata(Cluster cluster) {
    log.debug("Updating cluster metadata {}", cluster);
    partitionAssignor.updateClusterMetadata(cluster);

    this.cluster = cluster;
    Map<String, Set<Integer>> brokersHostingLeaders = new HashMap<>();
    Map<String, Integer> tenantPartitionsOnThisBroker = new HashMap<>();
    tenantQuotas.keySet().forEach(tenant -> tenantPartitionsOnThisBroker.put(tenant, 0));
    for (String topic : cluster.topics()) {
      String tenant = topicTenant(topic);
      if (!tenant.isEmpty()) {
        for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {
          Node leader = partitionInfo.leader();
          if (leader != null) {
            if (leader.id() == brokerId) {
              tenantPartitionsOnThisBroker.merge(tenant, 1, Integer::sum);
            }
            // we record brokers even for tenants that do not have quota, because those tenant
            // may be applied default tenant quota which may be not unlimited
            if (!brokersHostingLeaders.containsKey(tenant)) {
              brokersHostingLeaders.put(tenant, new HashSet<Integer>());
            }
            brokersHostingLeaders.get(tenant).add(leader.id());
          }
        }
      }
    }

    boolean updated = false;
    for (Map.Entry<String, Integer> entry : tenantPartitionsOnThisBroker.entrySet()) {
      String tenant = entry.getKey();
      TenantQuota tenantQuota = getOrCreateTenantQuota(tenant, defaultTenantQuota, false);
      int leaderPartitions = entry.getValue();
      int brokersWithLeaders = brokersHostingLeaders.getOrDefault(
          tenant, Collections.<Integer>emptySet()).size();
      updated |= tenantQuota.updatePartitions(leaderPartitions, brokersWithLeaders);
    }
    if (updated) {
      log.trace("Some tenant quotas have been updated, new quotas: {}", tenantQuotas);
    }
    return updated;
  }

  public Cluster cluster() {
    return cluster;
  }

  @Override
  public void close() {
    synchronized (INSTANCES) {
      INSTANCES.remove(brokerId);
    }
  }

  /**
   * Creates tenant quota if does not exist; and updates existing quotas if 'forceUpdate' flag is
   * set
   * @param tenant tenant name
   * @param clusterQuotaConfig cluster-wide tenant quota config
   * @param forceUpdate true if the cluster quota config comes from tenant metadata update rather
   *                    than from cluster metadata update
   * @return tenant quotas assigned to this broker
   */
  TenantQuota getOrCreateTenantQuota(String tenant,
                                     QuotaConfig clusterQuotaConfig,
                                     boolean forceUpdate) {
    TenantQuota tenantQuota = new TenantQuota(clusterQuotaConfig);
    TenantQuota prevQuota = tenantQuotas.putIfAbsent(tenant, tenantQuota);
    if (prevQuota != null) {
      tenantQuota = prevQuota;
      // if we just created quota for this tenant, the quota is already up to date
      // so only need to update on 'forceUpdate' if we updated the quota
      if (forceUpdate) {
        QuotaConfig prevQuotas = tenantQuota.updateClusterQuota(clusterQuotaConfig);
        for (ClientQuotaType quotaType : ClientQuotaType.values()) {
          if (prevQuotas.quota(quotaType) != tenantQuota.quotaLimit(quotaType)) {
            quotaResetPending.get(quotaType).getAndSet(true);
          }
        }
      }
    } else if (forceUpdate) {
      // first time quotas created, update the flags for all quota types
      for (ClientQuotaType quotaType : ClientQuotaType.values()) {
          quotaResetPending.get(quotaType).getAndSet(true);
      }
    }
    return tenantQuota;
  }

  /**
   * Update provisioned tenant quota configuration. This method is invoked when tenant
   * cluster quotas or default tenant cluster quota is updated. Recompute quotas for
   * all affected tenants.
   */
  private synchronized void updateTenantQuotas(Map<String, QuotaConfig> tenantClusterQuotas,
      QuotaConfig defaultTenantQuota) {
    this.defaultTenantQuota = defaultTenantQuota;
    tenantQuotas.keySet().removeIf(tenant -> !tenantClusterQuotas.containsKey(tenant));
    for (Map.Entry<String, QuotaConfig> entry : tenantClusterQuotas.entrySet()) {
      getOrCreateTenantQuota(entry.getKey(), entry.getValue(), true);
    }
    log.trace("Updated tenant quotas, new quotas: {}", tenantQuotas);
  }

  /**
   * Update provisioned tenant quota configuration and/or default tenant quota.
   * This method is invoked when tenant cluster quotas or default tenant cluster quota is updated.
   */
  public static void updateQuotas(Map<String, QuotaConfig> tenantQuotas,
                                  QuotaConfig defaultTenantQuota) {
    log.debug("Update quotas: tenantQuotas={} default={}", tenantQuotas, defaultTenantQuota);
    synchronized (INSTANCES) {
      INSTANCES.values()
          .forEach(callback -> callback.updateTenantQuotas(tenantQuotas, defaultTenantQuota));
    }
  }

  public static TenantPartitionAssignor partitionAssignor(Map<String, ?> configs) {
    int brokerId = intConfig(configs, KafkaConfig$.MODULE$.BrokerIdProp());
    TenantPartitionAssignor partitionAssignor = null;
    synchronized (INSTANCES) {
      TenantQuotaCallback quotaCallback = INSTANCES.get(brokerId);
      if (quotaCallback != null) {
        partitionAssignor = quotaCallback.partitionAssignor;
      } else {
        log.debug("Tenant quota callback not configured for broker {}", brokerId);
      }
    }
    return partitionAssignor;
  }

  // Used only in tests
  static void closeAll() {
    synchronized (INSTANCES) {
      while (!INSTANCES.isEmpty()) {
        INSTANCES.values().iterator().next().close();
      }
    }
  }

  private static String topicTenant(String topic) {
    if (TenantContext.isTenantPrefixed(topic)) {
      return TenantContext.extractTenant(topic);
    } else {
      return "";
    }
  }

  private static Map<String, String> tenantMetricTags(String tenant) {
    return Collections.singletonMap(TenantMetrics.TENANT_TAG, tenant);
  }

  private static int intConfig(Map<String, ?> configs, String configName) {
    Object configValue = configs.get(configName);
    if (configValue == null) {
      throw new ConfigException(configName + " is not set");
    }
    return Integer.parseInt(configValue.toString());
  }

  private static long loadPerTenantBrokerByteRateConfig(
      Map<String, ?> configs, String configName, Long defaultValue, Long minValue) {
    Object configValue = configs.get(configName);
    if (configValue == null && defaultValue != null) {
      return defaultValue;
    }
    if (configValue == null) {
      throw new ConfigException(configName + " is not set");
    }
    long maxPerTenantBrokerByteRate = Long.parseLong(configValue.toString());
    if (maxPerTenantBrokerByteRate < minValue) {
      throw new ConfigException(configName, maxPerTenantBrokerByteRate, "must be >= " + minValue);
    }
    return maxPerTenantBrokerByteRate;
  }

  class TenantQuota {
    // Cluster configs related to the tenant, accessed only with TenantQuotaCallback lock
    int leaderPartitions;   // Tenant partitions with this broker as leader
    int brokersWithLeaders; // Number of brokers that host tenant's leaders
    // Configured cluster-wide quota
    QuotaConfig clusterQuotaConfig;

    // Quotas for this broker
    volatile QuotaConfig brokerQuotas;

    public TenantQuota(QuotaConfig clusterQuotaConfig) {
      this.leaderPartitions = 0;
      this.brokersWithLeaders = 0;
      this.clusterQuotaConfig = clusterQuotaConfig;
      updateBrokerQuota();
    }

    /**
     * Recomputes tenant quota for this broker based on the provided leader partitions of
     * this tenant on this broker and the total number of tenant partitions.
     */
    boolean updatePartitions(int leaderPartitions, int brokersWithLeaders) {
      this.leaderPartitions = leaderPartitions;
      this.brokersWithLeaders = brokersWithLeaders;
      QuotaConfig oldBrokerQuotas = brokerQuotas;
      updateBrokerQuota();
      return !Objects.equals(oldBrokerQuotas, brokerQuotas);
    }

    /**
     * Recomputes tenant quota for this broker based on the new provisioned cluster quota config
     * provided.
     * @return previous broker quotas
     */
    QuotaConfig updateClusterQuota(QuotaConfig clusterQuotaConfig) {
      QuotaConfig oldBrokerQuotas = brokerQuotas;
      if (!clusterQuotaConfig.equals(this.clusterQuotaConfig)) {
        this.clusterQuotaConfig = clusterQuotaConfig;
        updateBrokerQuota();
      }
      return oldBrokerQuotas;
    }

    /**
     * Updates the quotas for this broker based on the configured provisioned cluster quota
     * for the tenant. If the provisioned cluster quota is not yet known (e.g. tenant quota has
     * not yet been refreshed on the broker), default tenant quota is used for the calculation.
     * Quota returned is always greater than zero to avoid excessive throttling of requests
     * received before cluster metadata or quota configs are refreshed.
     *
     * Cluster-wide tenant quota is divided equally among brokers that host tenant's partition
     * leaders. Per broker tenant quota is capped to a configured (or default) maximum, to make
     * sure that a load from single tenant cannot overload a single broker.
     *
     * <p>For a cluster with:
     *    nodes = `n`,
     *    tenant cluster quota = `c`,
     *    total tenant partitions = `p`,
     *    tenant partitions with this broker as leader = `l`
     *    number of brokers hosting tenant's partition leaders = `b`,
     *    maximum tenant quota that can be achieved on a single broker = `max_q`,
     *    a broker with no leaders is always assigned a minimum tenant quota, `min_q`
     * Tenant quota 'q` is computed as follows:
     *    q = l == 0 ? min_q : min(c/b, max_q)
     * </p><p>Scenarios:
     * <ul>
     *   <li>Typical case - tenant creates medium/large number of partitions, full quota achieved
     *   across cluster:
     *       l > 0, p > 0, n > 0, b >= c / max_q : q = c / b
     *   </li>
     *   <li>Tenant creates one (or a small number) of partitions, and this broker is the leader
     *       of one or more partitions. Full quota is not achieved.
     *       l > 0, p > 0, n > 0, b < c / max_q : q = max_q
     *   </li>
     *   <li>Tenant created, metadata not refreshed on this broker - quota divided equally
     *       amongst nodes until partitions are created and metadata is refreshed.
     *       l = 0, p = 0, n > 0 : q = min(c/n, max_q)
     *   </li>
     *   <li>Tenant creates partitions, but this broker is not currently
     *       the leader of any. To avoid excessive throttling if a request arrives before metadata
     *       is refreshed, quota for one partition is allocated to this broker. This is a very
     *       tiny timing window, so the additional quota shouldn't cause any issues.
     *       l = 0, p > n, n > 0 : q = min_q
     *   </li>
     *   <li>Tenant request arrives before cluster metadata is refreshed on the callback.
     *       `q` is calculated with number of  brokers with leaders from previous cluster
     *       metadata if available or min_q otherwise.
     *   </li>
     * </ul>
     * </p>
     * <p>Quota guarantees:
     * <ul>
     * <li>We always guarantee quota > 0 to avoid excessive throttling.</li>
     * <li>For small timing windows related to metadata refresh, the total cluster quota
     *     allocated across brokers may be higher than the total provisioned quota, but
     *     is expected to be adjusted very quickly since this window only appears due
     *     to request handling on different threads</li>
     * <li>For larger timing windows related to tenant quota refresh, we may allocate
     *     older or default tenant quotas until the refresh is processed, but we will not
     *     exceed the total (older/default) quota of the tenant across the cluster.</li>
     * <li>The maximum consume and produce quotas achievable per broker is configurable:
     *     confluent.quota.tenant.broker.max.producer.rate and
     *     confluent.quota.tenant.broker.max.consumer.rate, with default values 12.5MB/sec each
     *     </li>
     * <li>Brokers (with non-tenant principals) are allocated unlimited quotas and are never
     *     throttled.</li>
     * </ul>
     * </p>
     */
    void updateBrokerQuota() {
      Long produceQuota = null;
      if (clusterQuotaConfig.hasQuotaLimit(ClientQuotaType.PRODUCE)) {
        produceQuota = leaderPartitions == 0 ?
                       minPerTenantBrokerProducerRate :
                       Math.min(clusterQuotaConfig.equalQuotaPerBrokerOrUnlimited(
                                    ClientQuotaType.PRODUCE,
                                    brokersWithLeaders, minPerTenantBrokerProducerRate),
                                maxPerTenantBrokerProducerRate);
      }


      Long consumeQuota = null;
      if (clusterQuotaConfig.hasQuotaLimit(ClientQuotaType.FETCH)) {
        consumeQuota = leaderPartitions == 0 ?
                       minPerTenantBrokerConsumerRate :
                       Math.min(clusterQuotaConfig.equalQuotaPerBrokerOrUnlimited(
                                    ClientQuotaType.FETCH,
                                    brokersWithLeaders, minPerTenantBrokerConsumerRate),
                                maxPerTenantBrokerConsumerRate);
      }

      // TODO: More investigation is required to figure out the best way to
      // distribute request quota. In phase 1, we will allocate high request
      // quotas to reduce throttling based on request quotas.
      brokerQuotas = new QuotaConfig(produceQuota,
                                     consumeQuota,
                                     clusterQuotaConfig.quota(ClientQuotaType.REQUEST),
                                     QuotaConfig.UNLIMITED_QUOTA);
    }

    boolean hasQuotaLimit(ClientQuotaType quotaType) {
      return brokerQuotas.hasQuotaLimit(quotaType);
    }

    Double quotaLimit(ClientQuotaType quotaType) {
      return brokerQuotas.quota(quotaType);
    }

    @Override
    public String toString() {
      return "TenantQuota("
          + "brokersWithLeaders=" + brokersWithLeaders + ", "
          + "leaderPartitions=" + leaderPartitions + ", "
          + "clusterQuotaConfig=" + clusterQuotaConfig + ", "
          + "brokerQuotas=" + brokerQuotas + ")";
    }
  }
}

