// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.multitenant.quota;

import java.util.EnumMap;
import org.apache.kafka.server.quota.ClientQuotaType;

public class QuotaConfig {

  static final double UNLIMITED_THROUGHPUT_QUOTA = Long.MAX_VALUE;
  static final double UNLIMITED_REQUEST_QUOTA = Integer.MAX_VALUE;

  public static final QuotaConfig UNLIMITED_QUOTA = new QuotaConfig(
      UNLIMITED_THROUGHPUT_QUOTA,
      UNLIMITED_THROUGHPUT_QUOTA,
      UNLIMITED_REQUEST_QUOTA);

  private final EnumMap<ClientQuotaType, Double> quotas = new EnumMap<>(ClientQuotaType.class);

  public QuotaConfig(Long produceQuota, Long fetchQuota, Double requestQuota,
                     QuotaConfig defaultQuota) {
    this(produceQuota == null ? defaultQuota.quota(ClientQuotaType.PRODUCE) : produceQuota,
         fetchQuota == null ? defaultQuota.quota(ClientQuotaType.FETCH) : fetchQuota,
         requestQuota == null ? defaultQuota.quota(ClientQuotaType.REQUEST) : requestQuota);
  }

  public Long equalQuotaPerBrokerOrUnlimited(ClientQuotaType quotaType,
                                             int numBrokers, Long quotaIfNoBrokers) {
    if (numBrokers <= 0) {
      return quotaIfNoBrokers;
    }
    Double quota = hasQuotaLimit(quotaType) ?
                   quota(quotaType) / numBrokers :
                   UNLIMITED_QUOTA.quota(quotaType);
    return quota.longValue();
  }

  private QuotaConfig(double produceQuota, double fetchQuota, double requestQuota) {
    if (produceQuota <= 0.0 || fetchQuota <= 0.0 || requestQuota <= 0.0) {
      throw new IllegalArgumentException(
          String.format("Invalid quota produce=%f fetch=%f request=%f",
              produceQuota, fetchQuota, requestQuota));
    }
    quotas.put(ClientQuotaType.PRODUCE, produceQuota);
    quotas.put(ClientQuotaType.FETCH, fetchQuota);
    quotas.put(ClientQuotaType.REQUEST, requestQuota);
  }

  public boolean hasQuotaLimit(ClientQuotaType quotaType) {
    return quota(quotaType) != UNLIMITED_QUOTA.quota(quotaType);
  }

  public double quota(ClientQuotaType quotaType) {
    return quotas.get(quotaType);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    QuotaConfig that = (QuotaConfig) o;
    return quotas.equals(that.quotas);
  }

  @Override
  public int hashCode() {
    return quotas.hashCode();
  }

  @Override
  public String toString() {
    return "QuotaConfig(quotas=" + quotas + ')';
  }
}
