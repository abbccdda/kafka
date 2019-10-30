/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.server.Defaults;
import kafka.server.KafkaConfig;

public class TierFetcherConfig {
    public final int numFetchThreads;
    public final int offsetCacheSize;
    public final int offsetCacheExpirationMs;
    public final int offsetCacheExpiryPeriodMs;

    public TierFetcherConfig(KafkaConfig config) {
        this.numFetchThreads = config.tierFetcherNumThreads();
        this.offsetCacheSize = config.tierFetcherOffsetCacheSize();
        this.offsetCacheExpirationMs = config.tierFetcherOffsetCacheExpirationMs();
        this.offsetCacheExpiryPeriodMs = config.tierFetcherOffsetCacheExpiryPeriodMs();
    }

    public TierFetcherConfig(int numFetchThreads, int offsetCacheSize,
                             int offsetCacheExpirationMs, int offsetCacheExpiryPeriodMs) {
        this.numFetchThreads = numFetchThreads;
        this.offsetCacheSize = offsetCacheSize;
        this.offsetCacheExpirationMs = offsetCacheExpirationMs;
        this.offsetCacheExpiryPeriodMs = offsetCacheExpiryPeriodMs;
    }

    public TierFetcherConfig() {
        this.numFetchThreads = Defaults.TierFetcherNumThreads();
        this.offsetCacheSize = Defaults.TierFetcherOffsetCacheSize();
        this.offsetCacheExpirationMs = Defaults.TierFetcherOffsetCacheExpirationMs();
        this.offsetCacheExpiryPeriodMs = Defaults.TierFetcherOffsetCacheExpiryPeriodMs();
    }
}
