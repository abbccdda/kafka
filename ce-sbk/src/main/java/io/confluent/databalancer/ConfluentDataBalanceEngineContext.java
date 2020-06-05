/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import io.confluent.databalancer.metrics.DataBalancerMetricsRegistry;
import io.confluent.databalancer.persistence.ApiStatePersistenceStore;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * An object that contains all information related to an active DataBalanceEngine.
 */
public class ConfluentDataBalanceEngineContext implements DataBalanceEngineContext {

    private static final Logger LOG = LoggerFactory.getLogger(ConfluentDataBalanceEngineContext.class);

    private final DataBalancerMetricsRegistry dataBalancerMetricsRegistry;
    private final Time time;

    private volatile KafkaCruiseControl cruiseControl;
    private volatile ApiStatePersistenceStore persistenceStore;

    public ConfluentDataBalanceEngineContext(DataBalancerMetricsRegistry dataBalancerMetricsRegistry,
                                             KafkaCruiseControl cruiseControl,
                                             Time time) {
        this.dataBalancerMetricsRegistry = Objects.requireNonNull(dataBalancerMetricsRegistry, "DataBalancerMetricsRegistry must be non-null");
        this.cruiseControl = cruiseControl;
        this.time = time;
    }

    public KafkaCruiseControl getCruiseControl() {
        return cruiseControl;
    }

    public boolean isCruiseControlInitialized() {
        return cruiseControl != null;
    }

    public void setCruiseControl(KafkaCruiseControl cruiseControl) {
        this.cruiseControl = cruiseControl;
    }

    private void closeAndClearCruiseControl() {
        closeQuietly(() -> {
            if (cruiseControl != null) cruiseControl.shutdown();
        });

        this.cruiseControl = null;
    }

    public Time getTime() {
        return time;
    }

    public DataBalancerMetricsRegistry getDataBalancerMetricsRegistry() {
        return dataBalancerMetricsRegistry;
    }

    public ApiStatePersistenceStore getPersistenceStore() {
        return persistenceStore;
    }

    public void setPersistenceStore(ApiStatePersistenceStore persistenceStore) {
        this.persistenceStore = persistenceStore;
    }

    private void closeAndClearPersistenceStore() {
        closeQuietly(persistenceStore);
        this.persistenceStore = null;
    }

    public void closeAndClearState() {
        this.closeAndClearCruiseControl();
        this.closeAndClearPersistenceStore();
        this.dataBalancerMetricsRegistry.clearShortLivedMetrics();
    }

    private void closeQuietly(AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Exception ex) {
            // Ignore
            LOG.debug("Error when closing resource.", ex);
        }
    }
}
