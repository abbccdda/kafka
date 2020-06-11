/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalFuture;
import io.confluent.databalancer.metrics.DataBalancerMetricsRegistry;
import io.confluent.databalancer.persistence.ApiStatePersistenceStore;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

/**
 * An object that contains all information related to an active DataBalanceEngine.
 */
public class ConfluentDataBalanceEngineContext implements DataBalanceEngineContext {

    private static final Logger LOG = LoggerFactory.getLogger(ConfluentDataBalanceEngineContext.class);

    private final DataBalancerMetricsRegistry dataBalancerMetricsRegistry;
    private final Time time;

    // package-private for testing
    final Map<Integer, BrokerRemovalFuture> brokerRemovalFutures;

    private volatile KafkaCruiseControl cruiseControl;
    private volatile ApiStatePersistenceStore persistenceStore;

    public ConfluentDataBalanceEngineContext(DataBalancerMetricsRegistry dataBalancerMetricsRegistry,
                                             KafkaCruiseControl cruiseControl,
                                             Time time) {
        this.dataBalancerMetricsRegistry = Objects.requireNonNull(dataBalancerMetricsRegistry, "DataBalancerMetricsRegistry must be non-null");
        this.cruiseControl = cruiseControl;
        this.time = time;
        this.brokerRemovalFutures = new ConcurrentHashMap<>();
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

    /**
     * Store a future of the broker removal operation in memory
     * @param brokerId the id of the broker
     * @param future a #{@link BrokerRemovalFuture} for the full broker removal operation of broker #{@code brokerId}
     */
    public void putBrokerRemovalFuture(int brokerId, BrokerRemovalFuture future) {
        brokerRemovalFutures.put(brokerId, future);
    }

    /**
     * Clean up a stored (in memory) future of the broker removal operation
     */
    public void removeBrokerRemovalFuture(int brokerId) {
        brokerRemovalFutures.remove(brokerId);
    }

    /**
     * @return nullable, a #{@link Future<Future>} for the full broker removal operation of broker #{@code brokerId} -
     * the plan computation/shutdown operation and then the underlying reassignments execution
     */
    public BrokerRemovalFuture brokerRemovalFuture(int brokerId) {
        return brokerRemovalFutures.get(brokerId);
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
