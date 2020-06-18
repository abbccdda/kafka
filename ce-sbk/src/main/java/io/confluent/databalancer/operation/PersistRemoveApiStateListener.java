/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.operation;

import io.confluent.databalancer.persistence.ApiStatePersistenceStore;
import io.confluent.databalancer.persistence.BrokerRemovalStateRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of #{@link BrokerRemovalProgressListener} that
 * persists all state updates for a single broker removal operation to disk
 */
public class PersistRemoveApiStateListener implements BrokerRemovalProgressListener {

    private static final Logger LOG = LoggerFactory.getLogger(PersistRemoveApiStateListener.class);

    private final ApiStatePersistenceStore persistenceStore;

    public PersistRemoveApiStateListener(ApiStatePersistenceStore persistenceStore) {
        this.persistenceStore = persistenceStore;
    }

    @Override
    public void onProgressChanged(int brokerId, BrokerRemovalStateMachine.BrokerRemovalState state, Exception e) {
        BrokerRemovalStateRecord removalStateTracker = new BrokerRemovalStateRecord(brokerId, state, e);
        BrokerRemovalStateRecord existingRemovalStateRecord = persistenceStore.getBrokerRemovalStateRecord(brokerId);


        if (existingRemovalStateRecord != null) {
            removalStateTracker.setStartTime(existingRemovalStateRecord.startTime());
        }
        try {
            persistenceStore.save(removalStateTracker, existingRemovalStateRecord == null);
            LOG.info("Removal status for broker {} changed from {} to {}",
                    brokerId, existingRemovalStateRecord, removalStateTracker);
        } catch (InterruptedException ex) {
            LOG.error("Interrupted when broker removal state for broker: {}", brokerId, ex);
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }
    }
}
