/*
 Copyright 2020 Confluent Inc.
 */

package kafka.tier.tools;

import kafka.server.KafkaConfig;
import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStoreConfig;
import kafka.tier.store.TierObjectStoreUtils;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TierObjectStoreFactoryTest {

    @Test
    public void testObjectStoreInstanceReference() {
        Properties props = new Properties();
        props.setProperty("cluster-id", "mock-cluster");
        props.put(KafkaConfig.BrokerIdProp(), 42);
        TierObjectStoreConfig mockConfig = TierObjectStoreUtils.generateBackendConfig(TierObjectStore.Backend.Mock, props);
        TierObjectStore objectStore = TierObjectStoreFactory.getObjectStoreInstance(TierObjectStore.Backend.Mock, mockConfig);
        TierObjectStore secondInstance = TierObjectStoreFactory.getObjectStoreInstance(TierObjectStore.Backend.Mock, mockConfig);
        assertEquals(objectStore, secondInstance);
        assertEquals(1, TierObjectStoreFactory.closeBackendInstance(TierObjectStore.Backend.Mock));
        assertEquals(0, TierObjectStoreFactory.closeBackendInstance(TierObjectStore.Backend.Mock));
        assertNotEquals(objectStore, TierObjectStoreFactory.getObjectStoreInstance(TierObjectStore.Backend.Mock, mockConfig));
        assertEquals(0, TierObjectStoreFactory.closeBackendInstance(TierObjectStore.Backend.Mock));
    }
}
