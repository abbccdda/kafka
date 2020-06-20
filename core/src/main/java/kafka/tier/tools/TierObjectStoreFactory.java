/*
 Copyright 2020 Confluent Inc.
 */

package kafka.tier.tools;

import kafka.tier.store.MockInMemoryTierObjectStore;
import kafka.tier.store.S3TierObjectStore;
import kafka.tier.store.S3TierObjectStoreConfig;
import kafka.tier.store.GcsTierObjectStore;
import kafka.tier.store.GcsTierObjectStoreConfig;
import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStoreConfig;
import kafka.tier.store.TierObjectStore.Backend;

import java.util.HashMap;
import java.util.Map;

/**
 * This class will act as the factory for generating and maintaining the instance map for the various kinds
 * of the object store backends.
 * For use in tests, please call closeBackendInstance() in the @After method for cleaning up the static instances
 * Currently we support S3 and Mock backends.
 */
public class TierObjectStoreFactory {
    /**
     * This map will enable reuse of backend tierstore instances across requests
     * The access methods are synchronized because of the singleton access pattern
     */
    private static final Map<Backend, ReferenceCountedObjStore> BACKEND_INSTANCE_MAP = new HashMap<Backend, ReferenceCountedObjStore>() {{
        put(Backend.S3, new ReferenceCountedObjStore());
        put(Backend.GCS, new ReferenceCountedObjStore());
        put(Backend.Mock, new ReferenceCountedObjStore());
    }};

    /**
     * This method will instantiate the necessary backend store instance and then return the same instance for
     * consecutive requests.
     * Note: Users can use TierObjectStoreUtils.generateBackendConfig for creating the necessary config
     * The synchronization is necessary for the singleton pattern.
     * This method also throws IllegalArgumentException for various illegal/unsupported backend args
     *
     * @param backend The backend enum for which an instance is required
     * @param config The object store config. Can use TierObjectStoreUtils.generateBackendConfig for generation
     * @return The TierObjectStore instance
     */
    synchronized static TierObjectStore getObjectStoreInstance(Backend backend, TierObjectStoreConfig config) {
        if (!BACKEND_INSTANCE_MAP.containsKey(backend)) {
            throw new IllegalArgumentException("Unsupported backend: " + backend);
        }
        return BACKEND_INSTANCE_MAP.get(backend).getObjectStore(backend, config);
    }

    /**
     * This method will clear the corresponding singleton instance in the instance map as well as close the existing
     * instance, if any.
     * Please remember to call this method in the @After of unit tests.
     *
     * @param backend the backend type that needs to be cleaned
     */
    synchronized static int closeBackendInstance(Backend backend) {
        if (!BACKEND_INSTANCE_MAP.containsKey(backend)) {
            throw new IllegalArgumentException("Unsupported backend: " + backend);
        }
        return BACKEND_INSTANCE_MAP.get(backend).close();
    }

    synchronized static TierObjectStore maybeInitObjStore(Backend backend, TierObjectStoreConfig config) {
        switch (backend) {
            case S3:
                return maybeInitS3Store(config);
            case GCS:
                return maybeInitGcsStore(config);
            case Mock:
                return maybeInitMockStore(config);
            default:
                throw new IllegalArgumentException("Unsupported backend: " + backend);
        }
    }

    private static S3TierObjectStore maybeInitS3Store(TierObjectStoreConfig config) {
        if (config instanceof S3TierObjectStoreConfig) {
            return new S3TierObjectStore((S3TierObjectStoreConfig) config);
        } else {
            throw new IllegalArgumentException("Expected S3TierObjectStoreConfig but received instance of: " + config.getClass());
        }
    }

    private static GcsTierObjectStore maybeInitGcsStore(TierObjectStoreConfig config) {
        if (config instanceof GcsTierObjectStoreConfig) {
            return new GcsTierObjectStore((GcsTierObjectStoreConfig) config);
        } else {
            throw new IllegalArgumentException("Expected GcsTierObjectStoreConfig but received instance of: " + config.getClass());
        }
    }

    private static MockInMemoryTierObjectStore maybeInitMockStore(TierObjectStoreConfig config) {
        return new MockInMemoryTierObjectStore(config);
    }

    private static class ReferenceCountedObjStore {
        private TierObjectStore objectStore;
        private int refCount;

        ReferenceCountedObjStore() {
            this.objectStore = null;
            this.refCount = 0;
        }
        
        synchronized int close() {
            if (refCount > 0) {
                refCount -= 1;
                if (refCount == 0 && objectStore != null) {
                    objectStore.close();
                    objectStore = null;
                }
            }
            return refCount;
        }
        
        synchronized TierObjectStore getObjectStore(Backend backend, TierObjectStoreConfig config) {
            if (refCount < 0) {
                throw new IllegalStateException("Incorrect state in ReferenceCountedObjStore for backend: " + backend +
                        " at refCount: " + refCount);
            } else if (refCount == 0) {
                objectStore = maybeInitObjStore(backend, config);
            }
            refCount += 1;
            return objectStore;
        }
    }
}
