/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.store;

import java.io.InputStream;

public interface TierObjectStoreResponse extends AutoCloseable {
    InputStream getInputStream();

    Long getStreamSize();
}
