/**
 * Copyright (C) 2020 Confluent Inc.
 */

package io.confluent.databalancer;

import kafka.server.KafkaConfig;

/**
 * Interface to expose CruiseControl operations to users of that functionality (the KafkaDataBalanceManager). This is a limited subset of all the
 * interfaces that CruiseControl actually exposes, most of which aren't needed for the DataBalanceManager. The intent is that implementors
 * of this class will take care of managing synchronization and computation resources for CruiseControl and the methods exposed
 * here map 1:1 to the underlying CruiseControl operations.
 */
public interface DataBalanceEngine {
    void startUp(KafkaConfig kafkaConfig);

    void shutdown();

    void updateThrottle(Long newThrottle);

    boolean isActive();
}
