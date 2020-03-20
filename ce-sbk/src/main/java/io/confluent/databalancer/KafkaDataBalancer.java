/**
 * Copyright (C) 2020 Confluent Inc.
 */

package io.confluent.databalancer;

import kafka.controller.DataBalancer;
import kafka.server.KafkaConfig;

public class KafkaDataBalancer implements DataBalancer {
    private KafkaConfig kafkaConfig;

    public KafkaDataBalancer(KafkaConfig config) {
        kafkaConfig = config;
    }

    @Override
    public void startUp() {

    }

    @Override
    public void shutdown() {

    }
}

