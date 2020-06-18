/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer;

import kafka.server.KafkaConfig;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class EngineInitializationContext {
  final KafkaConfig kafkaConfig;
  final Map<Integer, Long> brokerEpochs;
  final Function<Integer, AtomicReference<String>> brokerRemovalStateMetricRegistrationHandler;

  /**
   *
   * @param kafkaConfig - the Kafka config
   * @param brokerEpochs - a map consisting of broker ID and the corresponding broker's epoch at the time of initialization
   * @param brokerRemovalStateMetricRegistrationHandler - a function that takes in a broker ID as a parameter
   *                                                    and registers a metric that tracks the broker removal state
   */
  public EngineInitializationContext(KafkaConfig kafkaConfig, Map<Integer, Long> brokerEpochs,
                                     Function<Integer, AtomicReference<String>> brokerRemovalStateMetricRegistrationHandler) {
    this.kafkaConfig = kafkaConfig;
    this.brokerEpochs = brokerEpochs;
    this.brokerRemovalStateMetricRegistrationHandler = brokerRemovalStateMetricRegistrationHandler;
  }
}
