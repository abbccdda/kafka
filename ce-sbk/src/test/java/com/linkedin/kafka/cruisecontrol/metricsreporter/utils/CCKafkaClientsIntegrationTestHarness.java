/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.test.TestSslUtils;


public abstract class CCKafkaClientsIntegrationTestHarness extends CCKafkaIntegrationTestHarness {

  @Override
  public void setUp() {
    super.setUp();
  }

  protected Producer<String, String> createProducer(Properties overrides) {
    Properties props = getProducerProperties(overrides);
    return new KafkaProducer<>(props);
  }

  protected void produceData(String topic, int bytesToProduce) throws ExecutionException, InterruptedException {
    List<Future<RecordMetadata>> produceFutures = new ArrayList<>();
    int bytesProduced = 0;

    try (Producer<String, String> producer = CCKafkaTestUtils.producerFor(_brokers.get(0))) {
      String payload = "DEADBEEF";
      int bytesPerMsg = payload.getBytes(StandardCharsets.UTF_8).length;

      while (bytesProduced < bytesToProduce) {
        produceFutures.add(producer.send(new ProducerRecord<>(topic, payload)));
        bytesProduced += bytesPerMsg;
      }
      producer.flush();

      for (Future<RecordMetadata> future : produceFutures) {
        future.get();
      }
    }
  }

  protected Properties getProducerProperties(Properties overrides) {
    Properties result = new Properties();

    //populate defaults
    result.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    result.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
    result.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());

    setSecurityConfigs(result, "producer");

    //apply overrides
    if (overrides != null) {
      result.putAll(overrides);
    }

    return result;
  }

  protected void setSecurityConfigs(Properties clientProps, String certAlias) {
    SecurityProtocol protocol = securityProtocol();
    if (protocol == SecurityProtocol.SSL) {
      File trustStoreFile = trustStoreFile();
      if (trustStoreFile == null) {
        throw new AssertionError("ssl set but no trust store provided");
      }
      clientProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol.name);
      clientProps.setProperty(KafkaConfig.SslEndpointIdentificationAlgorithmProp(), "");
      try {
        clientProps.putAll(TestSslUtils.createSslConfig(true, true, Mode.CLIENT, trustStoreFile, certAlias));
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }
}