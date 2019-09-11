/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.security.audit.appender;

import com.google.common.collect.ImmutableMap;
import io.confluent.security.audit.serde.CloudEventProtoSerde;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class EventProducerDefaults {

  // ensure that our production is replicated
  static final String ACKS_CONFIG = "all";
  static final String COMPRESSION_TYPE_CONFIG = "lz4";
  static final String INTERCEPTOR_CLASSES_CONFIG = "";
  static final String KEY_SERIALIZER_CLASS_CONFIG = ByteArraySerializer.class.getName();
  static final String LINGER_MS_CONFIG = "500";
  // default is 0, we would like to keep trying if possible
  static final int RETRIES_CONFIG = 10;
  static final long RETRY_BACKOFF_MS_CONFIG = 500;
  static final String VALUE_SERIALIZER_CLASS_CONFIG = CloudEventProtoSerde.class.getName();

  public static final Map<String, Object> PRODUCER_CONFIG_DEFAULTS =
      ImmutableMap.<String, Object>builder()
          .put(ProducerConfig.ACKS_CONFIG, ACKS_CONFIG)
          .put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE_CONFIG)
          .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_CLASS_CONFIG)
          .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_CLASS_CONFIG)
          .put(ProducerConfig.LINGER_MS_CONFIG, LINGER_MS_CONFIG)
          .put(ProducerConfig.RETRIES_CONFIG, RETRIES_CONFIG)
          .put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, INTERCEPTOR_CLASSES_CONFIG)
          .put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, RETRY_BACKOFF_MS_CONFIG)
          .build();
}
