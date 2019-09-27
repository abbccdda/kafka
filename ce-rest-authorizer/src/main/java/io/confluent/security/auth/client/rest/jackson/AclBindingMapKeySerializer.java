/*
 * Copyright 2019 Confluent Inc.
 */

package io.confluent.security.auth.client.rest.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.confluent.security.authorizer.jackson.KafkaModule;
import org.apache.kafka.common.acl.AclBinding;

import java.io.IOException;

public class AclBindingMapKeySerializer extends JsonSerializer<AclBinding> {
  static final ObjectMapper OBJECT_MAPPER;

  static {
    OBJECT_MAPPER = new ObjectMapper();
    OBJECT_MAPPER.registerModule(new KafkaModule());
  }

  @Override
  public void serialize(final AclBinding value,
                        final JsonGenerator gen,
                        final SerializerProvider serializers) throws IOException {
    gen.writeFieldName(OBJECT_MAPPER.writeValueAsString(value));
  }
}
