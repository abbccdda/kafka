/*
 * Copyright 2019 Confluent Inc.
 */

package io.confluent.security.auth.client.rest.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.kafka.common.acl.AclBindingFilter;

import java.io.IOException;

public class AclBindingFilterMapKeySerializer extends JsonSerializer<AclBindingFilter> {

  @Override
  public void serialize(final AclBindingFilter value,
                        final JsonGenerator gen,
                        final SerializerProvider serializers) throws IOException {
    gen.writeFieldName(AclBindingMapKeySerializer.OBJECT_MAPPER.writeValueAsString(value));
  }
}
