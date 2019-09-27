/*
 * Copyright 2019 Confluent Inc.
 */

package io.confluent.security.auth.client.rest.jackson;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import org.apache.kafka.common.acl.AclBinding;

import java.io.IOException;

public class AclBindingMapKeyDeserializer extends KeyDeserializer {

  @Override
  public Object deserializeKey(final String key,
                               final DeserializationContext ctxt) throws IOException {
    return AclBindingMapKeySerializer.OBJECT_MAPPER.readValue(key,  AclBinding.class);
  }
}
