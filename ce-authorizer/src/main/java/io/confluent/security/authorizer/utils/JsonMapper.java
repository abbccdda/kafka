// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.ResourceType;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;

public class JsonMapper {
  private static final ObjectMapper OBJECT_MAPPER;

  static {
    OBJECT_MAPPER = new ObjectMapper();
    OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
    OBJECT_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    OBJECT_MAPPER.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
    OBJECT_MAPPER.registerModule(new Jdk8Module());
    OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

    SimpleModule simpleModule = new SimpleModule();
    simpleModule.addSerializer(KafkaPrincipal.class, new PrincipalSerializer(KafkaPrincipal.class));
    simpleModule.addDeserializer(KafkaPrincipal.class, new PrincipalDeserializer(KafkaPrincipal.class));

    // ResourceTypes, Operations are Strings rather than enums for extensibility. Serialize/Deserialize as String.
    simpleModule.addSerializer(ResourceType.class, new ResourceTypeSerializer(ResourceType.class));
    simpleModule.addDeserializer(ResourceType.class, new ResourceTypeDeserializer(ResourceType.class));

    simpleModule.addSerializer(Operation.class, new OperationSerializer(Operation.class));
    simpleModule.addDeserializer(Operation.class, new OperationDeserializer(Operation.class));

    OBJECT_MAPPER.registerModule(simpleModule);
  }

  public static ObjectMapper objectMapper() {
        return OBJECT_MAPPER;
  }

  public static byte[] toByteArray(Object obj) {
    try {
      return OBJECT_MAPPER.writeValueAsBytes(obj);
    } catch (IOException e) {
      throw new IllegalArgumentException("JSON serialization failed for: " + obj, e);
    }
  }

  public static ByteBuffer toByteBuffer(Object obj) {
    try {
      return ByteBuffer.wrap(OBJECT_MAPPER.writeValueAsBytes(obj));
    } catch (IOException e) {
      throw new IllegalArgumentException("JSON serialization failed for: " + obj, e);
    }
  }

  public static <T> T fromByteBuffer(ByteBuffer buffer, Class<T> clazz) {
    try {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      return OBJECT_MAPPER.readValue(bytes, clazz);
    } catch (IOException e) {
      throw new IllegalArgumentException("JSON deserialization failed for object of class " + clazz, e);
    }
  }

  private static class PrincipalSerializer extends StdSerializer<KafkaPrincipal> {

    public PrincipalSerializer(Class<KafkaPrincipal> t) {
      super(t);
    }

    @Override
    public void serialize(KafkaPrincipal principal, JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider) throws IOException {
      jsonGenerator.writeString(principal.toString());
    }
  }

  private static class PrincipalDeserializer extends StdDeserializer<KafkaPrincipal> {

    public PrincipalDeserializer(Class<KafkaPrincipal> t) {
      super(t);
    }

    @Override
    public KafkaPrincipal deserialize(JsonParser jsonParser,
        DeserializationContext deserializationContext) throws IOException {
      return SecurityUtils.parseKafkaPrincipal(jsonParser.getValueAsString());
    }
  }

  private static class ResourceTypeSerializer extends StdSerializer<ResourceType> {

    public ResourceTypeSerializer(Class<ResourceType> t) {
      super(t);
    }

    @Override
    public void serialize(ResourceType resourceType, JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider) throws IOException {
      jsonGenerator.writeString(resourceType.name());
    }
  }

  private static class ResourceTypeDeserializer extends StdDeserializer<ResourceType> {

    public ResourceTypeDeserializer(Class<ResourceType> t) {
      super(t);
    }

    @Override
    public ResourceType deserialize(JsonParser jsonParser,
        DeserializationContext deserializationContext) throws IOException {
      return new ResourceType(jsonParser.getValueAsString());
    }
  }

  private static class OperationSerializer extends StdSerializer<Operation> {

    public OperationSerializer(Class<Operation> t) {
      super(t);
    }

    @Override
    public void serialize(Operation operation, JsonGenerator jsonGenerator,
                          SerializerProvider serializerProvider) throws IOException {
      jsonGenerator.writeString(operation.name());
    }
  }

  private static class OperationDeserializer extends StdDeserializer<Operation> {

    public OperationDeserializer(Class<Operation> t) {
      super(t);
    }

    @Override
    public Operation deserialize(JsonParser jsonParser,
                                    DeserializationContext deserializationContext) throws IOException {
      return new Operation(jsonParser.getValueAsString());
    }
  }
}
