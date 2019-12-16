/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.security.audit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import io.cloudevents.CloudEvent;
import io.cloudevents.json.ZonedDateTimeDeserializer;
import io.cloudevents.json.ZonedDateTimeSerializer;
import io.confluent.events.CloudEventUtils;
import io.confluent.events.EventLoggerConfig;
import io.confluent.events.ProtobufEvent;
import io.confluent.events.ProtobufEvent.Builder;
import java.time.ZonedDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.Test;

public class AuditLogProtobufToJsonTest {

  private Builder simpleEventBuilder = ProtobufEvent.newBuilder()
      .setType("io.confluent.kafka.server/authorization")
      .setSource("crn://confluent.cloud/kafka=lkc-ld9rz")
      .setSubject("crn://confluent.cloud/kafka=lkc-ld9rz/topic=my_new_topic")
      .setId("e7872058-f971-496c-8a14-e6b0196c7ce")
      .setEncoding(EventLoggerConfig.DEFAULT_CLOUD_EVENT_ENCODING_CONFIG)
      .setData(AuditLogEntry.newBuilder().build());

  private Pattern timestampPattern = Pattern.compile("\"(2020.*?)\"");

  @Test
  public void testTimePrecisionNone() {
    CloudEvent message = simpleEventBuilder
        .setTime(ZonedDateTime.parse("2020-04-05T17:31:00Z"))
        .build();
    Matcher matcher = timestampPattern.matcher(CloudEventUtils.toJsonString(message));
    assertTrue(matcher.find());
    assertEquals("2020-04-05T17:31:00.000Z", matcher.group(1));
  }

  @Test
  public void testTimePrecision000() {
    CloudEvent message = simpleEventBuilder
        .setTime(ZonedDateTime.parse("2020-04-05T17:31:00.000Z"))
        .build();
    Matcher matcher = timestampPattern.matcher(CloudEventUtils.toJsonString(message));
    assertTrue(matcher.find());
    assertEquals("2020-04-05T17:31:00.000Z", matcher.group(1));
  }

  @Test
  public void testTimePrecision1() {
    CloudEvent message = simpleEventBuilder
        .setTime(ZonedDateTime.parse("2020-04-05T17:31:00.1Z"))
        .build();
    Matcher matcher = timestampPattern.matcher(CloudEventUtils.toJsonString(message));
    assertTrue(matcher.find());
    assertEquals("2020-04-05T17:31:00.100Z", matcher.group(1));
  }

  @Test
  public void testTimePrecision100() {
    CloudEvent message = simpleEventBuilder
        .setTime(ZonedDateTime.parse("2020-04-05T17:31:00.100Z"))
        .build();
    Matcher matcher = timestampPattern.matcher(CloudEventUtils.toJsonString(message));
    assertTrue(matcher.find());
    assertEquals("2020-04-05T17:31:00.100Z", matcher.group(1));
  }

  @Test
  public void testTimePrecision001() {
    CloudEvent message = simpleEventBuilder
        .setTime(ZonedDateTime.parse("2020-04-05T17:31:00.001Z"))
        .build();
    Matcher matcher = timestampPattern.matcher(CloudEventUtils.toJsonString(message));
    assertTrue(matcher.find());
    assertEquals("2020-04-05T17:31:00.001Z", matcher.group(1));
  }

  @Test
  public void testTimePrecisionTruncates() {
    CloudEvent message = simpleEventBuilder
        .setTime(ZonedDateTime.parse("2020-04-05T17:31:00.00123Z"))
        .build();
    Matcher matcher = timestampPattern.matcher(CloudEventUtils.toJsonString(message));
    assertTrue(matcher.find());
    assertEquals("2020-04-05T17:31:00.001Z", matcher.group(1));
  }

  @Test
  public void testTimePrecisionTruncatesDown() {
    CloudEvent message = simpleEventBuilder
        .setTime(ZonedDateTime.parse("2020-04-05T17:31:00.001999999Z"))
        .build();
    Matcher matcher = timestampPattern.matcher(CloudEventUtils.toJsonString(message));
    assertTrue(matcher.find());
    assertEquals("2020-04-05T17:31:00.001Z", matcher.group(1));
  }

  @Test
  public void testCloudTopicCreateToJSON() throws Exception {

    AuditLogEntry ale = AuditLogEntry.newBuilder()
        .setServiceName("crn://confluent.cloud/kafka=lkc-ld9rz")
        .setMethodName("CreateTopics")
        .setResourceName("crn://confluent.cloud/kafka=lkc-ld9rz/topic=my_new_topic")
        .setAuthenticationInfo(
            AuthenticationInfo.newBuilder()
                .setPrincipal("/users/123").build())
        .setAuthorizationInfo(
            AuthorizationInfo.newBuilder()
                .setResourceType("Topic")
                .setResourceName("my_new_topic")
                .setOperation("Create")
                .setPatternType("Literal")
                .setAclAuthorization(
                    AclAuthorizationInfo.newBuilder()
                        .setHost("vPeOCWypqUOSepEvx0cbog")
                        .setPermissionType("Allow")
                        .build()
                )
                .setGranted(true)
                .build()
        )
        .setRequest(Struct.newBuilder()
            .putFields("requestType",
                Value.newBuilder().setStringValue("CreateTopics").build())
            .putFields("resourceType",
                Value.newBuilder().setStringValue("Topic").build())
            .putFields("resourceName",
                Value.newBuilder().setStringValue("my_new_topic").build())
            .putFields("correlationId",
                Value.newBuilder().setNumberValue(12345).build())
            .putFields("clientId",
                Value.newBuilder().setStringValue("userSupplied").build())
            .build())
        .setRequestMetadata(Struct.newBuilder()
            .putFields("callerIp",
                Value.newBuilder().setStringValue("192.168.1.23").build())
            .build())
        .build();

    CloudEvent message = ProtobufEvent.newBuilder()
        .setType("io.confluent.kafka.server/authorization")
        .setSource("crn://confluent.cloud/kafka=lkc-ld9rz")
        .setSubject("crn://confluent.cloud/kafka=lkc-ld9rz/topic=my_new_topic")
        .setId("e7872058-f971-496c-8a14-e6b0196c7ce")
        .setTime(ZonedDateTime.parse("2020-04-05T17:31:00Z"))
        .setEncoding(EventLoggerConfig.DEFAULT_CLOUD_EVENT_ENCODING_CONFIG)
        .setData(ale)
        .build();

    String jsonString = CloudEventUtils.toJsonString(message);

    /*
{
    "data": {
        "serviceName": "crn://confluent.cloud/kafka=lkc-ld9rz",
        "methodName": "CreateTopics",
        "resourceName": "crn://confluent.cloud/kafka=lkc-ld9rz/topic=my_new_topic",
        "authenticationInfo": {
            "principal": "/users/123"
        },
        "authorizationInfo": {
            "granted": true,
            "operation": "Create",
            "resourceType": "Topic",
            "resourceName": "my_new_topic",
            "patternType": "Literal",
            "aclAuthorization": {
                "permissionType": "Allow",
                "host": "vPeOCWypqUOSepEvx0cbog"
            }
        },
        "request": {
            "requestType": "CreateTopics",
            "resourceType": "Topic",
            "resourceName": "my_new_topic",
            "correlationId": 12345.0,
            "clientId": "userSupplied"
        },
        "requestMetadata": {
            "callerIp": "192.168.1.23"
        }
    },
    "id": "e7872058-f971-496c-8a14-e6b0196c7ce",
    "source": "crn://confluent.cloud/kafka=lkc-ld9rz",
    "specversion": "0.3",
    "type": "io.confluent.kafka.server/authorization",
    "time": "2020-04-05T17:31:00Z",
    "datacontenttype": "application/json",
    "subject": "crn://confluent.cloud/kafka=lkc-ld9rz/topic=my_new_topic"
}

     */

    assertEquals(
        "{\"data\":{\"serviceName\":\"crn://confluent.cloud/kafka=lkc-ld9rz\",\"methodName\":\"CreateTopics\",\"resourceName\":\"crn://confluent.cloud/kafka=lkc-ld9rz/topic=my_new_topic\",\"authenticationInfo\":{\"principal\":\"/users/123\"},\"authorizationInfo\":{\"granted\":true,\"operation\":\"Create\",\"resourceType\":\"Topic\",\"resourceName\":\"my_new_topic\",\"patternType\":\"Literal\",\"aclAuthorization\":{\"permissionType\":\"Allow\",\"host\":\"vPeOCWypqUOSepEvx0cbog\"}},\"request\":{\"requestType\":\"CreateTopics\",\"resourceType\":\"Topic\",\"resourceName\":\"my_new_topic\",\"correlationId\":12345.0,\"clientId\":\"userSupplied\"},\"requestMetadata\":{\"callerIp\":\"192.168.1.23\"}},\"id\":\"e7872058-f971-496c-8a14-e6b0196c7ce\",\"source\":\"crn://confluent.cloud/kafka=lkc-ld9rz\",\"specversion\":\"0.3\",\"type\":\"io.confluent.kafka.server/authorization\",\"time\":\"2020-04-05T17:31:00.000Z\",\"datacontenttype\":\"application/json\",\"subject\":\"crn://confluent.cloud/kafka=lkc-ld9rz/topic=my_new_topic\"}",
        jsonString);

  }

  @Test
  public void testOnpremTopicCreateToJSON() throws Exception {

    AuditLogEntry ale = AuditLogEntry.newBuilder()
        .setServiceName("crn://mds.example.com/kafka=vPeOCWypqUOSepEvx0cbog")
        .setMethodName("CreateTopics")
        .setResourceName("crn://mds.example.com/kafka=vPeOCWypqUOSepEvx0cbog/topic=my_new_topic")
        .setAuthenticationInfo(
            AuthenticationInfo.newBuilder()
                .setPrincipal("User:Alice").build())
        .setAuthorizationInfo(
            AuthorizationInfo.newBuilder()
                .setResourceType("Topic")
                .setResourceName("my_new_topic")
                .setOperation("Create")
                .setPatternType("Literal")
                .setRbacAuthorization(
                    RbacAuthorizationInfo.newBuilder()
                        .setRole("ResourceOwner")
                        .setScope(
                            AuthorizationScope.newBuilder()
                                .addOuterScope("myorg")
                                .addOuterScope("myenv")
                                .putClusters("kafka-cluster", "vPeOCWypqUOSepEvx0cbog")
                                .build()
                        )
                        .build()
                )
                .setGranted(true)
                .build()
        )
        .setRequest(Struct.newBuilder()
            .putFields("requestType",
                Value.newBuilder().setStringValue("CreateTopics").build())
            .putFields("resourceType",
                Value.newBuilder().setStringValue("Topic").build())
            .putFields("resourceName",
                Value.newBuilder().setStringValue("my_new_topic").build())
            .putFields("correlationId",
                Value.newBuilder().setNumberValue(12345).build())
            .putFields("clientId",
                Value.newBuilder().setStringValue("userSupplied").build())
            .build())
        .setRequestMetadata(Struct.newBuilder()
            .putFields("callerIp",
                Value.newBuilder().setStringValue("192.168.1.23").build())
            .build())
        .build();

    CloudEvent message = ProtobufEvent.newBuilder()
        .setType("io.confluent.kafka.server/authorization")
        .setSource("crn://mds.example.com/kafka=vPeOCWypqUOSepEvx0cbog")
        .setSubject("crn://mds.example.com/kafka=vPeOCWypqUOSepEvx0cbog/topic=my_new_topic")
        .setId("e7872058-f971-496c-8a14-e6b0196c7ce")
        .setTime(ZonedDateTime.parse("2020-04-05T17:31:00Z"))
        .setEncoding(EventLoggerConfig.DEFAULT_CLOUD_EVENT_ENCODING_CONFIG)
        .setData(ale)
        .build();

    String jsonString = CloudEventUtils.toJsonString(message);

    /*

{
  "data": {
    "serviceName": "crn://mds.example.com/kafka=vPeOCWypqUOSepEvx0cbog",
    "methodName": "CreateTopics",
    "resourceName": "crn://mds.example.com/kafka=vPeOCWypqUOSepEvx0cbog/topic=my_new_topic",
    "authenticationInfo": {
      "principal": "User:Alice"
    },
    "authorizationInfo": {
      "granted": true,
      "operation": "Create",
      "resourceType": "Topic",
      "resourceName": "my_new_topic",
      "patternType": "Literal",
      "rbacAuthorization": {
        "role": "ResourceOwner",
        "scope": {
          "outerScope": [
            "myorg",
            "myenv"
          ],
          "clusters": {
            "kafka-cluster": "vPeOCWypqUOSepEvx0cbog"
          }
        }
      }
    },
    "request": {
      "requestType": "CreateTopics",
      "resourceType": "Topic",
      "resourceName": "my_new_topic",
      "correlationId": 12345.0,
      "clientId": "userSupplied"
    },
    "requestMetadata": {
      "callerIp": "192.168.1.23"
    }
  },
  "id": "e7872058-f971-496c-8a14-e6b0196c7ce",
  "source": "crn://mds.example.com/kafka=vPeOCWypqUOSepEvx0cbog",
  "specversion": "0.3",
  "type": "io.confluent.kafka.server/authorization",
  "time": "2020-04-05T17:31:00Z",
  "datacontenttype": "application/json",
  "subject": "crn://mds.example.com/kafka=vPeOCWypqUOSepEvx0cbog/topic=my_new_topic"
}

     */

    assertEquals(
        "{\"data\":{\"serviceName\":\"crn://mds.example.com/kafka=vPeOCWypqUOSepEvx0cbog\",\"methodName\":\"CreateTopics\",\"resourceName\":\"crn://mds.example.com/kafka=vPeOCWypqUOSepEvx0cbog/topic=my_new_topic\",\"authenticationInfo\":{\"principal\":\"User:Alice\"},\"authorizationInfo\":{\"granted\":true,\"operation\":\"Create\",\"resourceType\":\"Topic\",\"resourceName\":\"my_new_topic\",\"patternType\":\"Literal\",\"rbacAuthorization\":{\"role\":\"ResourceOwner\",\"scope\":{\"outerScope\":[\"myorg\",\"myenv\"],\"clusters\":{\"kafka-cluster\":\"vPeOCWypqUOSepEvx0cbog\"}}}},\"request\":{\"requestType\":\"CreateTopics\",\"resourceType\":\"Topic\",\"resourceName\":\"my_new_topic\",\"correlationId\":12345.0,\"clientId\":\"userSupplied\"},\"requestMetadata\":{\"callerIp\":\"192.168.1.23\"}},\"id\":\"e7872058-f971-496c-8a14-e6b0196c7ce\",\"source\":\"crn://mds.example.com/kafka=vPeOCWypqUOSepEvx0cbog\",\"specversion\":\"0.3\",\"type\":\"io.confluent.kafka.server/authorization\",\"time\":\"2020-04-05T17:31:00.000Z\",\"datacontenttype\":\"application/json\",\"subject\":\"crn://mds.example.com/kafka=vPeOCWypqUOSepEvx0cbog/topic=my_new_topic\"}",
        jsonString);

  }

  @Test
  public void testOnpremTopicCreateToJSONGrantedFalse() throws Exception {
    AuditLogEntry ale = AuditLogEntry.newBuilder()
        .setServiceName("/clusters/vPeOCWypqUOSepEvx0cbog")
        .setMethodName("CreateTopics")
        .setResourceName("/clusters/vPeOCWypqUOSepEvx0cbog/topic/my_new_topic")
        .setAuthenticationInfo(
            AuthenticationInfo.newBuilder()
                .setPrincipal("User:Alice").build())
        .setAuthorizationInfo(
            AuthorizationInfo.newBuilder()
                .setResourceType("Topic")
                .setResourceName("my_new_topic")
                .setOperation("Create")
                .setPatternType("Literal")
                .setRbacAuthorization(
                    RbacAuthorizationInfo.newBuilder()
                        .setRole("ResourceOwner")
                        .setScope(
                            AuthorizationScope.newBuilder()
                                .addOuterScope("myorg")
                                .addOuterScope("myenv")
                                .putClusters("kafka-cluster", "vPeOCWypqUOSepEvx0cbog")
                                .build()
                        )
                        .build()
                )
                .setGranted(false)
                .build()
        )
        .setRequest(Struct.newBuilder()
            .putFields("requestType",
                Value.newBuilder().setStringValue("CreateTopics").build())
            .putFields("resourceType",
                Value.newBuilder().setStringValue("Topic").build())
            .putFields("resourceName",
                Value.newBuilder().setStringValue("my_new_topic").build())
            .putFields("correlationId",
                Value.newBuilder().setNumberValue(12345).build())
            .putFields("clientId",
                Value.newBuilder().setStringValue("userSupplied").build())
            .build())
        .setRequestMetadata(Struct.newBuilder()
            .putFields("callerIp",
                Value.newBuilder().setStringValue("192.168.1.23").build())
            .build())
        .build();

    CloudEvent message = ProtobufEvent.newBuilder()
        .setType("io.confluent.kafka.server/authorization")
        .setSource("/clusters/vPeOCWypqUOSepEvx0cbog")
        .setSubject("/clusters/vPeOCWypqUOSepEvx0cbog/topic/my_new_topic")
        .setId("e7872058-f971-496c-8a14-e6b0196c7ce")
        .setTime(ZonedDateTime.parse("2020-04-05T17:31:00Z"))
        .setEncoding(EventLoggerConfig.DEFAULT_CLOUD_EVENT_ENCODING_CONFIG)
        .setData(ale)
        .build();

    String jsonString = CloudEventUtils.toJsonString(message);

    assertTrue(jsonString.contains("\"granted\":false"));
  }

  // Test to replicate https://confluentinc.atlassian.net/browse/CPKAFKA-3888
  @Test(expected = InvalidDefinitionException.class)
  public void testJsonErrorWhenProtobufModuleIsNotRegistered() throws Exception {
    AuditLogEntry ale = AuditLogEntry.newBuilder()
        .setServiceName("crn:///kafka=CdiHxnm2SwGtUg5nnB8rBQ")
        .setMethodName("kafka.Metadata")
        .setResourceName("crn:///kafka=CdiHxnm2SwGtUg5nnB8rBQ/topic=_confluent-metadata-auth")
        .setAuthenticationInfo(
            AuthenticationInfo.newBuilder()
                .setPrincipal("User:ANONYMOUS").build())
        .setAuthorizationInfo(
            AuthorizationInfo.newBuilder()
                .setResourceType("Topic")
                .setResourceName("_confluent-metadata-auth")
                .setOperation("Describe")
                .setPatternType("LITERAL")
                .setGranted(true)
                .build()
        )
        .setRequest(Struct.newBuilder()
            .putFields("correlation_id",
                Value.newBuilder().setStringValue("13").build())
            .putFields("client_id",
                Value.newBuilder().setStringValue("_confluent-metadata-auth-consumer-1").build())
            .build())
        .setRequestMetadata(Struct.newBuilder()
            .putFields("client_address",
                Value.newBuilder().setStringValue("/172.31.11.172").build())
            .build())
        .build();

    // The mapper in the Cloudevents SDK is declared in a static block. We initialize it the same
    // to replicate the bug: https://confluentinc.atlassian.net/browse/CPKAFKA-3888.

    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new Jdk8Module());

    SimpleModule module = new SimpleModule();
    module.addSerializer(ZonedDateTime.class, new ZonedDateTimeSerializer());
    module.addDeserializer(ZonedDateTime.class, new ZonedDateTimeDeserializer());
    mapper.registerModule(module);

    // Check that protobuf module is missing.
    assertTrue(mapper.getRegisteredModuleIds().stream()
        .noneMatch(m -> m.equals(ProtobufModule.class.getCanonicalName())));

    CloudEvent message = ProtobufEvent.newBuilder()
        .setType("io.confluent.kafka.server/authorization")
        .setSource("crn:///kafka=CdiHxnm2SwGtUg5nnB8rBQ")
        .setSubject("crn:///kafka=CdiHxnm2SwGtUg5nnB8rBQ/topic=_confluent-metadata-auth")
        .setId("728497fe-2ab4-47ae-8984-40127c5a65cb")
        .setTime(ZonedDateTime.parse("2019-11-04T21:49:27.552Z"))
        .setEncoding(EventLoggerConfig.DEFAULT_CLOUD_EVENT_ENCODING_CONFIG)
        .setData(ale)
        .build();

    // This should produce the exception
    // com.fasterxml.jackson.databind.exc.InvalidDefinitionException: Direct self-reference
    // leading to cycle (through reference chain: io.cloudevents.v03.CloudEventImpl["data"]->
    // io.confluent.security.audit.AuditLogEntry["unknownFields"]
    // ->com.google.protobuf.UnknownFieldSet["defaultInstanceForType"])
    try {
      mapper.writeValueAsBytes(message);
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }
}
