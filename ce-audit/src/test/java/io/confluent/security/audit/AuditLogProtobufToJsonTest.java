/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.security.audit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.cloudevents.CloudEvent;
import io.confluent.events.CloudEventUtils;
import io.confluent.events.EventLoggerConfig;
import io.confluent.events.ProtobufEvent;
import java.time.ZonedDateTime;
import org.junit.Test;

public class AuditLogProtobufToJsonTest {

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
                "{\"data\":{\"serviceName\":\"crn://confluent.cloud/kafka=lkc-ld9rz\",\"methodName\":\"CreateTopics\",\"resourceName\":\"crn://confluent.cloud/kafka=lkc-ld9rz/topic=my_new_topic\",\"authenticationInfo\":{\"principal\":\"/users/123\"},\"authorizationInfo\":{\"granted\":true,\"operation\":\"Create\",\"resourceType\":\"Topic\",\"resourceName\":\"my_new_topic\",\"patternType\":\"Literal\",\"aclAuthorization\":{\"permissionType\":\"Allow\",\"host\":\"vPeOCWypqUOSepEvx0cbog\"}},\"request\":{\"requestType\":\"CreateTopics\",\"resourceType\":\"Topic\",\"resourceName\":\"my_new_topic\",\"correlationId\":12345.0,\"clientId\":\"userSupplied\"},\"requestMetadata\":{\"callerIp\":\"192.168.1.23\"}},\"id\":\"e7872058-f971-496c-8a14-e6b0196c7ce\",\"source\":\"crn://confluent.cloud/kafka=lkc-ld9rz\",\"specversion\":\"0.3\",\"type\":\"io.confluent.kafka.server/authorization\",\"time\":\"2020-04-05T17:31:00Z\",\"datacontenttype\":\"application/json\",\"subject\":\"crn://confluent.cloud/kafka=lkc-ld9rz/topic=my_new_topic\"}",
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
                "{\"data\":{\"serviceName\":\"crn://mds.example.com/kafka=vPeOCWypqUOSepEvx0cbog\",\"methodName\":\"CreateTopics\",\"resourceName\":\"crn://mds.example.com/kafka=vPeOCWypqUOSepEvx0cbog/topic=my_new_topic\",\"authenticationInfo\":{\"principal\":\"User:Alice\"},\"authorizationInfo\":{\"granted\":true,\"operation\":\"Create\",\"resourceType\":\"Topic\",\"resourceName\":\"my_new_topic\",\"patternType\":\"Literal\",\"rbacAuthorization\":{\"role\":\"ResourceOwner\",\"scope\":{\"outerScope\":[\"myorg\",\"myenv\"],\"clusters\":{\"kafka-cluster\":\"vPeOCWypqUOSepEvx0cbog\"}}}},\"request\":{\"requestType\":\"CreateTopics\",\"resourceType\":\"Topic\",\"resourceName\":\"my_new_topic\",\"correlationId\":12345.0,\"clientId\":\"userSupplied\"},\"requestMetadata\":{\"callerIp\":\"192.168.1.23\"}},\"id\":\"e7872058-f971-496c-8a14-e6b0196c7ce\",\"source\":\"crn://mds.example.com/kafka=vPeOCWypqUOSepEvx0cbog\",\"specversion\":\"0.3\",\"type\":\"io.confluent.kafka.server/authorization\",\"time\":\"2020-04-05T17:31:00Z\",\"datacontenttype\":\"application/json\",\"subject\":\"crn://mds.example.com/kafka=vPeOCWypqUOSepEvx0cbog/topic=my_new_topic\"}",
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

}
