package io.confluent.security.audit;

import io.confluent.security.audit.router.AuditLogRouter;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
import java.io.IOException;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AuditLogRouterTest {

  /*

  These tests use this config:

{
     "destinations": {
        "bootstrap_servers": [
            "host1:port",
            "host2:port"
        ],
        // This section defines named topics that are referred to in the "routes" section
        // Updating the retention_ms here will cause a change to the existing topic
        "topics": {
            "_confluent-audit-log_success": {
                "retention_ms": 2592000000
            },
            "_confluent-audit-log_failure": {
                "retention_ms": 2592000000
            },
            "_confluent-audit-log_ksql": {
                "retention_ms": 2592000000
            },
            "_confluent-audit-log_connect_success": {
                "retention_ms": 2592000000
            },
            "_confluent-audit-log_connect_failure": {
                "retention_ms": 15552000000
            },
            "_confluent-audit-log_clicks_produce_allowed": {
                "retention_ms": 15552000000
            },
            "_confluent-audit-log_clicks_produce_denied": {
                "retention_ms": 15552000000
            },
            "_confluent-audit-log_clicks_consume_allowed": {
                "retention_ms": 15552000000
            },
            "_confluent-audit-log_clicks_consume_denied": {
                "retention_ms": 15552000000
            },
            "_confluent-audit-log_accounting": {
                "retention_ms": 15552000000
            },
            "_confluent-audit-log_cluster": {
                "retention_ms": 15552000000
            }
        }
    },
    // If no routes specify a different topic, audit logs are sent to these topics
    "default_topics": {
        "allowed": "_confluent-audit-log_success",  // Topic to send successful authorizations to
        "denied": "_confluent-audit-log_failure"  // Topic to send failed authorizations to
    },
    // Don't log authorizations for these principals.
    // Note: the Audit Log principal is automatically excluded
    "excluded_principals": [
        "User:Alice",
        "User:service_account_id"
    ],
    "routes": {
        // MDS Authorization Audit logging
        // Configure audit log routing for MDS Authorizations for a specific KSQL cluster
        "crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/ksql=ksql1": {
            "authorize": {
                "allowed": "", // empty string topic name means that the log message is not sent
                "denied": "_confluent-audit-log_ksql"
            }
        },
        // Configure audit log routing for MDS Authorizations for all Connect clusters
        "crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/connect=*": {
            "authorize": {
                "allowed": "_confluent-audit-log_connect_success",
                "denied": "_confluent-audit-log_connect_failure"
            }
        },
        // Kafka Authorization Audit Logging
        // Configure audit log routing for a literal topic
        "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks": {
            "produce": {
                "allowed": "_confluent-audit-log_clicks_produce_allowed", // when a Produce is allowed, a log message is sent to the topic with this name
                "denied": "_confluent-audit-log_clicks_produce_denied"
            },
            "consume": {
                "allowed": "_confluent-audit-log_clicks_consume_allowed",
                "denied": "_confluent-audit-log_clicks_consume_denied"
            }
        },
        // Configure audit log routing for a topic prefix
        "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=accounting-*": {
            "produce": {
                "allowed": null, // null key defaults to default_topic.allowed
                "denied": "_confluent-audit-log_accounting"
            }
            // because Consume is not specified, it defaults to default_topic.allowed and default_topic.denied
        },
        // Configure audit log routing for all topics on a cluster
        "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=*": {
            "produce": {
                "allowed": "", // empty string topic name means that the log message is not sent
                "denied": "_confluent-audit-log_cluster"
            },
            "consume": {
                // because "allowed" is not specified, it defaults to default_topic.allowed
                "denied": "_confluent-audit-log_cluster"
            }
        },
        // Configure audit log routing for non-topic events on a cluster
        "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA": {
            "interbroker": {
                "allowed": "", // empty string topic name means that the log message is not sent
                "denied": "_confluent-audit-log_cluster"
            },
            // All other events
            "other": {
                // because "allowed" is not specified, it defaults to default_topic.allowed
                "denied": "_confluent-audit-log_cluster"
            }
        },
        // Configure audit log routing for non-topic events on all clusters
        "crn://mds1.example.com/kafka=*": {
            "interbroker": {
                "allowed": "", // empty string topic name means that the log message is not sent
                "denied": "_confluent-audit-log_cluster"
            },
            // All other events
            "other": {
                // because "allowed" is not specified, it defaults to default_topic.allowed
                "denied": "_confluent-audit-log_cluster"
            }
        }
    },
    "metadata": {
        // Server Generated
        "config_version": "f109371d0a856a40a2a96cca98f90ec2",
        "last_updated": "2019-08-21T18:31:47+00:00"
    }
}
 */
  private String json = "{\"destinations\":{\"bootstrap_servers\":[\"host1:port\",\"host2:port\"],\"topics\":{\"_confluent-audit-log_success\":{\"retention_ms\":2592000000},\"_confluent-audit-log_failure\":{\"retention_ms\":2592000000},\"_confluent-audit-log_ksql\":{\"retention_ms\":2592000000},\"_confluent-audit-log_connect_success\":{\"retention_ms\":2592000000},\"_confluent-audit-log_connect_failure\":{\"retention_ms\":15552000000},\"_confluent-audit-log_clicks_produce_allowed\":{\"retention_ms\":15552000000},\"_confluent-audit-log_clicks_produce_denied\":{\"retention_ms\":15552000000},\"_confluent-audit-log_clicks_consume_allowed\":{\"retention_ms\":15552000000},\"_confluent-audit-log_clicks_consume_denied\":{\"retention_ms\":15552000000},\"_confluent-audit-log_accounting\":{\"retention_ms\":15552000000},\"_confluent-audit-log_cluster\":{\"retention_ms\":15552000000}}},\"default_topics\":{\"allowed\":\"_confluent-audit-log_success\",\"denied\":\"_confluent-audit-log_failure\"},\"excluded_principals\":[\"User:Alice\",\"User:service_account_id\"],\"routes\":{\"crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/ksql=ksql1\":{\"authorize\":{\"allowed\":\"\",\"denied\":\"_confluent-audit-log_ksql\"}},\"crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/connect=*\":{\"authorize\":{\"allowed\":\"_confluent-audit-log_connect_success\",\"denied\":\"_confluent-audit-log_connect_failure\"}},\"crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks\":{\"produce\":{\"allowed\":\"_confluent-audit-log_clicks_produce_allowed\",\"denied\":\"_confluent-audit-log_clicks_produce_denied\"},\"consume\":{\"allowed\":\"_confluent-audit-log_clicks_consume_allowed\",\"denied\":\"_confluent-audit-log_clicks_consume_denied\"}},\"crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=accounting-*\":{\"produce\":{\"allowed\":null,\"denied\":\"_confluent-audit-log_accounting\"}},\"crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=*\":{\"produce\":{\"allowed\":\"\",\"denied\":\"_confluent-audit-log_cluster\"},\"consume\":{\"denied\":\"_confluent-audit-log_cluster\"}},\"crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA\":{\"interbroker\":{\"allowed\":\"\",\"denied\":\"_confluent-audit-log_cluster\"},\"other\":{\"denied\":\"_confluent-audit-log_cluster\"}},\"crn://mds1.example.com/kafka=*\":{\"interbroker\":{\"allowed\":\"\",\"denied\":\"_confluent-audit-log_cluster\"},\"other\":{\"denied\":\"_confluent-audit-log_cluster\"}}},\"metadata\":{\"config_version\":\"f109371d0a856a40a2a96cca98f90ec2\",\"last_updated\":\"2019-08-21T18:31:47+00:00\"}}";
  private AuditLogRouter router;

  @Before
  public void setUp() throws Exception {
    router = new AuditLogRouter(AuditLogRouterJsonConfig.load(json), 10000);
  }

  private CloudEvent sampleEvent(String subject, String method, String principal, boolean granted) {
    return CloudEventUtils
        .wrap("io.confluent.security.authorization",
            "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA",
            subject,
            AuditLogEntry.newBuilder()
                .setMethodName(method)
                .setAuthenticationInfo(AuthenticationInfo.newBuilder()
                    .setPrincipal(principal)
                    .build())
                .setAuthorizationInfo(AuthorizationInfo.newBuilder()
                    .setGranted(granted))
                .build());
  }

  @Test
  public void testExcludePrincipals() {
    // Suppress message from Alice
    Assert.assertEquals(Optional.of(""),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks",
                "Kafka.Produce", "User:Alice", true)));

    // Don't suppress same message from Bob
    Assert.assertEquals(Optional.of("_confluent-audit-log_clicks_produce_allowed"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks",
                "Kafka.Produce", "User:Bob", true)));

    // Suppress message from User:service_account_id
    Assert.assertEquals(Optional.of(""),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks",
                "Kafka.Produce", "User:service_account_id", true)));
  }

  @Test
  public void testSuppressAllowed() {
    // Allowed goes nowhere
    Assert.assertEquals(Optional.of(""),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/ksql=ksql1",
                "Mds.Authorize", "User:Bob", true)));

    // Denied goes to defined topic
    Assert.assertEquals(Optional.of("_confluent-audit-log_ksql"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/ksql=ksql1",
                "Mds.Authorize", "User:Bob", false)));
  }

  @Test
  public void testAllConnect() {
    // Matches connect=*
    Assert.assertEquals(Optional.of("_confluent-audit-log_connect_success"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/connect=connect1",
                "Mds.Authorize", "User:Bob", true)));

    // Matches connect=*
    Assert.assertEquals(Optional.of("_confluent-audit-log_connect_failure"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/connect=connect2",
                "Mds.Authorize", "User:Bob", false)));
  }


  @Test
  public void testCategories() {
    // Produce goes to produce_
    Assert.assertEquals(Optional.of("_confluent-audit-log_clicks_produce_allowed"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks",
                "Kafka.Produce", "User:Bob", true)));

    // Consume goes to consume_
    Assert.assertEquals(Optional.of("_confluent-audit-log_clicks_consume_allowed"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks",
                "Kafka.FetchConsumer", "User:Bob", true)));
  }

  @Test
  public void testGranted() {
    // Allowed goes to _allowed
    Assert.assertEquals(Optional.of("_confluent-audit-log_clicks_produce_allowed"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks",
                "Kafka.Produce", "User:Bob", true)));

    // Denied goes to _denied
    Assert.assertEquals(Optional.of("_confluent-audit-log_clicks_produce_denied"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks",
                "Kafka.Produce", "User:Bob", false)));
  }

  @Test
  public void testDefault() {
    // Produce Allowed goes to default
    Assert.assertEquals(Optional.of("_confluent-audit-log_success"),
        router.topic(
            sampleEvent(
                "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=accounting-payroll",
                "Kafka.Produce", "User:Bob", true)));

    // Produce Denied goes to defined topic
    Assert.assertEquals(Optional.of("_confluent-audit-log_accounting"),
        router.topic(
            sampleEvent(
                "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=accounting-payroll",
                "Kafka.Produce", "User:Bob", false)));

    // Consume Allowed goes to default
    Assert.assertEquals(Optional.of("_confluent-audit-log_success"),
        router.topic(
            sampleEvent(
                "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=accounting-payroll",
                "Kafka.FetchConsumer", "User:Bob", true)));

    // Consume Denied goes to defined topic
    Assert.assertEquals(Optional.of("_confluent-audit-log_failure"),
        router.topic(
            sampleEvent(
                "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=accounting-payroll",
                "Kafka.FetchConsumer", "User:Bob", false)));
  }

  @Test
  public void testClusterEvents() {
    // Interbroker Allowed suppressed
    Assert.assertEquals(Optional.of(""),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA",
                "Kafka.FetchFollower", "User:Bob", true)));

    // Interbroker Denied goes to defined topic
    Assert.assertEquals(Optional.of("_confluent-audit-log_cluster"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA",
                "Kafka.FetchFollower", "User:Bob", false)));

    // Consume Allowed goes to default
    Assert.assertEquals(Optional.of("_confluent-audit-log_success"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA",
                "CreateTopics", "User:Bob", true)));

    // Consume Denied goes to defined topic
    Assert.assertEquals(Optional.of("_confluent-audit-log_cluster"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA",
                "CreateTopics", "User:Bob", false)));
  }


  @Test
  public void testAllClusterEvents() {
    // Interbroker Allowed suppressed
    Assert.assertEquals(Optional.of(""),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=f5B4bB7_RZi4-muWq2pLlg",
                "Kafka.FetchFollower", "User:Bob", true)));

    // Interbroker Denied goes to defined topic
    Assert.assertEquals(Optional.of("_confluent-audit-log_cluster"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=RGiGzT0RRyKWEoGJrk-rkQ",
                "Kafka.FetchFollower", "User:Bob", false)));

    // Consume Allowed goes to default
    Assert.assertEquals(Optional.of("_confluent-audit-log_success"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=3qW6InmVT0CVkN2NrBFOPQ",
                "CreateTopics", "User:Bob", true)));

    // Consume Denied goes to defined topic
    Assert.assertEquals(Optional.of("_confluent-audit-log_cluster"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=xqeA2ZZzT4moO0vq9q1rkw",
                "CreateTopics", "User:Bob", false)));
  }

  @Test
  public void testNotAuditLogEvent() {
    CloudEvent emptyEvent = CloudEvent.newBuilder().build();

    Assert.assertEquals(Optional.empty(), router.topic(emptyEvent));
  }

  @Test
  public void testValidateHappyPath() throws IOException {
    // make sure happy path works
    AuditLogRouterJsonConfig.load(
        "{\n"
            + "    \"routes\": {\n"
            + "        \"crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/ksql=ksql1\": {\n"
            + "            \"authorize\": {\n"
            + "                \"allowed\": \"\",\n"
            + "                \"denied\": \"_confluent-audit-log_ksql\"\n"
            + "            }\n"
            + "        }\n"
            + "    },\n"
            + "    \"metadata\": null,\n"
            + "    \"destinations\": {\n"
            + "        \"bootstrapServers\": [\n"
            + "            \"localhost:9092\"\n"
            + "        ],\n"
            + "        \"topics\": {\n"
            + "            \"_confluent-audit-log\": {\n"
            + "                \"retention_ms\": 7776000000\n"
            + "            },\n"
            + "            \"_confluent-audit-log_ksql\": {\n"
            + "                \"retention_ms\": 7776000000\n"
            + "            }\n"
            + "        }\n"
            + "    },\n"
            + "    \"default_topics\": {\n"
            + "        \"allowed\": \"_confluent-audit-log\",\n"
            + "        \"denied\": \"_confluent-audit-log\"\n"
            + "    },\n"
            + "    \"excluded_principals\": []\n"
            + "}"
    );
  }

  @Test
  public void testValidateEmptyConfig() {
    Assert.assertThrows(IllegalArgumentException.class, () ->
        AuditLogRouterJsonConfig.load("{}"));
  }

  @Test
  public void testValidateNoDefaults() {
    // no defaults
    Assert.assertThrows(IllegalArgumentException.class, () ->
        AuditLogRouterJsonConfig.load(
            "{\n"
                + "    \"routes\": {\n"
                + "        \"crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/ksql=ksql1\": {\n"
                + "            \"authorize\": {\n"
                + "                \"allowed\": \"\",\n"
                + "                \"denied\": \"_confluent-audit-log_ksql\"\n"
                + "            }\n"
                + "        }\n"
                + "    },\n"
                + "    \"metadata\": null,\n"
                + "    \"destinations\": {\n"
                + "        \"bootstrapServers\": [\n"
                + "            \"localhost:9092\"\n"
                + "        ],\n"
                + "        \"topics\": {\n"
                + "            \"_confluent-audit-log\": {\n"
                + "                \"retention_ms\": 7776000000\n"
                + "            },\n"
                + "            \"_confluent-audit-log_ksql\": {\n"
                + "                \"retention_ms\": 7776000000\n"
                + "            }\n"
                + "        }\n"
                + "    },\n"
                + "    \"excluded_principals\": []\n"
                + "}"
        ));
  }

  @Test
  public void testValidateWrongResults() {
    Assert.assertThrows(IllegalArgumentException.class, () ->
        AuditLogRouterJsonConfig.load(
            "{\n"
                + "    \"routes\": {\n"
                + "        \"crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/ksql=ksql1\": {\n"
                + "            \"authorize\": {\n"
                + "                \"success\": \"\",\n" // should be allowed, denied
                + "                \"failure\": \"_confluent-audit-log_ksql\"\n"
                + "            }\n"
                + "        }\n"
                + "    },\n"
                + "    \"metadata\": null,\n"
                + "    \"destinations\": {\n"
                + "        \"bootstrapServers\": [\n"
                + "            \"localhost:9092\"\n"
                + "        ],\n"
                + "        \"topics\": {\n"
                + "            \"_confluent-audit-log\": {\n"
                + "                \"retention_ms\": 7776000000\n"
                + "            },\n"
                + "            \"_confluent-audit-log_ksql\": {\n"
                + "                \"retention_ms\": 7776000000\n"
                + "            }\n"
                + "        }\n"
                + "    },\n"
                + "    \"default_topics\": {\n"
                + "        \"allowed\": \"_confluent-audit-log\",\n"
                + "        \"denied\": \"_confluent-audit-log\"\n"
                + "    },\n"
                + "    \"excluded_principals\": []\n"
                + "}"
        ));
  }

  @Test
  public void testValidateWrongCategory() {
    Assert.assertThrows(IllegalArgumentException.class, () ->
        AuditLogRouterJsonConfig.load(
            "{\n"
                + "    \"routes\": {\n"
                + "        \"crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/ksql=ksql1\": {\n"
                + "            \"random\": {\n" // unknown category
                + "                \"allowed\": \"\",\n"
                + "                \"denied\": \"_confluent-audit-log_ksql\"\n"
                + "            }\n"
                + "        }\n"
                + "    },\n"
                + "    \"metadata\": null,\n"
                + "    \"destinations\": {\n"
                + "        \"bootstrapServers\": [\n"
                + "            \"localhost:9092\"\n"
                + "        ],\n"
                + "        \"topics\": {\n"
                + "            \"_confluent-audit-log\": {\n"
                + "                \"retention_ms\": 7776000000\n"
                + "            },\n"
                + "            \"_confluent-audit-log_ksql\": {\n"
                + "                \"retention_ms\": 7776000000\n"
                + "            }\n"
                + "        }\n"
                + "    },\n"
                + "    \"default_topics\": {\n"
                + "        \"allowed\": \"_confluent-audit-log\",\n"
                + "        \"denied\": \"_confluent-audit-log\"\n"
                + "    },\n"
                + "    \"excluded_principals\": []\n"
                + "}"
        ));
  }

  @Test
  public void testValidateWrongPrefix() {
    Assert.assertThrows(IllegalArgumentException.class, () ->
        AuditLogRouterJsonConfig.load(
            "{\n"
                + "    \"routes\": {\n"
                + "        \"crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/ksql=ksql1\": {\n"
                + "            \"authorize\": {\n"
                + "                \"allowed\": \"\",\n"
                + "                \"denied\": \"ksql_audit\"\n" // topic doesn't start with prefix
                + "            }\n"
                + "        }\n"
                + "    },\n"
                + "    \"metadata\": null,\n"
                + "    \"destinations\": {\n"
                + "        \"bootstrapServers\": [\n"
                + "            \"localhost:9092\"\n"
                + "        ],\n"
                + "        \"topics\": {\n"
                + "            \"_confluent-audit-log\": {\n"
                + "                \"retention_ms\": 7776000000\n"
                + "            },\n"
                + "            \"ksql_audit\": {\n"
                + "                \"retention_ms\": 7776000000\n"
                + "            }\n"
                + "        }\n"
                + "    },\n"
                + "    \"default_topics\": {\n"
                + "        \"allowed\": \"_confluent-audit-log\",\n"
                + "        \"denied\": \"_confluent-audit-log\"\n"
                + "    },\n"
                + "    \"excluded_principals\": []\n"
                + "}"));

  }

  @Test
  public void testValidateEmptyTopic() throws IOException {
    // the empty topic should be allowed
    AuditLogRouterJsonConfig.load(
        "{\n"
            + "    \"routes\": {\n"
            + "    },\n"
            + "    \"metadata\": null,\n"
            + "    \"destinations\": {\n"
            + "        \"bootstrapServers\": [\n"
            + "            \"localhost:9092\"\n"
            + "        ],\n"
            + "        \"topics\": {\n"
            + "            \"_confluent-audit-log\": {\n"
            + "                \"retention_ms\": 7776000000\n"
            + "            },\n"
            + "            \"_confluent-audit-log_ksql\": {\n"
            + "                \"retention_ms\": 7776000000\n"
            + "            }\n"
            + "        }\n"
            + "    },\n"
            + "    \"default_topics\": {\n"
            + "        \"allowed\": \"\",\n"
            + "        \"denied\": \"\"\n"
            + "    },\n"
            + "    \"excluded_principals\": []\n"
            + "}");
  }
}
