/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.security.audit;

import static io.confluent.security.audit.AuditLogUtils.AUTHENTICATION_EVENT_NAME;
import static io.confluent.security.audit.AuditLogUtils.AUTHENTICATION_FAILED_EVENT_USER;
import static io.confluent.security.audit.router.AuditLogRouter.SUPPRESSED;
import static org.junit.Assert.assertTrue;

import io.confluent.security.audit.router.AuditLogCategoryResultRouter;
import io.confluent.security.audit.router.AuditLogRouter;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import org.apache.kafka.server.audit.AuditEventStatus;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AuditLogRouterTest {

  /*

  These tests use this config. This is copied to sample-audit-log-routing.json

{
     "destinations": {
        "bootstrap_servers": [
            "host1:port",
            "host2:port"
        ],
        // This section defines named topics that are referred to in the "routes" section
        // Updating the retention_ms here will cause a change to the existing topic
        "topics": {
            "confluent-audit-log-events_success": {
                "retention_ms": 2592000000
            },
            "confluent-audit-log-events_failure": {
                "retention_ms": 2592000000
            },
            "confluent-audit-log-events_ksql": {
                "retention_ms": 2592000000
            },
            "confluent-audit-log-events_connect_success": {
                "retention_ms": 2592000000
            },
            "confluent-audit-log-events_connect_failure": {
                "retention_ms": 15552000000
            },
            "confluent-audit-log-events_clicks_produce_allowed": {
                "retention_ms": 15552000000
            },
            "confluent-audit-log-events_clicks_produce_denied": {
                "retention_ms": 15552000000
            },
            "confluent-audit-log-events_clicks_consume_allowed": {
                "retention_ms": 15552000000
            },
            "confluent-audit-log-events_clicks_consume_denied": {
                "retention_ms": 15552000000
            },
            "confluent-audit-log-events_accounting": {
                "retention_ms": 15552000000
            },
            "confluent-audit-log-events_cluster": {
                "retention_ms": 15552000000
            },
            "confluent-audit-log-events_authentication_success": {
                "retention_ms": 15552000000
            },
            "confluent-audit-log-events_authentication_failure": {
                "retention_ms": 15552000000
            }
        }
    },
    // If no routes specify a different topic, audit logs are sent to these topics
    "default_topics": {
        "allowed": "confluent-audit-log-events_success",  // Topic to send successful events to
        "denied": "confluent-audit-log-events_failure"  // Topic to send failed events to
    },
    // Don't log authorizations for these principals.
    // Note: the Audit Log principal is automatically excluded
    "excluded_principals": [
        "User:Alice",
        "User:service_account_id",
        "None:UNKNOWN_USER"
    ],
    "routes": {
        // MDS Authorization Audit logging
        // Configure audit log routing for MDS Authorizations for a specific KSQL cluster
        "crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/ksql=ksql1": {
            "authorize": {
                "allowed": "", // empty string topic name means that the log message is not sent
                "denied": "confluent-audit-log-events_ksql"
            }
        },
        // Configure audit log routing for MDS Authorizations for all Connect clusters
        "crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/connect=*": {
            "authorize": {
                "allowed": "confluent-audit-log-events_connect_success",
                "denied": "confluent-audit-log-events_connect_failure"
            }
        },
        // Kafka Authorization Audit Logging
        // Configure audit log routing for a literal topic
        "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks": {
            "produce": {
                "allowed": "confluent-audit-log-events_clicks_produce_allowed", // when a Produce is allowed, a log message is sent to the topic with this name
                "denied": "confluent-audit-log-events_clicks_produce_denied"
            },
            "consume": {
                "allowed": "confluent-audit-log-events_clicks_consume_allowed",
                "denied": "confluent-audit-log-events_clicks_consume_denied"
            }
        },
        // Configure audit log routing for a topic prefix
        "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=accounting-*": {
            "produce": {
                "allowed": null, // null key defaults to default_topic.allowed
                "denied": "confluent-audit-log-events_accounting"
            }
            // because Consume is not specified, it defaults to default_topic.allowed and default_topic.denied
        },
        // Configure audit log routing for all topics on a cluster
        "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=*": {
            "produce": {
                "allowed": "", // empty string topic name means that the log message is not sent
                "denied": "confluent-audit-log-events_cluster"
            },
            "consume": {
                // because "allowed" is not specified, it defaults to default_topic.allowed
                "denied": "confluent-audit-log-events_cluster"
            }
        },
        // Configure audit log routing for non-topic events on a cluster
        "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA": {
            "interbroker": {
                "allowed": "", // empty string topic name means that the log message is not sent
                "denied": "confluent-audit-log-events_cluster"
            },
            // Configure audit log routing for authentication events
           "authentication": {
                "allowed": "confluent-audit-log-events_authentication_success",
                "denied": "confluent-audit-log-events_authentication_failure"
             },
            // All other events
            "other": {
                // because "allowed" is not specified, it defaults to default_topic.allowed
                "denied": "confluent-audit-log-events_cluster"
            }
        },
        // Configure audit log routing for non-topic events on all clusters
        "crn://mds1.example.com/kafka=*": {
            "interbroker": {
                "allowed": "", // empty string topic name means that the log message is not sent
                "denied": "confluent-audit-log-events_cluster"
            },
            // All other events
            "other": {
                // because "allowed" is not specified, it defaults to default_topic.allowed
                "denied": "confluent-audit-log-events_cluster"
            }
        }
    },
    "metadata": {
        // Server Generated
        "resource_version": "f109371d0a856a40a2a96cca98f90ec2",
        "updated_at": "2019-08-21T18:31:47+00:00"
    }
}
 */

  private AuditLogRouter router;

  @Before
  public void setUp() throws Exception {
    router = new AuditLogRouter(AuditLogRouterJsonConfig.load(routerConfig()), 10000);
  }

  private String routerConfig() throws IOException {
    final String path = AuditLogRouterTest.class.getResource("/sample-audit-log-routing.json").getFile();
    return new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
  }

  private AuditLogEntry sampleEvent(String subject, String method, String principal,
      boolean granted) {
    return
        AuditLogEntry.newBuilder()
            .setResourceName(subject)
            .setMethodName(method)
            .setAuthenticationInfo(AuthenticationInfo.newBuilder()
                .setPrincipal(principal)
                .build())
            .setAuthorizationInfo(AuthorizationInfo.newBuilder()
                .setGranted(granted))
            .build();
  }

  private AuditLogEntry sampleAuthenticationEvent(String subject, String principal, AuditEventStatus eventStatus) {
    return
        AuditLogEntry.newBuilder()
            .setResourceName(subject)
            .setMethodName(AUTHENTICATION_EVENT_NAME)
            .setAuthenticationInfo(AuthenticationInfo.newBuilder()
                .setPrincipal(principal)
                .setMetadata(AuthenticationMetadata.newBuilder()
                    .setMechanism("SASL")
                    .setIdentifier("test")
                    .build())
                .build())
            .setResult(Result.newBuilder()
                .setStatus(eventStatus.name())
                .setMessage("test")
                .build())
            .build();
  }

  @Test
  public void testExcludePrincipals() {
    // Suppress message from Alice
    Assert.assertEquals(Optional.of(""),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks",
                "kafka.Produce", "User:Alice", true)));

    // Don't suppress same message from Bob
    Assert.assertEquals(Optional.of("confluent-audit-log-events_clicks_produce_allowed"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks",
                "kafka.Produce", "User:Bob", true)));

    // Suppress message from User:service_account_id
    Assert.assertEquals(Optional.of(""),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks",
                "kafka.Produce", "User:service_account_id", true)));

    // Suppress authentication message from "None:UNKNOWN_USER"
    Assert.assertEquals(Optional.of(""),
        router.topic(
            sampleAuthenticationEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks",
                AUTHENTICATION_FAILED_EVENT_USER, AuditEventStatus.UNKNOWN_USER_DENIED)));

    // Don't suppress authentication message from Bob
    Assert.assertEquals(Optional.of("confluent-audit-log-events_success"),
        router.topic(
            sampleAuthenticationEvent("crn://mds1.example.com/kafka=XYZ",
                 "User:Bob", AuditEventStatus.SUCCESS)));

  }

  @Test
  public void testSuppressAllowed() {
    // Allowed goes nowhere
    Assert.assertEquals(Optional.of(""),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/ksql=ksql1",
                "mds.Authorize", "User:Bob", true)));

    // Denied goes to defined topic
    Assert.assertEquals(Optional.of("confluent-audit-log-events_ksql"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/ksql=ksql1",
                "mds.Authorize", "User:Bob", false)));
  }

  @Test
  public void testAllConnect() {
    // Matches connect=*
    Assert.assertEquals(Optional.of("confluent-audit-log-events_connect_success"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/connect=connect1",
                "mds.Authorize", "User:Bob", true)));

    // Matches connect=*
    Assert.assertEquals(Optional.of("confluent-audit-log-events_connect_failure"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/connect=connect2",
                "mds.Authorize", "User:Bob", false)));
  }

  @Test
  public void testCategories() {
    // Produce goes to produce_
    Assert.assertEquals(Optional.of("confluent-audit-log-events_clicks_produce_allowed"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks",
                "kafka.Produce", "User:Bob", true)));

    // Consume goes to consume_
    Assert.assertEquals(Optional.of("confluent-audit-log-events_clicks_consume_allowed"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks",
                "kafka.FetchConsumer", "User:Bob", true)));
  }

  @Test
  public void testGranted() {
    // Allowed goes to _allowed
    Assert.assertEquals(Optional.of("confluent-audit-log-events_clicks_produce_allowed"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks",
                "kafka.Produce", "User:Bob", true)));

    // Denied goes to _denied
    Assert.assertEquals(Optional.of("confluent-audit-log-events_clicks_produce_denied"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks",
                "kafka.Produce", "User:Bob", false)));
  }

  @Test
  public void testDefaultFallback() {
    // Produce Allowed goes to default (which is to suppress Produce/Consume messages)
    Assert.assertEquals(Optional.of(""),
        router.topic(
            sampleEvent(
                "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=accounting-payroll",
                "kafka.Produce", "User:Bob", true)));

    // Produce Denied goes to defined topic
    Assert.assertEquals(Optional.of("confluent-audit-log-events_accounting"),
        router.topic(
            sampleEvent(
                "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=accounting-payroll",
                "kafka.Produce", "User:Bob", false)));

    // Consume Allowed goes to default (which is to suppress Produce/Consume messages)
    Assert.assertEquals(Optional.of(""),
        router.topic(
            sampleEvent(
                "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=accounting-payroll",
                "kafka.FetchConsumer", "User:Bob", true)));

    // Consume Denied goes to defined topic (which is to suppress Produce/Consume messages)
    Assert.assertEquals(Optional.of(""),
        router.topic(
            sampleEvent(
                "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=accounting-payroll",
                "kafka.FetchConsumer", "User:Bob", false)));
  }

  @Test
  public void testClusterEvents() {
    // Interbroker Allowed suppressed
    Assert.assertEquals(Optional.of(""),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA",
                "kafka.FetchFollower", "User:Bob", true)));

    // Interbroker Denied goes to defined topic
    Assert.assertEquals(Optional.of("confluent-audit-log-events_cluster"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA",
                "kafka.FetchFollower", "User:Bob", false)));

    // Consume Allowed goes to default
    Assert.assertEquals(Optional.of("confluent-audit-log-events_success"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA",
                "kafka.CreateTopics", "User:Bob", true)));

    // Consume Denied goes to defined topic
    Assert.assertEquals(Optional.of("confluent-audit-log-events_cluster"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA",
                "kafka.CreateTopics", "User:Bob", false)));
  }

  @Test
  public void testAllClusterEvents() {
    // Interbroker Allowed suppressed
    Assert.assertEquals(Optional.of(""),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=f5B4bB7_RZi4-muWq2pLlg",
                "kafka.FetchFollower", "User:Bob", true)));

    // Interbroker Denied goes to defined topic
    Assert.assertEquals(Optional.of("confluent-audit-log-events_cluster"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=RGiGzT0RRyKWEoGJrk-rkQ",
                "kafka.FetchFollower", "User:Bob", false)));

    // Other Allowed goes to default
    Assert.assertEquals(Optional.of("confluent-audit-log-events_success"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=3qW6InmVT0CVkN2NrBFOPQ",
                "kafka.CreateTopics", "User:Bob", true)));

    // Other Denied goes to defined topic
    Assert.assertEquals(Optional.of("confluent-audit-log-events_cluster"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=xqeA2ZZzT4moO0vq9q1rkw",
                "kafka.CreateTopics", "User:Bob", false)));
  }

  @Test
  public void testNotAuditLogEvent() {
    Assert.assertEquals(Optional.empty(), router.topic(AuditLogEntry.newBuilder().build()));
  }

  @Test
  public void testDefault() throws IOException {
    AuditLogRouter router = new AuditLogRouter(
        AuditLogRouterJsonConfig.load(
            AuditLogRouterJsonConfigUtils.defaultConfig("localhost:9092",
                "confluent-audit-log-events_allowed", "confluent-audit-log-events_denied")),
        100);

    // Authorize goes to default
    Assert.assertEquals(Optional.of("confluent-audit-log-events_allowed"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/ksql=ksql1",
                "mds.Authorize", "User:Bob", true)));

    Assert.assertEquals(Optional.of("confluent-audit-log-events_denied"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/ksql=ksql1",
                "mds.Authorize", "User:Bob", false)));

    // Other goes to default
    Assert.assertEquals(Optional.of("confluent-audit-log-events_allowed"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=3qW6InmVT0CVkN2NrBFOPQ",
                "kafka.CreateTopics", "User:Bob", true)));

    Assert.assertEquals(Optional.of("confluent-audit-log-events_denied"),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=xqeA2ZZzT4moO0vq9q1rkw",
                "kafka.CreateTopics", "User:Bob", false)));

    // Produce is suppressed
    Assert.assertEquals(Optional.of(""),
        router.topic(
            sampleEvent(
                "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=accounting-payroll",
                "kafka.Produce", "User:Bob", true)));

    Assert.assertEquals(Optional.of(""),
        router.topic(
            sampleEvent(
                "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=accounting-payroll",
                "kafka.Produce", "User:Bob", false)));

    // Consume is suppressed
    Assert.assertEquals(Optional.of(""),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks",
                "kafka.FetchConsumer", "User:Bob", true)));

    Assert.assertEquals(Optional.of(""),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks",
                "kafka.FetchConsumer", "User:Bob", false)));

    // Interbroker is suppressed
    Assert.assertEquals(Optional.of(""),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=f5B4bB7_RZi4-muWq2pLlg",
                "kafka.FetchFollower", "User:Bob", true)));

    Assert.assertEquals(Optional.of(""),
        router.topic(
            sampleEvent("crn://mds1.example.com/kafka=f5B4bB7_RZi4-muWq2pLlg",
                "kafka.FetchFollower", "User:Bob", false)));


  }

  @Test
  public void testConsumeAuditLogTopic() throws Exception {

    String config =
        "{\n"
            + "    \"destinations\": {\n"
            + "        \"topics\": {\n"
            + "            \"confluent-audit-log-events\": {\n"
            + "                \"retention_ms\": 2592000000\n"
            + "            }\n"
            + "        }\n"
            + "    },\n"
            + "    \"default_topics\": {\n"
            + "        \"allowed\": \"confluent-audit-log-events\",\n"
            + "        \"denied\": \"confluent-audit-log-events\"\n"
            + "    },\n"
            + "    \"routes\": {\n"
            + "        \"crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/topic=*\": {\n"
            + "            \"consume\": {\n"
            + "                \"allowed\": \"confluent-audit-log-events\",\n"
            + "                \"denied\": \"confluent-audit-log-events\"\n"
            + "            }\n"
            + "        }\n"
            + "    }\n"
            + "}";

    router = new AuditLogRouter(AuditLogRouterJsonConfig.load(config), 10000);

    Logger testLogger = Logger.getLogger(AuditLogCategoryResultRouter.class);
    StringWriter writer = new StringWriter();
    testLogger.removeAllAppenders();
    testLogger.addAppender(new WriterAppender(new PatternLayout("%m"), writer));

    // Consume events on the same topic are suppressed
    Assert.assertEquals(Optional.of(SUPPRESSED),
        router.topic(
            sampleEvent(
                "crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/topic=confluent-audit-log-events",
                "kafka.FetchConsumer", "User:Bob", true)));

    String logText = writer.toString();
    assertTrue(logText.contains("Principal User:Bob should be excluded from audit logging"));
  }

  @Test
  public void testAuthenticationEvents() {
    // Test authentication events to specific topic

    // Authentication success goes to confluent-audit-log-events_authentication_success
    Assert.assertEquals(Optional.of("confluent-audit-log-events_authentication_success"),
        router.topic(
            sampleAuthenticationEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA",
                "User:Bob", AuditEventStatus.SUCCESS)));

    // Authentication failure goes to confluent-audit-log-events_authentication_failure
    Assert.assertEquals(Optional.of("confluent-audit-log-events_authentication_failure"),
        router.topic(
            sampleAuthenticationEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA",
                "User:Bob", AuditEventStatus.UNAUTHENTICATED)));

    // unknown user denied goes to confluent-audit-log-events_authentication_failure
    Assert.assertEquals(Optional.of("confluent-audit-log-events_authentication_failure"),
        router.topic(
            sampleAuthenticationEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA",
                "User:Bob", AuditEventStatus.UNKNOWN_USER_DENIED)));


    // Other cluster  authentication events should go to default topics

    // Authentication success goes to confluent-audit-log-events_success
    Assert.assertEquals(Optional.of("confluent-audit-log-events_success"),
        router.topic(
            sampleAuthenticationEvent("crn://mds1.example.com/kafka=other_cluster",
                "User:Bob", AuditEventStatus.SUCCESS)));

    // Authentication failure goes to confluent-audit-log-events_failure
    Assert.assertEquals(Optional.of("confluent-audit-log-events_failure"),
        router.topic(
            sampleAuthenticationEvent("crn://mds1.example.com/kafka=other_cluster",
                "User:Bob", AuditEventStatus.UNAUTHENTICATED)));

    // unknown user denied goes to confluent-audit-log-events_failure
    Assert.assertEquals(Optional.of("confluent-audit-log-events_failure"),
        router.topic(
            sampleAuthenticationEvent("crn://mds1.example.com/kafka=other_cluster",
                "User:Bob", AuditEventStatus.UNKNOWN_USER_DENIED)));
  }
}