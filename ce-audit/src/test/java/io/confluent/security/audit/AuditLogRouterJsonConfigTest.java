/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.security.audit;

import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class AuditLogRouterJsonConfigTest {

  @Test
  public void testValidateHappyPath() throws IOException {
    // make sure happy path works
    AuditLogRouterJsonConfig.load(
        "{\n"
            + "    \"routes\": {\n"
            + "        \"crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/ksql=ksql1\": {\n"
            + "            \"authorize\": {\n"
            + "                \"allowed\": \"\",\n"
            + "                \"denied\": \"confluent-audit-log-events_ksql\"\n"
            + "            }\n"
            + "        }\n"
            + "    },\n"
            + "    \"metadata\": null,\n"
            + "    \"destinations\": {\n"
            + "        \"bootstrap_servers\": [\n"
            + "            \"localhost:9092\"\n"
            + "        ],\n"
            + "        \"topics\": {\n"
            + "            \"confluent-audit-log-events\": {\n"
            + "                \"retention_ms\": 7776000000\n"
            + "            },\n"
            + "            \"confluent-audit-log-events_ksql\": {\n"
            + "                \"retention_ms\": 7776000000\n"
            + "            }\n"
            + "        }\n"
            + "    },\n"
            + "    \"default_topics\": {\n"
            + "        \"allowed\": \"confluent-audit-log-events\",\n"
            + "        \"denied\": \"confluent-audit-log-events\"\n"
            + "    },\n"
            + "    \"excluded_principals\": []\n"
            + "}"
    );
  }

  @Test
  public void testValidateIgnoreUnknown() throws IOException {
    AuditLogRouterJsonConfig.load(
        "{\n"
            + "    \"routes\": {\n"
            + "        \"crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/ksql=ksql1\": {\n"
            + "            \"authorize\": {\n"
            + "                \"allowed\": \"\",\n"
            + "                \"denied\": \"confluent-audit-log-events_ksql\"\n"
            + "            }\n"
            + "        }\n"
            + "    },\n"
            + "    \"metadata\": {\"future\": 2099},\n"
            + "    \"destinations\": {\n"
            + "        \"bootstrap_servers\": [\n"
            + "            \"localhost:9092\"\n"
            + "        ],\n"
            + "        \"future\": 2099,\n"
            + "        \"topics\": {\n"
            + "            \"confluent-audit-log-events\": {\n"
            + "                \"retention_ms\": 7776000000\n"
            + "            },\n"
            + "            \"confluent-audit-log-events_ksql\": {\n"
            + "                \"retention_ms\": 7776000000,\n"
            + "                \"future\": 2099\n"
            + "            }\n"
            + "        }\n"
            + "    },\n"
            + "    \"default_topics\": {\n"
            + "        \"allowed\": \"confluent-audit-log-events\",\n"
            + "        \"denied\": \"confluent-audit-log-events\"\n"
            + "    },\n"
            + "    \"excluded_principals\": [],\n"
            + "    \"future\": 2099\n"
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
                + "                \"denied\": \"confluent-audit-log-events_ksql\"\n"
                + "            }\n"
                + "        }\n"
                + "    },\n"
                + "    \"metadata\": null,\n"
                + "    \"destinations\": {\n"
                + "        \"bootstrap_servers\": [\n"
                + "            \"localhost:9092\"\n"
                + "        ],\n"
                + "        \"topics\": {\n"
                + "            \"confluent-audit-log-events\": {\n"
                + "                \"retention_ms\": 7776000000\n"
                + "            },\n"
                + "            \"confluent-audit-log-events_ksql\": {\n"
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
                + "                \"failure\": \"confluent-audit-log-events_ksql\"\n"
                + "            }\n"
                + "        }\n"
                + "    },\n"
                + "    \"metadata\": null,\n"
                + "    \"destinations\": {\n"
                + "        \"bootstrap_servers\": [\n"
                + "            \"localhost:9092\"\n"
                + "        ],\n"
                + "        \"topics\": {\n"
                + "            \"confluent-audit-log-events\": {\n"
                + "                \"retention_ms\": 7776000000\n"
                + "            },\n"
                + "            \"confluent-audit-log-events_ksql\": {\n"
                + "                \"retention_ms\": 7776000000\n"
                + "            }\n"
                + "        }\n"
                + "    },\n"
                + "    \"default_topics\": {\n"
                + "        \"allowed\": \"confluent-audit-log-events\",\n"
                + "        \"denied\": \"confluent-audit-log-events\"\n"
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
                + "                \"denied\": \"confluent-audit-log-events_ksql\"\n"
                + "            }\n"
                + "        }\n"
                + "    },\n"
                + "    \"metadata\": null,\n"
                + "    \"destinations\": {\n"
                + "        \"bootstrap_servers\": [\n"
                + "            \"localhost:9092\"\n"
                + "        ],\n"
                + "        \"topics\": {\n"
                + "            \"confluent-audit-log-events\": {\n"
                + "                \"retention_ms\": 7776000000\n"
                + "            },\n"
                + "            \"confluent-audit-log-events_ksql\": {\n"
                + "                \"retention_ms\": 7776000000\n"
                + "            }\n"
                + "        }\n"
                + "    },\n"
                + "    \"default_topics\": {\n"
                + "        \"allowed\": \"confluent-audit-log-events\",\n"
                + "        \"denied\": \"confluent-audit-log-events\"\n"
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
                + "        \"bootstrap_servers\": [\n"
                + "            \"localhost:9092\"\n"
                + "        ],\n"
                + "        \"topics\": {\n"
                + "            \"confluent-audit-log-events\": {\n"
                + "                \"retention_ms\": 7776000000\n"
                + "            },\n"
                + "            \"ksql_audit\": {\n"
                + "                \"retention_ms\": 7776000000\n"
                + "            }\n"
                + "        }\n"
                + "    },\n"
                + "    \"default_topics\": {\n"
                + "        \"allowed\": \"confluent-audit-log-events\",\n"
                + "        \"denied\": \"confluent-audit-log-events\"\n"
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
            + "        \"bootstrap_servers\": [\n"
            + "            \"localhost:9092\"\n"
            + "        ],\n"
            + "        \"topics\": {\n"
            + "            \"confluent-audit-log-events\": {\n"
            + "                \"retention_ms\": 7776000000\n"
            + "            },\n"
            + "            \"confluent-audit-log-events_ksql\": {\n"
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
