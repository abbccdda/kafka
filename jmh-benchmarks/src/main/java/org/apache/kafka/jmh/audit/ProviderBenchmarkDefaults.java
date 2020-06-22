/*
 * Copyright [2020 - 2020] Confluent Inc.
 */

package org.apache.kafka.jmh.audit;

import io.confluent.security.audit.AuditLogConfig;
import io.confluent.security.audit.provider.ConfluentAuditLogProvider;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.config.internals.ConfluentConfigs.AUDIT_EVENT_ROUTER_CONFIG;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.CRN_AUTHORITY_NAME_CONFIG;

public class ProviderBenchmarkDefaults {

  static final int DISTINCT_KEYS = 10_000;
  static final int USERS = 101;
  static final int TOPICS = 97;
  static final String[] ACTIONS;
  static final ApiKeys[] API_KEYS;

  static {
    ArrayList<String> actions = new ArrayList<>();
    ArrayList<ApiKeys> apiKeys = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      actions.add("Produce");
      apiKeys.add(ApiKeys.PRODUCE);
      actions.add("FetchConsumer");
      apiKeys.add(ApiKeys.FETCH);
    }
    actions.add("CreateTopics");
    apiKeys.add(ApiKeys.CREATE_TOPICS);
    ACTIONS = actions.toArray(new String[0]);
    API_KEYS = apiKeys.toArray(new ApiKeys[0]);
  }

  static final String CLUSTER_ID = "63REM3VWREiYtMuVxZeplA";

  static ConfluentAuditLogProvider
      noneProvider =
      providerWithCountExporter(
          "{\n"
              + "    \"destinations\": {\n"
              + "        \"topics\": {\n"
              + "        }\n"
              + "    },\n"
              + "    \"default_topics\": {\n"
              + "        \"allowed\": \"\",\n"
              + "        \"denied\": \"\"\n"
              + "    }\n"
              + "}");

  static ConfluentAuditLogProvider
      createProduceOneLogProvider =
      providerWithCountExporter(
          "{\n"
              + "    \"routes\": {\n"
              + "        \"crn://mds.example.com/kafka=*/topic=topic0\": {\n"
              + "            \"produce\": {\n"
              + "                \"allowed\": \"confluent-audit-log-events-produce\",\n"
              + "                \"denied\": \"confluent-audit-log-events-produce\"\n"
              + "            }\n"
              + "        }\n"
              + "    },\n"
              + "    \"destinations\": {\n"
              + "        \"topics\": {\n"
              + "            \"confluent-audit-log-events-allowed\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            },\n"
              + "            \"confluent-audit-log-events-denied\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            },\n"
              + "            \"confluent-audit-log-events-produce\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            }\n"
              + "        }\n"
              + "    },\n"
              + "    \"default_topics\": {\n"
              + "        \"allowed\": \"confluent-audit-log-events-allowed\",\n"
              + "        \"denied\": \"confluent-audit-log-events-denied\"\n"
              + "    }\n"
              + "}");

  static ConfluentAuditLogProvider
      createProduceSomeLogProvider =
      providerWithCountExporter(
          "{\n"
              + "    \"routes\": {\n"
              + "        \"crn://mds.example.com/kafka=*/topic=topic1*\": {\n"
              + "            \"produce\": {\n"
              + "                \"allowed\": \"confluent-audit-log-events-produce\",\n"
              + "                \"denied\": \"confluent-audit-log-events-produce\"\n"
              + "            }\n"
              + "        }\n"
              + "    },\n"
              + "    \"destinations\": {\n"
              + "        \"topics\": {\n"
              + "            \"confluent-audit-log-events-allowed\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            },\n"
              + "            \"confluent-audit-log-events-denied\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            },\n"
              + "            \"confluent-audit-log-events-produce\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            }\n"
              + "        }\n"
              + "    },\n"
              + "    \"default_topics\": {\n"
              + "        \"allowed\": \"confluent-audit-log-events-allowed\",\n"
              + "        \"denied\": \"confluent-audit-log-events-denied\"\n"
              + "    }\n"
              + "}");

  static ConfluentAuditLogProvider
      createProduceAllLogProvider =
      providerWithCountExporter(
          "{\n"
              + "    \"routes\": {\n"
              + "        \"crn://mds.example.com/kafka=*/topic=*\": {\n"
              + "            \"produce\": {\n"
              + "                \"allowed\": \"confluent-audit-log-events-produce\",\n"
              + "                \"denied\": \"confluent-audit-log-events-produce\"\n"
              + "            }\n"
              + "        }\n"
              + "    },\n"
              + "    \"destinations\": {\n"
              + "        \"topics\": {\n"
              + "            \"confluent-audit-log-events-allowed\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            },\n"
              + "            \"confluent-audit-log-events-denied\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            },\n"
              + "            \"confluent-audit-log-events-produce\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            }\n"
              + "        }\n"
              + "    },\n"
              + "    \"default_topics\": {\n"
              + "        \"allowed\": \"confluent-audit-log-events-allowed\",\n"
              + "        \"denied\": \"confluent-audit-log-events-denied\"\n"
              + "    }\n"
              + "}");

  static ConfluentAuditLogProvider
      everythingLogProvider =
      providerWithCountExporter(
          "{\n"
              + "    \"routes\": {\n"
              + "        \"crn://mds.example.com/kafka=*/topic=*\": {\n"
              + "            \"produce\": {\n"
              + "                \"allowed\": \"confluent-audit-log-events-produce\",\n"
              + "                \"denied\": \"confluent-audit-log-events-produce\"\n"
              + "            },\n"
              + "            \"consume\": {\n"
              + "                \"allowed\": \"confluent-audit-log-events-produce\",\n"
              + "                \"denied\": \"confluent-audit-log-events-produce\"\n"
              + "            }\n"
              + "        }\n"
              + "    },\n"
              + "    \"destinations\": {\n"
              + "        \"topics\": {\n"
              + "            \"confluent-audit-log-events-allowed\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            },\n"
              + "            \"confluent-audit-log-events-denied\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            },\n"
              + "            \"confluent-audit-log-events-produce\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            }\n"
              + "        }\n"
              + "    },\n"
              + "    \"default_topics\": {\n"
              + "        \"allowed\": \"confluent-audit-log-events-allowed\",\n"
              + "        \"denied\": \"confluent-audit-log-events-denied\"\n"
              + "    }\n"
              + "}");

  static ConfluentAuditLogProvider
      createProvider =
      providerWithCountExporter(
          "{\n"
              + "    \"destinations\": {\n"
              + "        \"topics\": {\n"
              + "            \"confluent-audit-log-events-allowed\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            },\n"
              + "            \"confluent-audit-log-events-denied\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            }\n"
              + "        }\n"
              + "    },\n"
              + "    \"default_topics\": {\n"
              + "        \"allowed\": \"confluent-audit-log-events-allowed\",\n"
              + "        \"denied\": \"confluent-audit-log-events-denied\"\n"
              + "    }\n"
              + "}");

  static ConfluentAuditLogProvider
      allowProvider =
      providerWithCountExporter(
          "{\n"
              + "    \"destinations\": {\n"
              + "        \"topics\": {\n"
              + "            \"confluent-audit-log-events-allowed\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            },\n"
              + "            \"confluent-audit-log-events-denied\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            }\n"
              + "        }\n"
              + "    },\n"
              + "    \"default_topics\": {\n"
              + "        \"allowed\": \"confluent-audit-log-events-allowed\",\n"
              + "        \"denied\": \"\"\n"
              + "    }\n"
              + "}");

  static ConfluentAuditLogProvider
      denyProvider =
      providerWithCountExporter(
          "{\n"
              + "    \"destinations\": {\n"
              + "        \"topics\": {\n"
              + "            \"confluent-audit-log-events-allowed\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            },\n"
              + "            \"confluent-audit-log-events-denied\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            }\n"
              + "        }\n"
              + "    },\n"
              + "    \"default_topics\": {\n"
              + "        \"allowed\": \"\",\n"
              + "        \"denied\": \"confluent-audit-log-events-denied\"\n"
              + "    }\n"
              + "}");

  private static ConfluentAuditLogProvider providerWithCountExporter(String routerConfigJson) {
    try {
      Map<String, Object> configs = new HashMap<>();
      ConfluentAuditLogProvider provider = new ConfluentAuditLogProvider();
      configs.put(AUDIT_EVENT_ROUTER_CONFIG, routerConfigJson);
      configs.put(CRN_AUTHORITY_NAME_CONFIG, "mds.example.com");
      configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      configs.put(AuditLogConfig.EVENT_EXPORTER_CLASS_CONFIG, CountExporter.class.getName());
      provider.configure(configs);
      provider.onUpdate(new ClusterResource(CLUSTER_ID));
      provider.setMetrics(new Metrics());
      CompletableFuture<Void> startFuture = provider.start(configs)
          .toCompletableFuture();
      startFuture.get(10_000, TimeUnit.MILLISECONDS);
      return provider;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
