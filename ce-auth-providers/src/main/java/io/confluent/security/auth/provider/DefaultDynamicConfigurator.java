// (Copyright) [2020 - 2020] Confluent, Inc.

package io.confluent.security.auth.provider;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.utils.Utils;

public class DefaultDynamicConfigurator implements DynamicConfigurator {

  private final ConfluentAdmin adminClient;
  private final ConfigResource clusterResource = new ConfigResource(Type.BROKER, "");

  public DefaultDynamicConfigurator(ConfluentAdmin adminClient) {
    this.adminClient = adminClient;
  }

  @Override
  public KafkaFuture<Config> getClusterConfig() {
    return adminClient.describeConfigs(Collections.singleton(clusterResource))
        .values().get(clusterResource);
  }

  @Override
  public AlterConfigsResult setClusterConfig(Set<ConfigEntry> configEntries) {
    return adminClient.incrementalAlterConfigs(
        Utils.mkMap(Utils.mkEntry(clusterResource,
            configEntries.stream()
                .map(entry -> new AlterConfigOp(entry, OpType.SET))
                .collect(Collectors.toSet()))));
  }

  @Override
  public AlterConfigsResult deleteClusterConfig(Set<ConfigEntry> configEntries) {
    return adminClient.incrementalAlterConfigs(
        Utils.mkMap(Utils.mkEntry(clusterResource,
            configEntries.stream()
                .map(entry -> new AlterConfigOp(entry, OpType.DELETE))
                .collect(Collectors.toSet()))));
  }

  @Override
  public void close() {
    Utils.closeQuietly(adminClient, "DefaultDynamicConfigurator.adminClient");
  }
}
