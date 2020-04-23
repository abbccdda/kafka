// (Copyright) [2020 - 2020] Confluent, Inc.

package io.confluent.security.auth.provider;

import java.io.Closeable;
import java.util.Set;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.KafkaFuture;

/**
This interface allows a plug-in access to the dynamic configuration on the broker that it's
running on, without providing the full functionality of an AdminClient attached to the
internal listener
 */
public interface DynamicConfigurator extends Closeable {

  KafkaFuture<Config> getClusterConfig();

  AlterConfigsResult setClusterConfig(Set<ConfigEntry> configEntries);

  AlterConfigsResult deleteClusterConfig(Set<ConfigEntry> configEntries);
}
