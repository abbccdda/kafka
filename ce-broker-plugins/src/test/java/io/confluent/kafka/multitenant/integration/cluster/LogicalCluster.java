// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.multitenant.integration.cluster;

import io.confluent.kafka.test.utils.SecurityTestUtils;
import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulation of a logical cluster that represents a cluster from the point of
 * view of a single tenant. Each multi-tenant {@link PhysicalCluster} consists of
 * one or more logical clusters. See {@link PhysicalCluster} for more details on
 * the user model.
 */
public class LogicalCluster {
  private final String logicalClusterId;
  private final PhysicalCluster physicalCluster;
  private final Map<Integer, LogicalClusterUser> users;
  private final LogicalClusterUser adminUser;

  public LogicalCluster(PhysicalCluster physicalCluster, String logicalClusterId, UserMetadata adminUser) {
    this.physicalCluster = physicalCluster;
    this.logicalClusterId = logicalClusterId;
    this.users = new HashMap<>();
    this.adminUser = addUser(adminUser);
  }

  public synchronized LogicalClusterUser addUser(UserMetadata user) {
    int userId = user.userId();
    if (users.containsKey(userId)) {
      throw new IllegalArgumentException("User " + userId + " already exists in logical cluster");
    }
    LogicalClusterUser logicalClusterUser = new LogicalClusterUser(user, logicalClusterId);
    this.users.put(userId, logicalClusterUser);

    SecurityTestUtils.createScramUser(physicalCluster.kafkaCluster(),
        logicalClusterUser.saslUserName(),
        user.apiSecret());
    return logicalClusterUser;
  }

  public synchronized void removeUser(int userId) {
    this.users.remove(userId);
  }

  public synchronized LogicalClusterUser user(int userId) {
    return this.users.get(userId);
  }

  public synchronized LogicalClusterUser adminUser() {
    return adminUser;
  }

  public String logicalClusterId() {
    return logicalClusterId;
  }

}