/**
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.common;

import java.util.Collection;
import java.util.Set;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;

/**
 * The result of a #{@link org.apache.kafka.clients.admin.DescribeClusterResult}
 */
public class KafkaCluster {
  private final Collection<Node> nodes;
  private final Node controller;
  private final String clusterId;
  private final Set<AclOperation> authorizedOperations;

  public KafkaCluster(Collection<Node> nodes,
                      Node controller,
                      String clusterId,
                      Set<AclOperation> authorizedOperations) {
    this.nodes = nodes;
    this.controller = controller;
    this.clusterId = clusterId;
    this.authorizedOperations = authorizedOperations;
  }

  /**
   * A collection of nodes that consist the Kafka Cluster.
   */
  public Collection<Node> nodes() {
    return nodes;
  }

  /**
   * The current controller id.
   * Note that this may yield null, if the controller ID is not yet known.
   */
  public Node controller() {
    return controller;
  }

  /**
   * The current cluster id.
   */
  public String clusterId() {
    return clusterId;
  }

  /**
   * The authorized operations. The value will be non-null if the
   * broker supplied this information, and null otherwise.
   */
  public Set<AclOperation> authorizedOperations() {
    return authorizedOperations;
  }
}
