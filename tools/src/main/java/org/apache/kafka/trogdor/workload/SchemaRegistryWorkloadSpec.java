// (Copyright) [2019 - 2019] Confluent, Inc.

package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.trogdor.task.TaskController;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.task.TaskWorker;

import java.util.Collections;

public class SchemaRegistryWorkloadSpec extends TaskSpec {

  private final String clientNode;
  private final String schemaRegistryUrl;
  private final boolean parseResult;
  private final int numSchemas;
  private final int targetCallsPerSec;

  @JsonCreator
  public SchemaRegistryWorkloadSpec(@JsonProperty("startMs") long startMs,
                                    @JsonProperty("durationMs") long durationMs,
                                    @JsonProperty("clientNode") String clientNode,
                                    @JsonProperty("schemaRegistryUrl") String schemaRegistryUrl,
                                    @JsonProperty("parseResult") boolean parseResult,
                                    @JsonProperty("numSchemas") int numSchemas,
                                    @JsonProperty("targetCallsPerSec") int targetCallsPerSec) {
    super(startMs, durationMs);
    this.clientNode = clientNode == null ? "" : clientNode;
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.parseResult = parseResult;
    this.numSchemas = numSchemas;
    this.targetCallsPerSec = targetCallsPerSec;
  }

  @JsonProperty
  public String clientNode() {
    return clientNode;
  }

  @JsonProperty
  public int targetCallsPerSec() {
    return targetCallsPerSec;
  }

  @JsonProperty
  public boolean parseResult() {
    return parseResult;
  }

  @JsonProperty
  public int numSchemas() {
    return numSchemas;
  }

  @JsonProperty
  public String schemaRegistryUrl() {
    return schemaRegistryUrl;
  }

  @Override
  public TaskController newController(String id) {
    return topology -> Collections.singleton(clientNode);
  }

  @Override
  public TaskWorker newTaskWorker(String id) {
    return new SchemaRegistryWorker(id, this);
  }
}
