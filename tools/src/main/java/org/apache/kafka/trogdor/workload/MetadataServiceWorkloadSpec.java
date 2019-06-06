// (Copyright) [2019 - 2019] Confluent, Inc.

package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.trogdor.task.TaskController;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.task.TaskWorker;

import java.util.Collections;

public class MetadataServiceWorkloadSpec extends TaskSpec {
    private final String clientNode;
    private final String seedMetadataServerUrl;
    private final String adminUserCredentials;
    private final int numRoleBindings;
    private final int targetCallsPerSec;

    @JsonCreator
    public MetadataServiceWorkloadSpec(@JsonProperty("startMs") long startMs,
                                       @JsonProperty("durationMs") long durationMs,
                                       @JsonProperty("clientNode") String clientNode,
                                       @JsonProperty("seedMetadataServerUrl") String seedMetadataServerUrl,
                                       @JsonProperty("adminUserCredentials") String adminUserCredentials,
                                       @JsonProperty("numRoleBindings") int numRoleBindings,
                                       @JsonProperty("targetCallsPerSec") int targetCallsPerSec) {
        super(startMs, durationMs);
        this.clientNode = clientNode == null ? "" : clientNode;
        this.seedMetadataServerUrl = seedMetadataServerUrl == null ? "" : seedMetadataServerUrl;
        this.adminUserCredentials = adminUserCredentials == null ? "" : adminUserCredentials;
        this.numRoleBindings = numRoleBindings;
        this.targetCallsPerSec = targetCallsPerSec;
    }

    @JsonProperty
    public String clientNode() {
        return clientNode;
    }

    @JsonProperty
    public String adminUserCredentials() {
        return adminUserCredentials;
    }

    @JsonProperty
    public String seedMetadataServerUrl() {
        return seedMetadataServerUrl;
    }

    @JsonProperty
    public int numRoleBindings() {
        return numRoleBindings;
    }

    @JsonProperty
    public int targetCallsPerSec() {
        return targetCallsPerSec;
    }

    @Override
    public TaskController newController(String id) {
        return topology -> Collections.singleton(clientNode);
    }

    @Override
    public TaskWorker newTaskWorker(String id) {
        return new MetadataServiceWorker(id, this);
    }
}
