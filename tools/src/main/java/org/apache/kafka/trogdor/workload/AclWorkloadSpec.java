/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import org.apache.kafka.trogdor.task.TaskController;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.task.TaskWorker;


public class AclWorkloadSpec extends TaskSpec {
    private final String clientNode;
    private final String bootstrapServers;
    private final int noOfThreads;
    private final int targetOperationsPerSec;
    private final int minSupportedOpsPerSec;
    private final boolean aclDeletes;
    private final String adminJaasConfig;
    private final String securityProtocol;
    private final String saslMechanism;

    @JsonCreator
    public AclWorkloadSpec(@JsonProperty("startMs") long startMs,
                           @JsonProperty("durationMs") long durationMs,
                           @JsonProperty("clientNode") String clientNode,
                           @JsonProperty("bootstrapServers") String bootstrapServers,
                           @JsonProperty("targetOperationsPerSec") int targetOperationsPerSec,
                           @JsonProperty("minSupportedOpsPerSec") int minSupportedOpsPerSec,
                           @JsonProperty("aclDeletes") boolean aclDeletes,
                           @JsonProperty("noOfThreads") int noOfThreads,
                           @JsonProperty("adminJaasConfig") String adminJaasConfig,
                           @JsonProperty("securityProtocol") String securityProtocol,
                           @JsonProperty("saslMechanism") String saslMechanism) {
        super(startMs, durationMs);
        this.clientNode = clientNode == null ? "" : clientNode;
        this.bootstrapServers = bootstrapServers == null ? "" : bootstrapServers;
        this.targetOperationsPerSec = targetOperationsPerSec;
        this.minSupportedOpsPerSec = minSupportedOpsPerSec;
        this.aclDeletes = aclDeletes;
        this.noOfThreads = noOfThreads;
        this.adminJaasConfig = adminJaasConfig;
        this.saslMechanism = saslMechanism;
        this.securityProtocol = securityProtocol;
    }

    @JsonProperty
    public String clientNode() {
        return clientNode;
    }

    @JsonProperty
    public String bootstrapServers() {
        return bootstrapServers;
    }

    @JsonProperty
    public int targetOperationsPerSec() {
        return targetOperationsPerSec;
    }

    @JsonProperty
    public int minSupportedOpsPerSec() {
        return minSupportedOpsPerSec;
    }

    @JsonProperty
    public int noOfThreads() {
        return noOfThreads;
    }

    @JsonProperty
    public boolean aclDeletes() {
        return aclDeletes;
    }

    @Override
    public TaskController newController(String id) {
        return topology -> Collections.singleton(clientNode);
    }

    @JsonProperty
    public String adminJaasConfig() {
        return adminJaasConfig;
    }

    @JsonProperty
    public String saslMechanism() {
        return saslMechanism;
    }

    @JsonProperty
    public String securityProtocol() {
        return securityProtocol;
    }

    @Override
    public TaskWorker newTaskWorker(String id) {
        return new AclBenchWorker(id, this);
    }
}