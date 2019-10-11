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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/** Replicate consumer group state between clusters. Emits checkpoint records.
 *
 *  @see MirrorConnectorConfig for supported config properties.
 */
public class MirrorCheckpointConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(MirrorCheckpointConnector.class);

    private Scheduler scheduler;
    private MirrorConnectorConfig config;
    private GroupFilter groupFilter;
    private AdminClient sourceAdminClient;
    private SourceAndTarget sourceAndTarget;
    private String connectorName;
    private List<String> knownConsumerGroups = Collections.emptyList();

    @Override
    public void start(Map<String, String> props) {
        config = new MirrorConnectorConfig(props);
        if (!config.enabled()) {
            return;
        }
        connectorName = config.connectorName();
        sourceAndTarget = new SourceAndTarget(config.sourceClusterAlias(), config.targetClusterAlias());
        groupFilter = config.groupFilter();
        sourceAdminClient = AdminClient.create(config.sourceAdminConfig());
        scheduler = new Scheduler(MirrorCheckpointConnector.class, config.adminTimeout());
        scheduler.execute(this::createInternalTopics, "creating internal topics");
        scheduler.execute(this::loadInitialConsumerGroups, "loading initial consumer groups");
        scheduler.scheduleRepeatingDelayed(this::refreshConsumerGroups, config.refreshGroupsInterval(),
                "refreshing consumer groups");
        log.info("Started {} with {} consumer groups.", connectorName, knownConsumerGroups.size());
        log.debug("Started {} with consumer groups: {}", connectorName, knownConsumerGroups);
    }

    @Override
    public void stop() {
        if (!config.enabled()) {
            return;
        }
        Utils.closeQuietly(scheduler, "scheduler");
        Utils.closeQuietly(groupFilter, "group filter");
        Utils.closeQuietly(sourceAdminClient, "source admin client");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MirrorCheckpointTask.class;
    }

    // divide consumer groups among tasks
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (!config.enabled() || knownConsumerGroups.isEmpty()) {
            return Collections.emptyList();
        }
        int numTasks = Math.min(maxTasks, knownConsumerGroups.size());
        return ConnectorUtils.groupPartitions(knownConsumerGroups, numTasks).stream()
                .map(config::taskConfigForConsumerGroups)
                .collect(Collectors.toList());
    }

    @Override
    public ConfigDef config() {
        return MirrorConnectorConfig.CONNECTOR_CONFIG_DEF;
    }

    @Override
    public String version() {
        return "1";
    }

    private void refreshConsumerGroups()
            throws InterruptedException, ExecutionException {
        List<String> consumerGroups = findConsumerGroups();
        Set<String> newConsumerGroups = new HashSet<>();
        newConsumerGroups.addAll(consumerGroups);
        newConsumerGroups.removeAll(knownConsumerGroups);
        Set<String> deadConsumerGroups = new HashSet<>();
        deadConsumerGroups.addAll(knownConsumerGroups);
        deadConsumerGroups.removeAll(consumerGroups);
        if (!newConsumerGroups.isEmpty() || !deadConsumerGroups.isEmpty()) {
            log.info("Found {} consumer groups for {}. {} are new. {} were removed. Previously had {}.",
                    consumerGroups.size(), sourceAndTarget, newConsumerGroups.size(), deadConsumerGroups.size(),
                    knownConsumerGroups.size());
            log.debug("Found new consumer groups: {}", newConsumerGroups);
            knownConsumerGroups = consumerGroups;
            context.requestTaskReconfiguration();
        }
    }

    private void loadInitialConsumerGroups()
            throws InterruptedException, ExecutionException {
        knownConsumerGroups = findConsumerGroups();
    }

    private List<String> findConsumerGroups()
            throws InterruptedException, ExecutionException {
        return listConsumerGroups().stream()
                .filter(x -> !x.isSimpleConsumerGroup())
                .map(x -> x.groupId())
                .filter(this::shouldReplicate)
                .collect(Collectors.toList());
    }

    private Collection<ConsumerGroupListing> listConsumerGroups()
            throws InterruptedException, ExecutionException {
        return sourceAdminClient.listConsumerGroups().valid().get();
    }

    private void createInternalTopics() {
        MirrorUtils.createSinglePartitionCompactedTopic(config.checkpointsTopic(),
            config.checkpointsTopicReplicationFactor(), config.targetAdminConfig());
    } 

    boolean shouldReplicate(String group) {
        return groupFilter.shouldReplicateGroup(group);
    }
}
