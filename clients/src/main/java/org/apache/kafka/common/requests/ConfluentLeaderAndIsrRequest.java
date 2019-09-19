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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.requests.LeaderAndIsrRequest.PartitionState;

public class ConfluentLeaderAndIsrRequest extends AbstractControlRequest implements LeaderAndIsrRequestBase {
    private static final Field.ComplexArray TOPIC_STATES = new Field.ComplexArray("topic_states", "Topic states");
    private static final Field.ComplexArray PARTITION_STATES = new Field.ComplexArray("partition_states", "Partition states");
    private static final Field.ComplexArray LIVE_LEADERS = new Field.ComplexArray("live_leaders", "Live leaders");

    // PartitionState fields
    private static final Field.Int32 LEADER = new Field.Int32("leader", "The broker id for the leader.");
    private static final Field.Int32 LEADER_EPOCH = new Field.Int32("leader_epoch", "The leader epoch.");
    private static final Field.Array ISR = new Field.Array("isr", INT32, "The in sync replica ids.");
    private static final Field.Int32 ZK_VERSION = new Field.Int32("zk_version", "The ZK version.");
    private static final Field.Array REPLICAS = new Field.Array("replicas", INT32, "The replica ids.");
    private static final Field.Array ADDING_REPLICAS = new Field.Array("adding_replicas", INT32,
            "The replica ids we are in the process of adding to the replica set during a reassignment.");
    private static final Field.Array REMOVING_REPLICAS = new Field.Array("removing_replicas", INT32,
            "The replica ids we are in the process of removing from the replica set during a reassignment.");
    private static final Field.Bool IS_NEW = new Field.Bool("is_new", "Whether the replica should have existed on the broker or not");

    // live_leaders fields
    private static final Field.Int32 END_POINT_ID = new Field.Int32("id", "The broker id");
    private static final Field.Str HOST = new Field.Str("host", "The hostname of the broker.");
    private static final Field.Int32  PORT = new Field.Int32("port", "The port on which the broker accepts requests.");

    private static final Field PARTITION_STATES_V0 = PARTITION_STATES.withFields(
            PARTITION_ID,
            CONTROLLER_EPOCH,
            LEADER,
            LEADER_EPOCH,
            ISR,
            ZK_VERSION,
            REPLICAS,
            IS_NEW);

    private static final Field PARTITION_STATES_V1 = PARTITION_STATES.withFields(
            PARTITION_ID,
            CONTROLLER_EPOCH,
            LEADER,
            LEADER_EPOCH,
            ISR,
            ZK_VERSION,
            REPLICAS,
            ADDING_REPLICAS,
            REMOVING_REPLICAS,
            IS_NEW);

    private static final Field TOPIC_STATES_V0 = TOPIC_STATES.withFields(
            TOPIC_NAME,
            TOPIC_ID,
            PARTITION_STATES_V0);

    // TOPIC_STATES_V1 adds two new fields - adding_replicas and removing_replicas
    private static final Field TOPIC_STATES_V1 = TOPIC_STATES.withFields(
            TOPIC_NAME,
            TOPIC_ID,
            PARTITION_STATES_V1);

    private static final Field LIVE_LEADERS_V0 = LIVE_LEADERS.withFields(
            END_POINT_ID,
            HOST,
            PORT);

    private static final Schema CONFLUENT_LEADER_AND_ISR_REQUEST_V0 = new Schema(
            CONTROLLER_ID,
            CONTROLLER_EPOCH,
            BROKER_EPOCH,
            TOPIC_STATES_V0,
            LIVE_LEADERS_V0);

    // CONFLUENT_LEADER_AND_ISR_REQUEST_V1 added two new fields - adding_replicas and removing_replicas.
    // These fields respectively specify the replica IDs we want to add or remove as part of a reassignment
    private static final Schema CONFLUENT_LEADER_AND_ISR_REQUEST_V1 = new Schema(
            CONTROLLER_ID,
            CONTROLLER_EPOCH,
            BROKER_EPOCH,
            TOPIC_STATES_V1,
            LIVE_LEADERS_V0);

    public static Schema[] schemaVersions() {
        return new Schema[]{CONFLUENT_LEADER_AND_ISR_REQUEST_V0, CONFLUENT_LEADER_AND_ISR_REQUEST_V1};
    }

    public static class Builder extends AbstractControlRequest.Builder<ConfluentLeaderAndIsrRequest> {
        private final Map<TopicPartition, PartitionState> partitionStates;
        private final Map<String, UUID> topicIds;
        private final Set<Node> liveLeaders;

        public Builder(short version, int controllerId, int controllerEpoch, long brokerEpoch,
                       Map<String, UUID> topicIds, Map<TopicPartition, PartitionState> partitionStates,
                       Set<Node> liveLeaders) {
            super(ApiKeys.CONFLUENT_LEADER_AND_ISR, version, controllerId, controllerEpoch, brokerEpoch);
            this.topicIds = topicIds;
            this.partitionStates = partitionStates;
            this.liveLeaders = liveLeaders;
        }

        @Override
        public ConfluentLeaderAndIsrRequest build(short version) {
            return new ConfluentLeaderAndIsrRequest(controllerId, controllerEpoch, brokerEpoch,
                    topicIds, partitionStates, liveLeaders, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=ConfluentLeaderAndIsrRequest")
                .append(", controllerId=").append(controllerId)
                .append(", controllerEpoch=").append(controllerEpoch)
                .append(", brokerEpoch=").append(brokerEpoch)
                .append(", topicIds=").append(topicIds)
                .append(", partitionStates=").append(partitionStates)
                .append(", liveLeaders=(").append(Utils.join(liveLeaders, ", ")).append(")")
                .append(")");
            return bld.toString();
        }
    }

    private final Map<TopicPartition, PartitionState> partitionStates;
    private final Map<String, UUID> topicIds = new HashMap<>();
    private final Set<Node> liveLeaders;

    private ConfluentLeaderAndIsrRequest(int controllerId, int controllerEpoch, long brokerEpoch,
                                         Map<String, UUID> topicIds,
                                         Map<TopicPartition, PartitionState> partitionStates,
                                         Set<Node> liveLeaders, short version) {
        super(ApiKeys.CONFLUENT_LEADER_AND_ISR, version, controllerId, controllerEpoch, brokerEpoch);
        this.topicIds.putAll(topicIds);
        this.partitionStates = partitionStates;
        this.liveLeaders = liveLeaders;
    }

    public ConfluentLeaderAndIsrRequest(Struct struct, short version) {
        super(ApiKeys.CONFLUENT_LEADER_AND_ISR, struct, version);

        Map<TopicPartition, PartitionState> partitionStates = new HashMap<>();
        if (struct.hasField(TOPIC_STATES)) {
            for (Object topicStatesDataObj : struct.get(TOPIC_STATES)) {
                Struct topicStatesData = (Struct) topicStatesDataObj;
                String topic = topicStatesData.get(TOPIC_NAME);
                topicIds.put(topic, topicStatesData.get(TOPIC_ID));

                for (Object partitionStateDataObj : topicStatesData.get(PARTITION_STATES)) {
                    Struct partitionStateData = (Struct) partitionStateDataObj;
                    int partition = partitionStateData.get(PARTITION_ID);
                    PartitionState partitionState = new PartitionState(partitionStateData);
                    partitionStates.put(new TopicPartition(topic, partition), partitionState);
                }
            }
        } else {
            for (Object partitionStateDataObj : struct.get(PARTITION_STATES)) {
                Struct partitionStateData = (Struct) partitionStateDataObj;
                String topic = partitionStateData.get(TOPIC_NAME);
                int partition = partitionStateData.get(PARTITION_ID);
                PartitionState partitionState = new PartitionState(partitionStateData);
                partitionStates.put(new TopicPartition(topic, partition), partitionState);
            }
        }

        Set<Node> leaders = new HashSet<>();
        for (Object leadersDataObj : struct.get(LIVE_LEADERS)) {
            Struct leadersData = (Struct) leadersDataObj;
            int id = leadersData.get(END_POINT_ID);
            String host = leadersData.get(HOST);
            int port = leadersData.get(PORT);
            leaders.add(new Node(id, host, port));
        }

        this.partitionStates = partitionStates;
        this.liveLeaders = leaders;
    }

    @Override
    protected Struct toStruct() {
        short version = version();
        Struct struct = new Struct(ApiKeys.CONFLUENT_LEADER_AND_ISR.requestSchema(version));
        struct.set(CONTROLLER_ID, controllerId);
        struct.set(CONTROLLER_EPOCH, controllerEpoch);
        struct.setIfExists(BROKER_EPOCH, brokerEpoch);

        if (struct.hasField(TOPIC_STATES)) {
            Map<String, Map<Integer, PartitionState>> topicStates = CollectionUtils.groupPartitionDataByTopic(partitionStates);
            List<Struct> topicStatesData = new ArrayList<>(topicStates.size());
            for (Map.Entry<String, Map<Integer, PartitionState>> entry : topicStates.entrySet()) {
                Struct topicStateData = struct.instance(TOPIC_STATES);
                topicStateData.set(TOPIC_NAME, entry.getKey());

                if (topicStateData.hasField(TOPIC_ID)) {
                    UUID topicId = topicIds.get(entry.getKey());
                    topicStateData.set(TOPIC_ID, topicId);
                }

                Map<Integer, PartitionState> partitionMap = entry.getValue();
                List<Struct> partitionStatesData = new ArrayList<>(partitionMap.size());
                for (Map.Entry<Integer, PartitionState> partitionEntry : partitionMap.entrySet()) {
                    Struct partitionStateData = topicStateData.instance(PARTITION_STATES);
                    partitionStateData.set(PARTITION_ID, partitionEntry.getKey());
                    partitionEntry.getValue().setStruct(partitionStateData, partitionStateVersion());
                    partitionStatesData.add(partitionStateData);
                }
                topicStateData.set(PARTITION_STATES, partitionStatesData.toArray());
                topicStatesData.add(topicStateData);
            }
            struct.set(TOPIC_STATES, topicStatesData.toArray());
        } else {
            List<Struct> partitionStatesData = new ArrayList<>(partitionStates.size());
            for (Map.Entry<TopicPartition, PartitionState> entry : partitionStates.entrySet()) {
                Struct partitionStateData = struct.instance(PARTITION_STATES);
                TopicPartition topicPartition = entry.getKey();
                partitionStateData.set(TOPIC_NAME, topicPartition.topic());
                partitionStateData.set(PARTITION_ID, topicPartition.partition());
                entry.getValue().setStruct(partitionStateData, partitionStateVersion());
                partitionStatesData.add(partitionStateData);
            }
            struct.set(PARTITION_STATES, partitionStatesData.toArray());
        }

        List<Struct> leadersData = new ArrayList<>(liveLeaders.size());
        for (Node leader : liveLeaders) {
            Struct leaderData = struct.instance(LIVE_LEADERS);
            leaderData.set(END_POINT_ID, leader.id());
            leaderData.set(HOST, leader.host());
            leaderData.set(PORT, leader.port());
            leadersData.add(leaderData);
        }
        struct.set(LIVE_LEADERS, leadersData.toArray());
        return struct;
    }

    /**
     * Because we reuse the AK LeaderAndIsr PartitionState object,
     * we need to map our ConfluentLeaderAndIsr versions to the PartitionState version we want
     */
    private short partitionStateVersion() {
        if (version() >= 1) {
            return 3;
        } else {
            return 2;
        }
    }

    @Override
    public ConfluentLeaderAndIsrResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);

        Map<TopicPartition, Errors> responses = new HashMap<>(partitionStates.size());
        for (TopicPartition partition : partitionStates.keySet()) {
            responses.put(partition, error);
        }

        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                return new ConfluentLeaderAndIsrResponse(error, responses);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.CONFLUENT_LEADER_AND_ISR.latestVersion()));
        }
    }

    public int controllerId() {
        return controllerId;
    }

    public int controllerEpoch() {
        return controllerEpoch;
    }

    @Override
    public Map<String, UUID> topicIds() {
        return topicIds;
    }

    @Override
    public Map<TopicPartition, PartitionState> partitionStates() {
        return partitionStates;
    }

    public static ConfluentLeaderAndIsrRequest parse(ByteBuffer buffer, short version) {
        return new ConfluentLeaderAndIsrRequest(ApiKeys.CONFLUENT_LEADER_AND_ISR.parseRequest(version, buffer), version);
    }
}
