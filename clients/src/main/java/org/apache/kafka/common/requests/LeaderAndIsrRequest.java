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

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;

import java.util.UUID;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.ConfluentLeaderAndIsrRequestData;
import org.apache.kafka.common.message.LeaderAndIsrRequestData;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrLiveLeader;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrTopicState;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState;
import org.apache.kafka.common.message.LeaderAndIsrResponseData;
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrPartitionError;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.FlattenedIterator;
import org.apache.kafka.common.utils.MappedIterator;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LeaderAndIsrRequest extends AbstractControlRequest {

    public static class Builder extends AbstractControlRequest.Builder<LeaderAndIsrRequest> {

        private final List<LeaderAndIsrPartitionState> partitionStates;
        private final Collection<Node> liveLeaders;
        private final Map<String, UUID> topicIds;

        public Builder(short version, int controllerId, int controllerEpoch, long brokerEpoch,
            List<LeaderAndIsrPartitionState> partitionStates, Collection<Node> liveLeaders) {
            this(ApiKeys.LEADER_AND_ISR, version, controllerId, controllerEpoch, brokerEpoch,
                partitionStates, liveLeaders, emptyMap());
        }

        private Builder(ApiKeys apiKey, short version, int controllerId, int controllerEpoch,
                        long brokerEpoch, List<LeaderAndIsrPartitionState> partitionStates,
                        Collection<Node> liveLeaders, Map<String, UUID> topicIds) {
            super(apiKey, version, controllerId, controllerEpoch, brokerEpoch);
            this.partitionStates = partitionStates;
            this.liveLeaders = liveLeaders;
            this.topicIds = topicIds;
        }

        public static Builder create(short version, int controllerId, int controllerEpoch,
                                     long brokerEpoch,
                                     List<LeaderAndIsrPartitionState> partitionStates,
                                     Collection<Node> liveLeaders,
                                     Map<String, UUID> topicIds,
                                     boolean useConfluentRequest) {
            ApiKeys apiKey = ApiKeys.LEADER_AND_ISR;
            if (useConfluentRequest) {
                apiKey = ApiKeys.CONFLUENT_LEADER_AND_ISR;
                if (version >= 3)
                    version = 1;
                else
                    version = 0;
            }
            return new Builder(apiKey, version, controllerId, controllerEpoch, brokerEpoch, partitionStates,
                liveLeaders, topicIds);
        }

        @Override
        public LeaderAndIsrRequest build(short version) {
            Message data;
            if (apiKey() == ApiKeys.CONFLUENT_LEADER_AND_ISR) {
                data = buildConfluentLeaderAndIsrData();
            } else {
                data = buildLeaderAndIsrData(version);
            }
            return new LeaderAndIsrRequest(data, version);
        }

        private ConfluentLeaderAndIsrRequestData buildConfluentLeaderAndIsrData() {
            List<ConfluentLeaderAndIsrRequestData.LeaderAndIsrLiveLeader> leaders = liveLeaders.stream().map(n ->
                new ConfluentLeaderAndIsrRequestData.LeaderAndIsrLiveLeader()
                    .setBrokerId(n.id())
                    .setHostName(n.host())
                    .setPort(n.port())
            ).collect(Collectors.toList());

            Map<String, ConfluentLeaderAndIsrRequestData.LeaderAndIsrTopicState> topicStatesMap =
                groupByConfluentTopic(partitionStates, topicIds);

            return new ConfluentLeaderAndIsrRequestData()
                .setControllerId(controllerId)
                .setControllerEpoch(controllerEpoch)
                .setBrokerEpoch(brokerEpoch)
                .setLiveLeaders(leaders)
                .setTopicStates(new ArrayList<>(topicStatesMap.values()));
        }

        private LeaderAndIsrRequestData buildLeaderAndIsrData(short version) {
            List<LeaderAndIsrLiveLeader> leaders = liveLeaders.stream().map(n -> new LeaderAndIsrLiveLeader()
                .setBrokerId(n.id())
                .setHostName(n.host())
                .setPort(n.port())
            ).collect(Collectors.toList());

            LeaderAndIsrRequestData data = new LeaderAndIsrRequestData()
                .setControllerId(controllerId)
                .setControllerEpoch(controllerEpoch)
                .setBrokerEpoch(brokerEpoch)
                .setLiveLeaders(leaders);

            if (version >= 2) {
                Map<String, LeaderAndIsrTopicState> topicStatesMap = groupByTopic(partitionStates);
                data.setTopicStates(new ArrayList<>(topicStatesMap.values()));
            } else {
                data.setUngroupedPartitionStates(partitionStates);
            }
            return data;
        }

        private static Map<String, ConfluentLeaderAndIsrRequestData.LeaderAndIsrTopicState> groupByConfluentTopic(
                List<LeaderAndIsrPartitionState> partitionStates, Map<String, UUID> topicIds) {
            Map<String, ConfluentLeaderAndIsrRequestData.LeaderAndIsrTopicState> topicStates = new HashMap<>();
            for (LeaderAndIsrPartitionState partition : partitionStates) {
                ConfluentLeaderAndIsrRequestData.LeaderAndIsrTopicState topicState =
                    topicStates.computeIfAbsent(partition.topicName(), t ->
                        new ConfluentLeaderAndIsrRequestData.LeaderAndIsrTopicState()
                            .setTopicName(partition.topicName())
                            .setTopicId(topicIds.get(t)));
                topicState.partitionStates().add(new ConfluentLeaderAndIsrRequestData.LeaderAndIsrPartitionState()
                    .setPartitionIndex(partition.partitionIndex())
                    .setControllerEpoch(partition.controllerEpoch())
                    .setLeader(partition.leader())
                    .setLeaderEpoch(partition.leaderEpoch())
                    .setIsr(partition.isr())
                    .setZkVersion(partition.zkVersion())
                    .setReplicas(partition.replicas())
                    .setIsNew(partition.isNew())
                    .setAddingReplicas(partition.addingReplicas())
                    .setRemovingReplicas(partition.removingReplicas()));
            }
            return topicStates;
        }

        private static Map<String, LeaderAndIsrTopicState> groupByTopic(List<LeaderAndIsrPartitionState> partitionStates) {
            Map<String, LeaderAndIsrTopicState> topicStates = new HashMap<>();
            // We don't null out the topic name in LeaderAndIsrRequestPartition since it's ignored by
            // the generated code if version >= 2
            for (LeaderAndIsrPartitionState partition : partitionStates) {
                LeaderAndIsrTopicState topicState = topicStates.computeIfAbsent(partition.topicName(),
                    t -> new LeaderAndIsrTopicState().setTopicName(partition.topicName()));
                topicState.partitionStates().add(partition);
            }
            return topicStates;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=LeaderAndIsRequest")
                .append(", controllerId=").append(controllerId)
                .append(", controllerEpoch=").append(controllerEpoch)
                .append(", brokerEpoch=").append(brokerEpoch)
                .append(", partitionStates=").append(partitionStates)
                .append(", liveLeaders=(").append(Utils.join(liveLeaders, ", ")).append(")")
                .append(")");
            return bld.toString();

        }
    }

    private final Message data;

    // Visible for testing
    LeaderAndIsrRequest(LeaderAndIsrRequestData data, short version) {
        this((Message) data, version);
    }

    private LeaderAndIsrRequest(Message data, short version) {
        super(data instanceof ConfluentLeaderAndIsrRequestData ? ApiKeys.CONFLUENT_LEADER_AND_ISR :
            ApiKeys.LEADER_AND_ISR, version);
        this.data = data;
        // Do this from the constructor to make it thread-safe (even though it's only needed when some methods are called)
        normalize();
    }

    private void normalize() {
        // We normalize the standard `LeaderAndIsrRequestData` and fallback to on the fly conversions
        // for ConfluentLeaderAndIsrRequestData. The goal is to remove the need for the custom
        // ConfluentLeaderAndIsrRequestData soon so we pay the efficiency overhead instead of
        // trying to optimize it.
        if (!(data instanceof ConfluentLeaderAndIsrRequestData)) {
            LeaderAndIsrRequestData requestData = (LeaderAndIsrRequestData) data;
            if (version() >= 2) {
                for (LeaderAndIsrTopicState topicState : requestData.topicStates()) {
                    for (LeaderAndIsrPartitionState partitionState : topicState.partitionStates()) {
                        // Set the topic name so that we can always present the ungrouped view to callers
                        partitionState.setTopicName(topicState.topicName());
                    }
                }
            }
        }
    }

    public LeaderAndIsrRequest(Struct struct, short version, boolean useConfluentRequest) {
        this(useConfluentRequest ? new ConfluentLeaderAndIsrRequestData(struct, version) :
            new LeaderAndIsrRequestData(struct, version), version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    protected ByteBuffer toBytes() {
        ByteBuffer bytes = ByteBuffer.allocate(size());
        data().write(new ByteBufferAccessor(bytes), version());
        bytes.flip();
        return bytes;
    }

    @Override
    public LeaderAndIsrResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        LeaderAndIsrResponseData responseData = new LeaderAndIsrResponseData();
        Errors error = Errors.forException(e);
        responseData.setErrorCode(error.code());

        List<LeaderAndIsrPartitionError> partitions = new ArrayList<>();
        for (LeaderAndIsrPartitionState partition : partitionStates()) {
            partitions.add(new LeaderAndIsrPartitionError()
                .setTopicName(partition.topicName())
                .setPartitionIndex(partition.partitionIndex())
                .setErrorCode(error.code()));
        }
        responseData.setPartitionErrors(partitions);

        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
            case 2:
            case 3:
                return new LeaderAndIsrResponse(responseData, isConfluentRequest());
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.LEADER_AND_ISR.latestVersion()));
        }
    }

    @Override
    public int controllerId() {
        if (data instanceof ConfluentLeaderAndIsrRequestData)
            return ((ConfluentLeaderAndIsrRequestData) data).controllerId();
        return ((LeaderAndIsrRequestData) data).controllerId();
    }

    @Override
    public int controllerEpoch() {
        if (data instanceof ConfluentLeaderAndIsrRequestData)
            return ((ConfluentLeaderAndIsrRequestData) data).controllerEpoch();
        return ((LeaderAndIsrRequestData) data).controllerEpoch();
    }

    @Override
    public long brokerEpoch() {
        if (data instanceof ConfluentLeaderAndIsrRequestData)
            return ((ConfluentLeaderAndIsrRequestData) data).brokerEpoch();
        return ((LeaderAndIsrRequestData) data).brokerEpoch();
    }

    public Iterable<LeaderAndIsrPartitionState> partitionStates() {
        if (data instanceof ConfluentLeaderAndIsrRequestData) {
            ConfluentLeaderAndIsrRequestData requestData = (ConfluentLeaderAndIsrRequestData) data;
            return () -> new FlattenedIterator<>(requestData.topicStates().iterator(), topic ->
                new MappedIterator<>(topic.partitionStates().iterator(), partition ->
                    new LeaderAndIsrPartitionState()
                        .setTopicName(topic.topicName())
                        .setPartitionIndex(partition.partitionIndex())
                        .setControllerEpoch(partition.controllerEpoch())
                        .setLeader(partition.leader())
                        .setLeaderEpoch(partition.leaderEpoch())
                        .setIsr(partition.isr())
                        .setZkVersion(partition.zkVersion())
                        .setReplicas(partition.replicas())
                        .setIsNew(partition.isNew())
                        .setAddingReplicas(partition.addingReplicas())
                        .setRemovingReplicas(partition.removingReplicas())));
        }
        LeaderAndIsrRequestData requestData = (LeaderAndIsrRequestData) data;
        if (version() >= 2)
            return () -> new FlattenedIterator<>(requestData.topicStates().iterator(),
                topicState -> topicState.partitionStates().iterator());
        return requestData.ungroupedPartitionStates();
    }

    public List<LeaderAndIsrLiveLeader> liveLeaders() {
        if (data instanceof ConfluentLeaderAndIsrRequestData) {
            return ((ConfluentLeaderAndIsrRequestData) data).liveLeaders().stream().map(leader ->
                new LeaderAndIsrLiveLeader()
                    .setBrokerId(leader.brokerId())
                    .setHostName(leader.hostName())
                    .setPort(leader.port())
            ).collect(Collectors.toList());
        }

        return ((LeaderAndIsrRequestData) data).liveLeaders();
    }

    public Map<String, UUID> topicIds() {
        if (data instanceof ConfluentLeaderAndIsrRequestData) {
            ConfluentLeaderAndIsrRequestData requestData = (ConfluentLeaderAndIsrRequestData) data;
            return requestData.topicStates().stream().collect(toMap(ts -> ts.topicName(), ts -> ts.topicId()));
        }
        return emptyMap();
    }

    protected int size() {
        return data.size(version());
    }

    public boolean isConfluentRequest() {
        return data instanceof ConfluentLeaderAndIsrRequestData;
    }

    protected Message data() {
        return data;
    }

    public static LeaderAndIsrRequest parse(ByteBuffer buffer, short version) {
        return new LeaderAndIsrRequest(ApiKeys.LEADER_AND_ISR.parseRequest(version, buffer), version, false);
    }

}
