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

import static java.util.stream.Collectors.toList;

import org.apache.kafka.common.message.ConfluentLeaderAndIsrResponseData;
import org.apache.kafka.common.message.LeaderAndIsrResponseData;
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrPartitionError;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LeaderAndIsrResponse extends AbstractResponse {

    private final Message data;

    public LeaderAndIsrResponse(LeaderAndIsrResponseData data, boolean useConfluentResponse) {
        if (useConfluentResponse) {
            this.data = new ConfluentLeaderAndIsrResponseData()
                .setErrorCode(data.errorCode())
                .setPartitionErrors(data.partitionErrors().stream().map(pe ->
                    new ConfluentLeaderAndIsrResponseData.LeaderAndIsrPartitionError()
                        .setTopicName(pe.topicName())
                        .setPartitionIndex(pe.partitionIndex())
                        .setErrorCode(pe.errorCode())).collect(toList()));
        } else {
            this.data = data;
        }
    }

    public LeaderAndIsrResponse(Struct struct, short version, boolean useConfluentResponse) {
        if (useConfluentResponse)
            this.data = new ConfluentLeaderAndIsrResponseData(struct, version);
        else
            this.data = new LeaderAndIsrResponseData(struct, version);
    }

    public List<LeaderAndIsrPartitionError> partitions() {
        if (data instanceof ConfluentLeaderAndIsrResponseData) {
            return ((ConfluentLeaderAndIsrResponseData) data).partitionErrors().stream()
                .map(pe -> new LeaderAndIsrPartitionError()
                    .setErrorCode(pe.errorCode())
                    .setTopicName(pe.topicName())
                    .setPartitionIndex(pe.partitionIndex()))
                .collect(toList());
        }
        return ((LeaderAndIsrResponseData) data).partitionErrors();
    }

    public Errors error() {
        if (data instanceof ConfluentLeaderAndIsrResponseData)
            return Errors.forCode(((ConfluentLeaderAndIsrResponseData) data).errorCode());
        return Errors.forCode(((LeaderAndIsrResponseData) data).errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Errors error = error();
        if (error != Errors.NONE)
            // Minor optimization since the top-level error applies to all partitions
            return Collections.singletonMap(error, partitions().size());
        return errorCounts(partitions().stream().map(l -> Errors.forCode(l.errorCode())).collect(
            toList()));
    }

    public static LeaderAndIsrResponse parse(ByteBuffer buffer, short version) {
        return new LeaderAndIsrResponse(ApiKeys.LEADER_AND_ISR.parseResponse(version, buffer),
            version, false);
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public String toString() {
        return data.toString();
    }

}
