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
package org.apache.kafka.common.record;

import org.apache.kafka.common.message.LeaderChangeMessageData;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

/**
 * Utility class for easy interaction with {@link LeaderChangeMessageData}.
 */
public class RaftLeaderChangeMessageUtils {

    public static LeaderChangeMessageData deserialize(Record record) {
        return deserialize(record.value().duplicate());
    }

    public static LeaderChangeMessageData deserialize(ByteBuffer data) {
        LeaderChangeMessageData leaderChangeMessage = new LeaderChangeMessageData();
        Struct leaderChangeMessageStruct = LeaderChangeMessageData.SCHEMAS[leaderChangeMessage.highestSupportedVersion()]
                                               .read(data);
        leaderChangeMessage.fromStruct(leaderChangeMessageStruct, leaderChangeMessage.highestSupportedVersion());
        return leaderChangeMessage;
    }

    static int getLeaderChangeMessageSize(LeaderChangeMessageData leaderChangeMessage) {
        return DefaultRecordBatch.RECORD_BATCH_OVERHEAD + DefaultRecord.sizeInBytes(0, 0L,
            ControlRecordType.CURRENT_CONTROL_RECORD_KEY_SIZE,
            leaderChangeMessage.toStruct(leaderChangeMessage.highestSupportedVersion()).sizeOf(),
            Record.EMPTY_HEADERS);
    }
}
