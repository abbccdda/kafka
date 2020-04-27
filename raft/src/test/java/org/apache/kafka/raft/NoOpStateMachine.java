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
package org.apache.kafka.raft;

import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;

import java.util.Collections;
import java.util.Optional;

public class NoOpStateMachine implements DistributedStateMachine {
    private OffsetAndEpoch position = new OffsetAndEpoch(0, 0);

    private Optional<EpochState> state = Optional.empty();

    @Override
    public void becomeLeader(int epoch) {
        this.state = Optional.of(new LeaderState(0, epoch, 0L, Collections.emptySet()));
    }

    @Override
    public void becomeFollower(int epoch) {
        this.state = Optional.of(new FollowerState(epoch));
    }

    Optional<EpochState> state() {
        return state;
    }

    @Override
    public synchronized OffsetAndEpoch position() {
        return position;
    }

    @Override
    public synchronized void apply(Records records) {
        for (RecordBatch batch : records.batches()) {
            this.position = new OffsetAndEpoch(batch.lastOffset() + 1, batch.partitionLeaderEpoch());
        }
    }

    @Override
    public synchronized boolean accept(Records records) {
        return true;
    }

    void clear() {
        state = Optional.empty();
    }
}
