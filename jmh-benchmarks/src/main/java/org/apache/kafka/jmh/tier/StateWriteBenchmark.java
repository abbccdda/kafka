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

package org.apache.kafka.jmh.tier;

import kafka.log.LogConfig;
import kafka.tier.TopicIdPartition;
import kafka.tier.domain.TierTopicInitLeader;
import kafka.tier.state.TierPartitionStateFactory;
import kafka.tier.state.TierPartitionState;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@org.openjdk.jmh.annotations.State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class StateWriteBenchmark {
    private static final int COUNT = 10000;
    private static final String BASE_DIR = System.getProperty("java.io.tmpdir");
    private static final int EPOCH = 0;
    private static final TopicIdPartition TOPIC_PARTITION = new TopicIdPartition("mytopic", UUID.randomUUID(), 0);

    private void writeState(TierPartitionState state) throws IOException {
        state.beginCatchup();
        state.setTopicId(TOPIC_PARTITION.topicId());
        state.append(new TierTopicInitLeader(TOPIC_PARTITION, EPOCH,
                UUID.randomUUID(), 0));
        for (int i = 0; i < COUNT; i++) {
            TierUtils.uploadWithMetadata(state,
                    TOPIC_PARTITION,
                    EPOCH,
                    UUID.randomUUID(),
                    i * 2,
                    i * 2 + 1,
                    i,
                    i,
                    false,
                    true,
                    false);
        }
        state.flush();
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void appendReadByteBufferBench() throws Exception {
        LogConfig config = mock(LogConfig.class);
        when(config.tierEnable()).thenReturn(true);

        TierPartitionStateFactory factory = new TierPartitionStateFactory(true);
        TierPartitionState state = factory.initState(new File(BASE_DIR), TOPIC_PARTITION.topicPartition(), config);
        try {
            writeState(state);
        } finally {
            state.delete();
        }
    }
}
