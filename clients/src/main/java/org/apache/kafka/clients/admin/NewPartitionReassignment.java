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

package org.apache.kafka.clients.admin;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A new partition reassignment, which can be applied via {@link AdminClient#alterPartitionReassignments(Map, AlterPartitionReassignmentsOptions)}.
 *
 * {@code targetReplicas} is the total set of brokers assigned while {@code targetObservers} is the set brokers that will act as observers. The size of {@code targetReplicas} must be greater than the size of {@code targetObservers} and every {@code targetObservers} must be included in {@code targetReplicas}.
 */
public class NewPartitionReassignment {
    private final List<Integer> targetBrokers;
    private final List<Integer> targetObservers;

    public static Optional<NewPartitionReassignment> of(Integer... brokers) {
        return Optional.of(new NewPartitionReassignment(Arrays.asList(brokers)));
    }

    public NewPartitionReassignment(List<Integer> targetBrokers) {
        this.targetBrokers = Collections.unmodifiableList(new ArrayList<>(targetBrokers));
        this.targetObservers = Collections.emptyList();
    }

    public NewPartitionReassignment(List<Integer> targetBrokers, List<Integer> targetObservers) {
        this.targetBrokers = Collections.unmodifiableList(new ArrayList<>(targetBrokers));
        this.targetObservers = Collections.unmodifiableList(new ArrayList<>(targetObservers));
    }

    public List<Integer> targetBrokers() {
        return targetBrokers;
    }

    public List<Integer> targetObservers() {
        return targetObservers;
    }
}
