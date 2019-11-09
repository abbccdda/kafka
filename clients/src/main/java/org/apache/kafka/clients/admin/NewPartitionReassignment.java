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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A new partition reassignment, which can be applied via {@link AdminClient#alterPartitionReassignments(Map, AlterPartitionReassignmentsOptions)}.
 *
 * {@code targetReplicas} is the total set of brokers assigned while {@code targetObservers} is the set brokers that will act as observers. The size of {@code targetReplicas} must be greater than the size of {@code targetObservers} and every {@code targetObservers} must be included in {@code targetReplicas}.
 */
public class NewPartitionReassignment {
    private final List<Integer> targetReplicas;
    private final List<Integer> targetObservers;

    /**
     * @throws IllegalArgumentException if no replicas are supplied
     */
    public static NewPartitionReassignment ofReplicasAndObservers(List<Integer> replicas,
                                                                  List<Integer> observers) {
        return new NewPartitionReassignment(replicas, observers);
    }

    /**
     * @throws IllegalArgumentException if no replicas are supplied
     */
    public NewPartitionReassignment(List<Integer> targetReplicas) {
        this(targetReplicas, Collections.emptyList());
    }

    private NewPartitionReassignment(List<Integer> targetReplicas, List<Integer> targetObservers) {
        if (targetReplicas == null || targetReplicas.size() == 0) {
            throw new IllegalArgumentException("Cannot create a new partition reassignment without any replicas");
        }
        if (targetObservers == null) {
            throw new IllegalArgumentException("Cannot create a new partition reassignment with null observers");
        }

        this.targetReplicas = Collections.unmodifiableList(new ArrayList<>(targetReplicas));
        this.targetObservers = Collections.unmodifiableList(new ArrayList<>(targetObservers));
    }

    public List<Integer> targetReplicas() {
        return targetReplicas;
    }

    public List<Integer> targetObservers() {
        return targetObservers;
    }
}
