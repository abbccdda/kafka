/*
 * Copyright 2019 Confluent Inc.
 */

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Options for {@link ConfluentAdmin#replicaStatus(Set, ReplicaStatusOptions)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ReplicaStatusOptions extends AbstractOptions<ReplicaStatusOptions> {
}
