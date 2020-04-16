/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.Confluent;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Options for {@link ConfluentAdmin#listClusterLinks(ListClusterLinksOptions)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@Confluent
@InterfaceStability.Evolving
public class ListClusterLinksOptions extends AbstractOptions<ListClusterLinksOptions> {
}
