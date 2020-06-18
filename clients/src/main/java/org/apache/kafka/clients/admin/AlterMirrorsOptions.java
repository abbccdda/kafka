/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.Confluent;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Options for {@link ConfluentAdmin#alterMirrors(List, AlterMirrorsOptions)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@Confluent
@InterfaceStability.Evolving
public class AlterMirrorsOptions extends AbstractOptions<AlterMirrorsOptions> {

    private boolean validateOnly = false;

    /**
     * Whether to validate the mirror control operation, but not actually perform it.
     */
    public boolean validateOnly() {
        return this.validateOnly;
    }

    /**
     * Sets whether to validate the mirror control operation, but not actually perform it.
     */
    public AlterMirrorsOptions validateOnly(boolean validateOnly) {
        this.validateOnly = validateOnly;
        return this;
    }
}
