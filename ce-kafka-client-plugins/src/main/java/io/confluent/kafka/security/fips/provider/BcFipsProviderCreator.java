/*
 * Copyright 2019 Confluent Inc.
 */

package io.confluent.kafka.security.fips.provider;

import java.security.Provider;

import org.apache.kafka.common.security.SecurityProviderCreator;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;


/*
 * Create Bouncy Castle FIPS compliant secuirty provider.
 */
public class BcFipsProviderCreator implements SecurityProviderCreator {

    public BcFipsProviderCreator() {}

    /**
     * Create the security provider configured
     */
    public Provider getProvider() {
        return new BouncyCastleFipsProvider();
    }
}