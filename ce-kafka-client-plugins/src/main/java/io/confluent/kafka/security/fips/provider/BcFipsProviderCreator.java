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
     * Create the security provider configured. The constructor parameter is for configuring DRBG.
     * The following comment is from BC FIPS user guide:
     * In order to make the default DRBG suitable for key generation, the default DRBG is configured to be prediction
     * resistant and this can strain the JVMs entropy source especially if hardware RNG is not available.
     * In situations where the amount of entropy is constrained the default DRBG for the provider can be configured to
     * use an DRBG chain based on a SHA-512 SP 800-90A DRBG as the internal DRBG providing a seed generation.
     * To configure this use “C:HYBRID;ENABLE{All};”
     */
    public Provider getProvider() {
        return new BouncyCastleFipsProvider("C:HYBRID;ENABLE{All};");
    }
}