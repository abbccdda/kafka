/*
 * Copyright 2019 Confluent Inc.
 */

package io.confluent.kafka.security.fips.provider;

import java.security.Provider;
import org.apache.kafka.common.security.SecurityProviderCreator;
import org.bouncycastle.jsse.provider.BouncyCastleJsseProvider;

/*
 * Create Bouncy Castle FIPS compliant JSSE secuirty provider.
 */
public class BcFipsJsseProviderCreator implements SecurityProviderCreator {
    public BcFipsJsseProviderCreator() {}

    /**
     * Create the security provider configured
     */
    public Provider getProvider() {
        return new BouncyCastleJsseProvider("FIPS:BCFIPS");
    }
}