/*
 * Copyright 2019 Confluent Inc.
 */

package io.confluent.kafka.security.fips;

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import io.confluent.kafka.security.fips.exceptions.InvalidFipsTlsCipherSuiteException;
import io.confluent.kafka.security.fips.exceptions.InvalidFipsBrokerProtocolException;
import io.confluent.kafka.security.fips.exceptions.InvalidFipsTlsVersionException;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.Test;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;


public class FipsValidatorTest {
    @Test(expected = InvalidFipsTlsCipherSuiteException.class)
    public void testInvalidFipsTlsCipherSuitesConfigured() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG,
                    Arrays.asList("TLS_DHE_DSS_WITH_DES_EDE_CBC_SHA", "TLS_DHE_DSS_WITH_AES_92_CBC_SHA"));
        FipsValidator.validateFipsTls(config);
    }

    @Test(expected = InvalidFipsTlsVersionException.class)
    public void testInvalidFipsTlsVersionsConfigured() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, Arrays.asList("TLSv1.0", "SSL3.0"));
        FipsValidator.validateFipsTls(config);
    }

    @Test(expected = InvalidFipsBrokerProtocolException.class)
    public void testInvalidFipsBrokerProtocolsConfigured() throws Exception {
        Map<ListenerName, SecurityProtocol> securityProtocols = new HashMap<>();
        securityProtocols.put(new ListenerName("External"), SecurityProtocol.SASL_PLAINTEXT);
        securityProtocols.put(new ListenerName("Internal"), SecurityProtocol.SASL_PLAINTEXT);
        FipsValidator.validateFipsBrokerProtocol(securityProtocols);
    }

    @Test
    public void testValidFipsTlsConfiguration() {
        Map<String, Object> config = new HashMap<>();
        config.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG,
                    Arrays.asList("TLS_DHE_DSS_WITH_3DES_EDE_CBC_SHA", "TLS_DHE_DSS_WITH_AES_128_CBC_SHA"));
        config.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, Arrays.asList("TLSv1.2", "TLSv1.1"));
        FipsValidator.validateFipsTls(config);

        List<String> cipherSuites = Arrays.asList("TLS_DHE_DSS_WITH_3DES_EDE_CBC_SHA", "TLS_DHE_DSS_WITH_AES_128_CBC_SHA");
        FipsValidator.validateFipsTlsCipherSuite(cipherSuites);

        List<String> tlsVersions = Arrays.asList("TLSv1.2", "TLSv1.1");
        FipsValidator.validateFipsTlsVersion(tlsVersions);
    }

    @Test
    public void testValidFipsBrokerProtocolConfigured() throws Exception {
        Map<ListenerName, SecurityProtocol> securityProtocol = new HashMap<>();
        securityProtocol.put(new ListenerName("External"), SecurityProtocol.SASL_SSL);
        FipsValidator.validateFipsBrokerProtocol(securityProtocol);
    }
}
