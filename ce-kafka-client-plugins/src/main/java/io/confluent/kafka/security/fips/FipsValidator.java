/*
 * Copyright 2019 Confluent Inc.
 */

package io.confluent.kafka.security.fips;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.config.SslConfigs;
import io.confluent.kafka.security.fips.exceptions.InvalidFipsTlsCipherSuiteException;
import io.confluent.kafka.security.fips.exceptions.InvalidFipsBrokerProtocolException;
import io.confluent.kafka.security.fips.exceptions.InvalidFipsTlsVersionException;

/**
 * <p>This class centralizes FIPS validation for cipher suites, SSL/TLS versions and kafka broker protocols.
 * One of its primary uses is to validate FIPS requirements.
 *
 */
public class FipsValidator {
    private final static Logger log = LoggerFactory.getLogger(FipsValidator.class);

    private static final Set<String> ALLOWED_CIPHER_SUITES;
    private static final Set<String> ALLOWED_TLS_PROTOCOLS;
    private static final Set<String> ALLOWED_BROKER_PROTOCOLS;

    private static final String ERROR_CIPHER_SUITES = "FIPS 140-2 Configuration Error, invalid cipher suites: ";
    private static final String ERROR_TLS_VERSIONS = "FIPS 140-2 Configuration Error, invalid TLS versions: ";
    private static final String ERROR_BROKER_PROTOCOLS = "FIPS 140-2 Configuration Error, invalid broker protocols: ";

    static {
        /**
         * The following cipher suites are FIPS compliant from Bouncy Castel provider.
         * {@link https://downloads.bouncycastle.org/fips-java/BC-FJA-(D)TLSUserGuide-1.0.9.pdf}
         * Mose of them are also default enabled from SunJSSE provider.
         **/
        ALLOWED_CIPHER_SUITES = Stream.of(
                "TLS_DHE_DSS_WITH_3DES_EDE_CBC_SHA",          // not in SunJSSE provoder.
                "TLS_DHE_DSS_WITH_AES_128_CBC_SHA",
                "TLS_DHE_DSS_WITH_AES_128_CBC_SHA256",
                "TLS_DHE_DSS_WITH_AES_256_CBC_SHA",
                "TLS_DHE_DSS_WITH_AES_256_CBC_SHA256",
                "TLS_ECDHE_ECDSA_WITH_3DES_EDE_CBC_SHA",
                "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",       //not in SunJSSE provoder.
                "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
                "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
                "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
                "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA",
                "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
                "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
                "TLS_RSA_WITH_3DES_EDE_CBC_SHA",              // not in SunJSSE provoder.
                "TLS_RSA_WITH_AES_128_CBC_SHA",
                "TLS_RSA_WITH_AES_128_CBC_SHA256",
                "TLS_RSA_WITH_AES_128_CCM",                   // not in SunJSSE provoder.
                "TLS_RSA_WITH_AES_256_CBC_SHA",
                "TLS_RSA_WITH_AES_256_CBC_SHA256",
                "TLS_RSA_WITH_AES_256_CCM"                    // not in SunJSSE provoder.
        ).collect(Collectors.toCollection(HashSet::new));
        ALLOWED_TLS_PROTOCOLS = Stream.of("TLSv1.2", "TLSv1.1").collect(Collectors.toCollection(HashSet::new));
        ALLOWED_BROKER_PROTOCOLS = Stream.of("SASL_SSL", "SSL").collect(Collectors.toCollection(HashSet::new));
    }

    /**
     * Validate FIPS requirements on cipher suites, TLS protocols versions.
     *
     * @param configs the configuration contains cipher suites, TLS protocols.
     *
     * @throws InvalidFipsCipherSuiteException if cipher suites not FIPS compliant.
     * @throws InvalidFipsTlsVersionException if TLS protocols not FIPS compliant.
     */
    public static void validateFipsTls(Map<String, ?> configs) {
        validateFipsTlsCipherSuite(configs);
        validateFipsTlsVersion(configs);
    }

    /*
     * Validate broker protocol, make sure broker uses either SSL or SASL_SSL protocol.
     *
     * @param securityProtocolMap the Map contains map relationship between listener and security protocol.
     *
     * @throws InvalidFipsBrokerProtocolException if broker protocols not FIPS compliant.
     */
    public static void validateFipsBrokerProtocol(Map<ListenerName, SecurityProtocol> securityProtocolMap) {
        List<String> violatedBrokerProtocols = new ArrayList<>();
        violatedBrokerProtocols = securityProtocolMap.entrySet().stream()
                .filter(entry -> !ALLOWED_BROKER_PROTOCOLS.contains(entry.getValue().name))
                .map(entry -> entry.getKey().value() + ":" + entry.getValue().name)
                .collect(Collectors.toList());

        if (!violatedBrokerProtocols.isEmpty())  {
            String exmsg = String.format("%s%s", ERROR_BROKER_PROTOCOLS, String.join(",", violatedBrokerProtocols));
            log.error(exmsg);
            throw new InvalidFipsBrokerProtocolException(exmsg);
        }
    }

    /*
     * Validate cipher suites are FIPS compliant or not.
     *
     * @param configs the configuration contains cipher suites.
     *
     * @throws InvalidFipsCipherSuiteException if cipher suites not FIPS compliant.
     */
    public static void validateFipsTlsCipherSuite(Map<String, ?> configs) {
        @SuppressWarnings("unchecked")
        List<String> cipherSuitesList = (List<String>) configs.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
        validateFipsTlsCipherSuite(cipherSuitesList);
    }

    /*
     * Validate TLS versions are FIPS compliant or not.
     *
     * @param configs the configuration contains TLS versions.
     *
     * @throws InvalidFipsTlsVersionException if TLS protocol not FIPS compliant.
     */
    public static void validateFipsTlsVersion(Map<String, ?> configs) {
        @SuppressWarnings("unchecked")
        List<String> tlsVersionList = (List<String>) configs.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
        validateFipsTlsVersion(tlsVersionList);
    }

    /*
     * Validate cipher suites are FIPS compliant or not.
     *
     * @param cipherSuites a collection of cipher suites
     *
     * @throws InvalidFipsCipherSuiteException if cipher suites not FIPS compliant.
     */
    public static void validateFipsTlsCipherSuite(Collection<String> cipherSuites) {
        List<String> violatedCiphers = new ArrayList<>();
        if (cipherSuites != null && !cipherSuites.isEmpty()) {
            violatedCiphers = cipherSuites.stream()
                    .filter(cipher -> !ALLOWED_CIPHER_SUITES.contains(cipher)).collect(Collectors.toList());
        }

        if (!violatedCiphers.isEmpty()) {
            String exmsg = String.format("%s%s", ERROR_CIPHER_SUITES, String.join(",", violatedCiphers));
            log.error(exmsg);
            throw new InvalidFipsTlsCipherSuiteException(exmsg);
        }
    }

    /*
     * Validate TLS versions are FIPS compliant or not.
     *
     * @param tlsVersions a collection of TLS protocol version.
     *
     * @throws InvalidFipsTlsVersionException if TLS protocol not FIPS compliant.
     */
    public static void validateFipsTlsVersion(Collection<String> tlsVersions) {
        List<String> violatedTlsVersions = new ArrayList<>();
        if (tlsVersions != null && !tlsVersions.isEmpty()) {
            violatedTlsVersions = tlsVersions.stream()
                    .filter(protocol -> !ALLOWED_TLS_PROTOCOLS.contains(protocol)).collect(Collectors.toList());
        }

        if (!violatedTlsVersions.isEmpty()) {
            String exmsg = String.format("%s%s", ERROR_TLS_VERSIONS, String.join(",", violatedTlsVersions));
            log.error(exmsg);
            throw new InvalidFipsTlsVersionException(exmsg);
        }
    }

    /**
     * Don't let anyone instantiate this.
     */
    private FipsValidator() {
    }
}