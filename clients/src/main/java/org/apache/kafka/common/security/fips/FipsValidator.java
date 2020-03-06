package org.apache.kafka.common.security.fips;

import java.util.Map;
import org.apache.kafka.common.security.auth.SecurityProtocol;

/**
 * FIPS validator interface used by Kafka brokers or clients for proprietary features.
 */
public interface FipsValidator {

    boolean fipsEnabled();
    
    /**
     * Validate FIPS requirements on cipher suites, TLS protocols versions.
     *
     * @param configs the configuration contains cipher suites, TLS protocols.
     */
    void validateFipsTls(Map<String, ?> configs);

    /*
     * Validate broker protocol, make sure broker uses either SSL or SASL_SSL protocol.
     *
     * @param securityProtocolMap the Map contains map relationship between listener name and security protocol.
     */
    void validateFipsBrokerProtocol(Map<String, SecurityProtocol> securityProtocolMap);
}
