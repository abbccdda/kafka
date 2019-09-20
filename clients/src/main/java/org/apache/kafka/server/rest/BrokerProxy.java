// (Copyright) [2019 - 2019] Confluent, Inc.

package org.apache.kafka.server.rest;

/**
 * Proxy to expose information and/or services of the Broker to the REST Server.
 */
public interface BrokerProxy {

    String clusterId();

}
