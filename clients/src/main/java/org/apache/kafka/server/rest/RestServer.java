// (Copyright) [2019 - 2019] Confluent, Inc.

package org.apache.kafka.server.rest;

import java.net.URI;
import org.apache.kafka.common.Configurable;

/**
 * Represent a REST Server instantiated and ran by a Kafka Broker.
 */
public interface RestServer extends Configurable {

    void startup(BrokerProxy broker) throws RestServerException;

    void shutdown();

    URI serverUrl();

}
