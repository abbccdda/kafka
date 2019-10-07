/*
 * Copyright 2019 Confluent Inc.
 */
package org.apache.kafka.clients.admin;

import java.util.Map;
import java.util.Properties;

/**
 * This interface contains admin client methods that:
 * <ol>
 *     <li>are only present in Confluent Server, or</li>
 *     <li>existing Admin methods that return or take extra information only available in Confluent Server.</li>
 * </ol>
 *
 * Any new or update to admin client api that need these features should be done here.
 */
public interface ConfluentAdmin extends Admin {

    /**
     * Create a new ConfluentAdmin with the given configuration.
     *
     * @param props The configuration.
     * @return A concrete class implementing ConfluentAdmin interface, normally KafkaAdminClient.
     */
    static ConfluentAdmin create(Properties props) {
        return KafkaAdminClient.createInternal(new AdminClientConfig(props, true), null);
    }

    /**
     * Create a new ConfluentAdmin with the given configuration.
     *
     * @param conf The configuration.
     * @return A concrete class implementing ConfluentAdmin interface, normally KafkaAdminClient.
     */
    static ConfluentAdmin create(Map<String, Object> conf) {
        return KafkaAdminClient.createInternal(new AdminClientConfig(conf, true), null);
    }
}
