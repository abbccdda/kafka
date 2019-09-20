// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.restserver;

import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class KafkaRestServerConfig extends AbstractConfig {

    private static final ConfigDef CONFIG;

    public static final String LISTENERS_CONFIG = "confluent.rest.server.listeners";
    private static final String LISTENERS_DOC
            = "List of comma-separated URIs the REST API will listen on. The only supported protocols are HTTP and HTTPS.\n" +
            " Specify hostname as 0.0.0.0 to bind to all interfaces.\n" +
            " Leave hostname empty to bind to default interface.\n" +
            " Examples of legal listener lists: HTTP://myhost:8083,HTTPS://myhost:8084";

    public static final String INITIALIZERS_CLASSES_CONFIG =
        "confluent.rest.server.servlet.initializor.classes";
    public static final String INITIALIZERS_CLASSES_DOC =
        "Defines one or more initializer for the rest endpoint's ServletContextHandler. "
            + "Each initializer must implement Consumer<ServletContextHandler>. "
            + "It will be called to perform initialization of the handler, in order. "
            + "This is an internal feature and subject to change, "
            + "including changes to the Jetty version";

    static {
        CONFIG = SslUtils.withServerSslSupport(
            new ConfigDef()
                .define(LISTENERS_CONFIG, Type.LIST, Importance.LOW, LISTENERS_DOC)
                .define(INITIALIZERS_CLASSES_CONFIG, Type.LIST, Collections.emptyList(),
                    Importance.LOW, INITIALIZERS_CLASSES_DOC)
        );
    }

    public KafkaRestServerConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }
}
