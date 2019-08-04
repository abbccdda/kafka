// (Copyright) [2019 - 2019] Confluent, Inc.

package org.apache.kafka.server.interceptor;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.net.InetAddress;

public interface BrokerInterceptor extends Configurable {

    // Used by ce-broker-plugins
    void onAuthenticatedConnection(String connectionId, InetAddress clientAddress,
                                   KafkaPrincipal principal, Metrics metrics);

    // Used by ce-broker-plugins
    void onAuthenticatedDisconnection(String connectionId, InetAddress clientAddress,
                                      KafkaPrincipal principal, Metrics metrics);

    RequestContext newContext(RequestHeader header,
                              String connectionId,
                              InetAddress clientAddress,
                              KafkaPrincipal principal,
                              ListenerName listenerName,
                              SecurityProtocol securityProtocol,
                              Metrics metrics);

}
