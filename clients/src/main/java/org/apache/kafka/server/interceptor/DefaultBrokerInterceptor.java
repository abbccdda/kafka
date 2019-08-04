// (Copyright) [2019 - 2019] Confluent, Inc.

package org.apache.kafka.server.interceptor;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.net.InetAddress;
import java.util.Map;

public class DefaultBrokerInterceptor implements BrokerInterceptor {

    public void onAuthenticatedConnection(String connectionId, InetAddress clientAddress,
                                          KafkaPrincipal principal, Metrics metrics) {
    }

    public void onAuthenticatedDisconnection(String connectionId, InetAddress clientAddress,
                                             KafkaPrincipal principal, Metrics metrics) {
    }

    @Override
    public RequestContext newContext(RequestHeader header,
                                     String connectionId,
                                     InetAddress clientAddress,
                                     KafkaPrincipal principal,
                                     ListenerName listenerName,
                                     SecurityProtocol securityProtocol,
                                     Metrics metrics) {
        return new RequestContext(header, connectionId, clientAddress, principal, listenerName, securityProtocol);
    }

    @Override
    public void configure(Map<String, ?> configs) {}

}
