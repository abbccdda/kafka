package org.apache.kafka.jmh.audit;

import io.confluent.security.authorizer.RequestContext;
import java.net.InetAddress;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;

class MockRequestContext implements RequestContext {

  public final RequestHeader header;
  public final String connectionId;
  public final InetAddress clientAddress;
  public final KafkaPrincipal principal;
  public final ListenerName listenerName;
  public final SecurityProtocol securityProtocol;
  public final String requestSource;

  public MockRequestContext(RequestHeader header, String connectionId, InetAddress clientAddress,
      KafkaPrincipal principal, ListenerName listenerName, SecurityProtocol securityProtocol,
      String requestSource) {
    this.header = header;
    this.connectionId = connectionId;
    this.clientAddress = clientAddress;
    this.principal = principal;
    this.listenerName = listenerName;
    this.securityProtocol = securityProtocol;
    this.requestSource = requestSource;
  }

  @Override
  public String listenerName() {
    return listenerName.value();
  }

  @Override
  public SecurityProtocol securityProtocol() {
    return securityProtocol;
  }

  @Override
  public KafkaPrincipal principal() {
    return principal;
  }

  @Override
  public InetAddress clientAddress() {
    return clientAddress;
  }

  @Override
  public int requestType() {
    return header.apiKey().id;
  }

  @Override
  public int requestVersion() {
    return header.apiVersion();
  }

  @Override
  public String clientId() {
    return header.clientId();
  }

  @Override
  public int correlationId() {
    return header.correlationId();
  }

  @Override
  public String requestSource() {
    return requestSource;
  }
}
