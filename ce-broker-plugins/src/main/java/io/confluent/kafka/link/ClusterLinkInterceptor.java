// (Copyright) [2020 - 2020] Confluent, Inc.

package io.confluent.kafka.link;

import io.confluent.kafka.multitenant.schema.TransformableType;
import java.nio.ByteBuffer;
import java.util.Map;
import kafka.server.link.ClusterLinkManager$;
import org.apache.kafka.clients.ClientInterceptor;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestInternals;

public class ClusterLinkInterceptor implements ClientInterceptor, Configurable {

  private LinkContext linkContext;

  @Override
  public void configure(Map<String, ?> configs) {
    String destTenantPrefix = (String) configs.get(ClusterLinkManager$.MODULE$.DestinationTenantPrefixProp());
    if (destTenantPrefix == null || destTenantPrefix.isEmpty())
      throw new ConfigException("ClusterLinkInterceptor is multi-tenant and should be configured with a valid tenant prefix");
    this.linkContext = new LinkContext(destTenantPrefix);
  }

  @Override
  public Send toSend(RequestHeader requestHeader, AbstractRequest requestBody, String destination) {
    if (!ClusterLinkApis.isApiAllowed(requestHeader.apiKey())) {
      throw new IllegalStateException("Request " + requestHeader.apiKey() + " is not allowed on cluster links");
    }
    TransformableType<LinkContext> schema = ClusterLinkApis.requestSchema(requestHeader.apiKey(), requestHeader.apiVersion());
    Struct requestHeaderStruct = requestHeader.toStruct();
    Struct requestBodyStruct = RequestInternals.toStruct(requestBody);

    ByteBuffer buffer = ByteBuffer.allocate(requestHeaderStruct.sizeOf()
        + schema.sizeOf(requestBodyStruct, linkContext));
    requestHeaderStruct.writeTo(buffer);
    schema.write(buffer, requestBodyStruct, linkContext);
    buffer.flip();
    return new NetworkSend(destination, buffer);
  }

  @Override
  public Struct parseResponse(ByteBuffer responseBuffer, RequestHeader requestHeader) {
    if (!ClusterLinkApis.isApiAllowed(requestHeader.apiKey())) {
      throw new IllegalStateException("Request " + requestHeader.apiKey() + " is not allowed on cluster links");
    }

    TransformableType<LinkContext> schema = ClusterLinkApis.responseSchema(requestHeader.apiKey(), requestHeader.apiVersion());
    return (Struct) schema.read(responseBuffer, linkContext);
  }
}