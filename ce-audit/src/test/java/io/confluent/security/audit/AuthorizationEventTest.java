package io.confluent.security.audit;

import static io.confluent.security.audit.provider.ConfluentAuditLogProvider.AUTHORIZATION_MESSAGE_TYPE;
import static org.junit.Assert.assertTrue;

import io.cloudevents.CloudEvent;
import io.confluent.events.CloudEventUtils;
import io.confluent.events.EventLoggerConfig;
import io.confluent.events.ProtobufEvent;
import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.TenantMetadata;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.utils.AuthorizerUtils;
import java.io.IOException;
import java.net.InetAddress;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Test;

public class AuthorizationEventTest {

  @Test
  public void testMultiTenantPrincipal() throws IOException {
    String source = "crn://confluent.cloud/kafka=lkc-12345";
    String subject = "crn://confluent.cloud/kafka=lkc-12345";

    KafkaPrincipal principal = new MultiTenantPrincipal("0",
        new TenantMetadata("lkc-12345", "lkc-12345"));

    AuditLogEntry ale = AuditLogUtils.authorizationEvent(
        source,
        subject,
        AuthorizerUtils.kafkaRequestContext(
            new RequestContext(new RequestHeader(ApiKeys.ALTER_CONFIGS, (short) 1, "123", 1234),
                "connectionId", InetAddress.getLocalHost(), principal,
                ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                SecurityProtocol.PLAINTEXT)),
        new Action(Scope.ROOT_SCOPE, ResourcePattern.ALL, Operation.ALL),
        AuthorizeResult.ALLOWED,
        AuthorizePolicy.ALLOW_ON_NO_RULE
    );
    CloudEvent event = ProtobufEvent.newBuilder()
        .setType(AUTHORIZATION_MESSAGE_TYPE)
        .setSource(source)
        .setSubject(subject)
        .setEncoding(EventLoggerConfig.DEFAULT_CLOUD_EVENT_ENCODING_CONFIG)
        .setData(ale)
        .build();

    assertTrue(CloudEventUtils.toJsonString(event)
        .contains("\"authenticationInfo\":{\"principal\":\"TenantUser:lkc-12345_0\"}"));
  }

}
