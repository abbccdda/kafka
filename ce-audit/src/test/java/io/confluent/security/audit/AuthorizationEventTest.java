package io.confluent.security.audit;

import static io.confluent.security.audit.provider.ConfluentAuditLogProvider.AUTHORIZATION_MESSAGE_TYPE;
import static org.junit.Assert.assertTrue;

import io.cloudevents.CloudEvent;
import io.confluent.crn.ConfluentServerCrnAuthority;
import io.cloudevents.v03.AttributesImpl;
import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.TenantMetadata;
import io.confluent.telemetry.events.serde.Protobuf;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.ConfluentAuthorizationEvent;
import io.confluent.security.authorizer.utils.AuthorizerUtils;
import io.confluent.telemetry.events.Event;
import java.net.InetAddress;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Test;

public class AuthorizationEventTest {

  @Test
  public void testMultiTenantPrincipal() throws Exception {
    String source = "crn://confluent.cloud/kafka=lkc-12345";
    String subject = "crn://confluent.cloud/kafka=lkc-12345";

    KafkaPrincipal principal = new MultiTenantPrincipal("0",
        new TenantMetadata("lkc-12345", "lkc-12345"));

    ConfluentAuthorizationEvent authorizationEvent = new ConfluentAuthorizationEvent(Scope.ROOT_SCOPE,
        AuthorizerUtils.kafkaRequestContext(
            new RequestContext(new RequestHeader(ApiKeys.ALTER_CONFIGS, (short) 1, "123", 1234),
                "connectionId", InetAddress.getLocalHost(), principal,
                ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY)),
        new Action(Scope.ROOT_SCOPE, ResourcePattern.ALL, Operation.ALL),
        AuthorizeResult.ALLOWED,
        AuthorizePolicy.ALLOW_ON_NO_RULE);
    AuditLogEntry ale = AuditLogUtils.authorizationEvent(authorizationEvent, new ConfluentServerCrnAuthority());
    CloudEvent<AttributesImpl, AuditLogEntry> event = Event.<AuditLogEntry>newBuilder()
        .setType(AUTHORIZATION_MESSAGE_TYPE)
        .setSource(source)
        .setSubject(subject)
        .setDataContentType(Protobuf.APPLICATION_JSON)
        .setData(ale)
        .build();

    assertTrue(Protobuf.<AuditLogEntry>structuredSerializer().toString(event)
        .contains("\"authenticationInfo\":{\"principal\":\"TenantUser:lkc-12345_0\"}"));
  }

}
