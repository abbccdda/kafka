/*
 * Copyright [2020 - 2020] Confluent Inc.
 */

package io.confluent.security.audit;

import io.cloudevents.CloudEvent;
import io.confluent.crn.ConfluentServerCrnAuthority;
import io.confluent.crn.CrnSyntaxException;
import io.cloudevents.v1.AttributesImpl;
import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.TenantMetadata;
import io.confluent.kafka.security.audit.event.ConfluentAuthenticationEvent;
import io.confluent.security.authorizer.Scope;
import io.confluent.telemetry.events.serde.Protobuf;
import io.confluent.telemetry.events.Event;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SaslAuthenticationContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.audit.AuditEventStatus;
import org.apache.kafka.server.audit.AuthenticationEventImpl;
import org.junit.Test;

import javax.security.sasl.SaslServer;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import static io.confluent.security.audit.provider.ConfluentAuditLogProvider.AUTHENTICATION_MESSAGE_TYPE;
import static io.confluent.telemetry.events.EventLoggerConfig.DEFAULT_CLOUD_EVENT_ENCODING_CONFIG;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class AuthenticationEventTest {

  @Test
  public void testAuthenticationEvent() throws IOException, CrnSyntaxException {
    String source = "crn://confluent.cloud/kafka=lkc-12345";

    KafkaPrincipal principal = new MultiTenantPrincipal("0",
        new TenantMetadata("lkc-12345", "lkc-12345"));

    SaslServer server = mock(SaslServer.class);
    AuthenticationContext authenticationContext = new SaslAuthenticationContext(server,
        SecurityProtocol.SASL_PLAINTEXT, InetAddress.getLocalHost(), SecurityProtocol.SASL_PLAINTEXT.name());

    Map<String, Object> data = new HashMap<>();
    data.put("identifier", "identifier1");
    data.put("mechanism", "mechanism1");
    data.put("message", "errorMessage1");

    AuthenticationEventImpl authenticationEvent = new AuthenticationEventImpl(principal,
        authenticationContext,
        AuditEventStatus.UNAUTHENTICATED);
    authenticationEvent.setData(data);

    ConfluentAuthenticationEvent confluentAuthenticationEvent =
        new ConfluentAuthenticationEvent(authenticationEvent, Scope.ROOT_SCOPE);
    AuditLogEntry auditLogEntry =
        AuditLogUtils.authenticationEvent(confluentAuthenticationEvent, new ConfluentServerCrnAuthority());

    CloudEvent<AttributesImpl, AuditLogEntry> event = Event.<AuditLogEntry>newBuilder()
        .setType(AUTHENTICATION_MESSAGE_TYPE)
        .setSource(source)
        .setSubject(source)
        .setDataContentType(DEFAULT_CLOUD_EVENT_ENCODING_CONFIG)
        .setData(auditLogEntry)
        .build();

    String jsonString = Protobuf.<AuditLogEntry>structuredSerializer().toString(event);

    assertTrue(jsonString.contains("io.confluent.kafka.server/authentication"));
    assertTrue(jsonString.contains("\"metadata\":{\"mechanism\":\"mechanism1\",\"identifier\":\"identifier1\"}"));
    assertTrue(jsonString.contains("\"result\":{\"status\":\"UNAUTHENTICATED\",\"message\":\"errorMessage1\"}"));
  }

}
