/*
 * Copyright [2020 - 2020] Confluent Inc.
 */

package io.confluent.security.audit;

import io.cloudevents.CloudEvent;
import io.cloudevents.v1.AttributesImpl;
import io.confluent.crn.ConfluentServerCrnAuthority;
import io.confluent.crn.CrnSyntaxException;
import io.confluent.kafka.security.audit.event.ConfluentAuthenticationEvent;
import io.confluent.kafka.server.plugins.auth.PlainSaslServer;
import io.confluent.security.authorizer.Scope;
import io.confluent.telemetry.events.Event;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.PlaintextAuthenticationContext;
import org.apache.kafka.common.security.auth.SaslAuthenticationContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.auth.SslAuthenticationContext;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslServer;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.server.audit.AuditEventStatus;
import org.apache.kafka.server.audit.AuthenticationErrorInfo;
import org.apache.kafka.server.audit.DefaultAuthenticationEvent;
import org.junit.Test;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.sasl.SaslServer;
import java.io.IOException;
import java.net.InetAddress;
import java.security.Principal;

import static io.confluent.security.audit.AuditLogUtils.AUTHENTICATION_FAILED_EVENT_USER;
import static io.confluent.security.audit.provider.ConfluentAuditLogProvider.AUTHENTICATION_MESSAGE_TYPE;
import static io.confluent.telemetry.events.EventLoggerConfig.DEFAULT_CLOUD_EVENT_ENCODING_CONFIG;
import static org.apache.kafka.server.audit.AuthenticationErrorInfo.UNKNOWN_USER_ERROR;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthenticationEventTest {

    private final static Scope TEST_SCOPE = new Scope.Builder().withKafkaCluster("pkc-12345").build();

    @Test
    public void testSaslPlainAuthenticationEvent() throws CrnSyntaxException {
        PlainSaslServer server = mock(PlainSaslServer.class);
        when(server.getMechanismName()).thenReturn(PlainSaslServer.PLAIN_MECHANISM);
        when(server.userIdentifier()).thenReturn("APIKEY123");

        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");

        AuthenticationContext authenticationContext = new SaslAuthenticationContext(server, SecurityProtocol.SASL_SSL,
            InetAddress.getLoopbackAddress(), SecurityProtocol.SASL_SSL.name());
        DefaultAuthenticationEvent authenticationEvent = new
            DefaultAuthenticationEvent(principal, authenticationContext, AuditEventStatus.SUCCESS);

        CloudEvent<AttributesImpl, AuditLogEntry> cloudEvent = getCloudEvent(authenticationEvent);
        verifyCloudEvent(cloudEvent, "User:foo",
            "SASL_SSL/PLAIN",
            "APIKEY123",
            "SUCCESS",
            "");
    }

    @Test
    public void testSaslPlainAuthenticationFailureEvent_1() throws CrnSyntaxException {
        PlainSaslServer server = mock(PlainSaslServer.class);
        when(server.getMechanismName()).thenReturn(PlainSaslServer.PLAIN_MECHANISM);

        AuthenticationContext authenticationContext = new SaslAuthenticationContext(server, SecurityProtocol.SASL_SSL,
            InetAddress.getLoopbackAddress(), SecurityProtocol.SASL_SSL.name());
        //test with ssh handshake failure
        SslAuthenticationException authenticationException = new SslAuthenticationException("Ssl handshake failed");
        DefaultAuthenticationEvent authenticationEvent = new
            DefaultAuthenticationEvent(null, authenticationContext, AuditEventStatus.UNKNOWN_USER_DENIED, authenticationException);

        CloudEvent<AttributesImpl, AuditLogEntry> cloudEvent = getCloudEvent(authenticationEvent);
        verifyCloudEvent(cloudEvent, AUTHENTICATION_FAILED_EVENT_USER,
            "SASL_SSL/PLAIN",
            "",
            "UNKNOWN_USER_DENIED",
            "Ssl handshake failed");
    }

    @Test
    public void testSaslPlainAuthenticationFailureEvent_2() throws CrnSyntaxException {
        PlainSaslServer server = mock(PlainSaslServer.class);
        when(server.getMechanismName()).thenReturn(PlainSaslServer.PLAIN_MECHANISM);

        AuthenticationContext authenticationContext = new SaslAuthenticationContext(server, SecurityProtocol.SASL_SSL,
            InetAddress.getLoopbackAddress(), SecurityProtocol.SASL_SSL.name());
        //test username not specified error
        SaslAuthenticationException authenticationException = new SaslAuthenticationException("username not specified", UNKNOWN_USER_ERROR);
        DefaultAuthenticationEvent authenticationEvent = new
            DefaultAuthenticationEvent(null, authenticationContext, AuditEventStatus.UNKNOWN_USER_DENIED, authenticationException);

        CloudEvent<AttributesImpl, AuditLogEntry> cloudEvent = getCloudEvent(authenticationEvent);
        verifyCloudEvent(cloudEvent, AUTHENTICATION_FAILED_EVENT_USER,
            "SASL_SSL/PLAIN",
            "",
            "UNKNOWN_USER_DENIED",
            "username not specified");
    }

    @Test
    public void testSaslPlainAuthenticationFailureEvent_3() throws CrnSyntaxException {
        PlainSaslServer server = mock(PlainSaslServer.class);
        when(server.getMechanismName()).thenReturn(PlainSaslServer.PLAIN_MECHANISM);

        AuthenticationContext authenticationContext = new SaslAuthenticationContext(server, SecurityProtocol.SASL_SSL,
            InetAddress.getLoopbackAddress(), SecurityProtocol.SASL_SSL.name());
        //test with password not specified error
        AuthenticationErrorInfo errorInfo = new
            AuthenticationErrorInfo(AuditEventStatus.UNAUTHENTICATED, "", "APIKEY123", "");
        SaslAuthenticationException authenticationException = new SaslAuthenticationException("password not specified", errorInfo);
        DefaultAuthenticationEvent authenticationEvent = new
            DefaultAuthenticationEvent(null, authenticationContext, AuditEventStatus.UNAUTHENTICATED, authenticationException);

        CloudEvent<AttributesImpl, AuditLogEntry> cloudEvent = getCloudEvent(authenticationEvent);
        verifyCloudEvent(cloudEvent, AUTHENTICATION_FAILED_EVENT_USER,
            "SASL_SSL/PLAIN",
            "APIKEY123",
            "UNAUTHENTICATED",
            "password not specified");
    }

    @Test
    public void testSaslPlainAuthenticationFailureEvent_4() throws CrnSyntaxException {
        PlainSaslServer server = mock(PlainSaslServer.class);
        when(server.getMechanismName()).thenReturn(PlainSaslServer.PLAIN_MECHANISM);

        AuthenticationContext authenticationContext = new SaslAuthenticationContext(server, SecurityProtocol.SASL_SSL,
            InetAddress.getLoopbackAddress(), SecurityProtocol.SASL_SSL.name());
        // test with bad password error
        AuthenticationErrorInfo errorInfo =
            new AuthenticationErrorInfo(AuditEventStatus.UNAUTHENTICATED, "Bad password for user", "APIKEY123", "");
        SaslAuthenticationException authenticationException = new SaslAuthenticationException("Invalid username or password", errorInfo);
        DefaultAuthenticationEvent authenticationEvent = new
            DefaultAuthenticationEvent(null, authenticationContext, AuditEventStatus.UNAUTHENTICATED, authenticationException);

        CloudEvent<AttributesImpl, AuditLogEntry> cloudEvent = getCloudEvent(authenticationEvent);
        verifyCloudEvent(cloudEvent, AUTHENTICATION_FAILED_EVENT_USER,
            "SASL_SSL/PLAIN",
            "APIKEY123",
            "UNAUTHENTICATED",
            "Bad password for user");
    }

    @Test
    public void testSaslOAuthAuthenticationEvent() throws CrnSyntaxException {
        SaslServer server = mock(OAuthBearerSaslServer.class);
        when(server.getMechanismName()).thenReturn(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM);
        when(server.getAuthorizationID()).thenReturn("foo");

        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");

        AuthenticationContext authenticationContext = new SaslAuthenticationContext(server, SecurityProtocol.SASL_SSL,
            InetAddress.getLoopbackAddress(), SecurityProtocol.SASL_SSL.name());
        DefaultAuthenticationEvent authenticationEvent = new
            DefaultAuthenticationEvent(principal, authenticationContext, AuditEventStatus.SUCCESS);

        CloudEvent<AttributesImpl, AuditLogEntry> cloudEvent = getCloudEvent(authenticationEvent);
        verifyCloudEvent(cloudEvent, "User:foo",
            "SASL_SSL/OAUTHBEARER",
            "foo",
            "SUCCESS",
            "");
    }

    @Test
    public void testSaslOAuthAuthenticationFailureEvent_1() throws CrnSyntaxException {
        SaslServer server = mock(OAuthBearerSaslServer.class);
        when(server.getMechanismName()).thenReturn(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM);

        AuthenticationContext authenticationContext = new SaslAuthenticationContext(server, SecurityProtocol.SASL_SSL,
            InetAddress.getLoopbackAddress(), SecurityProtocol.SASL_SSL.name());
        // test with invalid response from client
        SaslAuthenticationException authenticationException = new SaslAuthenticationException("Received %x01 response from client after it received our error");
        DefaultAuthenticationEvent authenticationEvent = new
            DefaultAuthenticationEvent(null, authenticationContext, AuditEventStatus.UNKNOWN_USER_DENIED, authenticationException);

        CloudEvent<AttributesImpl, AuditLogEntry> cloudEvent = getCloudEvent(authenticationEvent);
        verifyCloudEvent(cloudEvent, AUTHENTICATION_FAILED_EVENT_USER,
            "SASL_SSL/OAUTHBEARER",
            "",
            "UNKNOWN_USER_DENIED",
            "Received %x01 response from client after it received our error");
    }

    @Test
    public void testSaslOAuthAuthenticationFailureEvent_2() throws CrnSyntaxException {
        SaslServer server = mock(OAuthBearerSaslServer.class);
        when(server.getMechanismName()).thenReturn(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM);

        AuthenticationContext authenticationContext = new SaslAuthenticationContext(server, SecurityProtocol.SASL_SSL,
            InetAddress.getLoopbackAddress(), SecurityProtocol.SASL_SSL.name());
        // test with cluster extension is missing
        AuthenticationErrorInfo errorInfo =
            new AuthenticationErrorInfo(AuditEventStatus.UNAUTHENTICATED, "The logical cluster extension is missing or is empty", "Customer", "");
        SaslAuthenticationException authenticationException = new SaslAuthenticationException("Authentication failed", errorInfo);
        DefaultAuthenticationEvent authenticationEvent = new
            DefaultAuthenticationEvent(null, authenticationContext, AuditEventStatus.UNAUTHENTICATED, authenticationException);

        CloudEvent<AttributesImpl, AuditLogEntry> cloudEvent = getCloudEvent(authenticationEvent);
        verifyCloudEvent(cloudEvent, AUTHENTICATION_FAILED_EVENT_USER,
            "SASL_SSL/OAUTHBEARER",
            "Customer",
            "UNAUTHENTICATED",
            "The logical cluster extension is missing or is empty");
    }

    @Test
    public void testSaslOAuthAuthenticationFailureEvent_3() throws CrnSyntaxException {
        SaslServer server = mock(OAuthBearerSaslServer.class);
        when(server.getMechanismName()).thenReturn(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM);

        AuthenticationContext authenticationContext = new SaslAuthenticationContext(server, SecurityProtocol.SASL_SSL,
            InetAddress.getLoopbackAddress(), SecurityProtocol.SASL_SSL.name());
        // cluster is not part of the allowed clusters in this token
        AuthenticationErrorInfo errorInfo =
            new AuthenticationErrorInfo(AuditEventStatus.UNAUTHENTICATED, "cluster is not part of the allowed clusters in this token", "Customer", "");
        SaslAuthenticationException authenticationException = new SaslAuthenticationException("Authentication failed", errorInfo);
        DefaultAuthenticationEvent authenticationEvent = new
            DefaultAuthenticationEvent(null, authenticationContext, AuditEventStatus.UNAUTHENTICATED, authenticationException);

        CloudEvent<AttributesImpl, AuditLogEntry> cloudEvent = getCloudEvent(authenticationEvent);
        verifyCloudEvent(cloudEvent, AUTHENTICATION_FAILED_EVENT_USER,
            "SASL_SSL/OAUTHBEARER",
            "Customer",
            "UNAUTHENTICATED",
            "cluster is not part of the allowed clusters in this token");
    }

    @Test
    public void testPlainTextAuthenticationEvent() throws CrnSyntaxException {
        AuthenticationContext authenticationContext =
            new PlaintextAuthenticationContext(InetAddress.getLoopbackAddress(), SecurityProtocol.PLAINTEXT.name());
        DefaultAuthenticationEvent authenticationEvent = new
            DefaultAuthenticationEvent(KafkaPrincipal.ANONYMOUS, authenticationContext, AuditEventStatus.SUCCESS);

        CloudEvent<AttributesImpl, AuditLogEntry> cloudEvent = getCloudEvent(authenticationEvent);
        verifyCloudEvent(cloudEvent, "User:ANONYMOUS",
            "PLAINTEXT",
            "",
            "SUCCESS",
            "");
    }

    @Test
    public void testSslAuthenticationEvent() throws IOException, CrnSyntaxException {
        SSLSession session = mock(SSLSession.class);
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");
        when(session.getPeerPrincipal()).thenReturn(new DummyPrincipal("CN:foo"));

        AuthenticationContext authenticationContext =
            new SslAuthenticationContext(session, InetAddress.getLoopbackAddress(), SecurityProtocol.SSL.name());
        DefaultAuthenticationEvent authenticationEvent = new
            DefaultAuthenticationEvent(principal, authenticationContext, AuditEventStatus.SUCCESS);

        CloudEvent<AttributesImpl, AuditLogEntry> cloudEvent = getCloudEvent(authenticationEvent);
        verifyCloudEvent(cloudEvent, "User:foo",
            "SSL",
            "CN:foo",
            "SUCCESS",
            "");
    }

    @Test
    public void testSslAuthenticationFailureEvent() throws IOException, CrnSyntaxException {
        SSLSession session = mock(SSLSession.class);
        when(session.getPeerPrincipal()).thenThrow(SSLPeerUnverifiedException.class);

        AuthenticationContext authenticationContext =
            new SslAuthenticationContext(session, InetAddress.getLoopbackAddress(), SecurityProtocol.SSL.name());
        SslAuthenticationException authenticationException = new SslAuthenticationException("Ssl handshake failed");
        DefaultAuthenticationEvent authenticationEvent = new
            DefaultAuthenticationEvent(null, authenticationContext, AuditEventStatus.UNKNOWN_USER_DENIED, authenticationException);

        CloudEvent<AttributesImpl, AuditLogEntry> cloudEvent = getCloudEvent(authenticationEvent);
        verifyCloudEvent(cloudEvent, AUTHENTICATION_FAILED_EVENT_USER,
            "SSL",
            "",
            "UNKNOWN_USER_DENIED",
            "Ssl handshake failed");
    }

    @Test
    public void testSaslGssapiAuthenticationEvent() throws CrnSyntaxException {
        SaslServer server = mock(SaslServer.class);
        when(server.getMechanismName()).thenReturn(SaslConfigs.GSSAPI_MECHANISM);
        when(server.getAuthorizationID()).thenReturn("foo/host@REALM.COM");

        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");

        AuthenticationContext authenticationContext = new SaslAuthenticationContext(server, SecurityProtocol.SASL_SSL,
            InetAddress.getLoopbackAddress(), SecurityProtocol.SASL_SSL.name());
        DefaultAuthenticationEvent authenticationEvent = new
            DefaultAuthenticationEvent(principal, authenticationContext, AuditEventStatus.SUCCESS);

        CloudEvent<AttributesImpl, AuditLogEntry> cloudEvent = getCloudEvent(authenticationEvent);
        verifyCloudEvent(cloudEvent, "User:foo",
            "SASL_SSL/GSSAPI",
            "foo/host@REALM.COM",
            "SUCCESS",
            "");
    }

    @Test
    public void testSaslScramAuthenticationEvent() throws CrnSyntaxException {
        SaslServer server = mock(SaslServer.class);
        when(server.getMechanismName()).thenReturn(ScramMechanism.SCRAM_SHA_256.mechanismName());
        when(server.getAuthorizationID()).thenReturn("foo");

        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");

        AuthenticationContext authenticationContext = new SaslAuthenticationContext(server, SecurityProtocol.SASL_SSL,
            InetAddress.getLoopbackAddress(), SecurityProtocol.SASL_SSL.name());
        DefaultAuthenticationEvent authenticationEvent = new
            DefaultAuthenticationEvent(principal, authenticationContext, AuditEventStatus.SUCCESS);

        CloudEvent<AttributesImpl, AuditLogEntry> cloudEvent = getCloudEvent(authenticationEvent);
        verifyCloudEvent(cloudEvent, "User:foo",
            "SASL_SSL/SCRAM-SHA-256",
            "foo",
            "SUCCESS",
            "");
    }

    private void verifyCloudEvent(final CloudEvent<AttributesImpl, AuditLogEntry> cloudEvent,
                                  final String principalString,
                                  final String authMechanism,
                                  final String identifier,
                                  final String status,
                                  final String message) {
        AttributesImpl attributes = cloudEvent.getAttributes();
        assertEquals("crn://confluent.cloud/kafka=pkc-12345", attributes.getSource().toString());
        assertEquals("io.confluent.kafka.server/authentication", attributes.getType());
        assertEquals("crn://confluent.cloud/kafka=pkc-12345", attributes.getSubject().get());

        AuditLogEntry auditLogEntry = cloudEvent.getData().get();
        assertEquals("crn://confluent.cloud/kafka=pkc-12345", auditLogEntry.getServiceName());
        assertEquals("kafka.Authentication", auditLogEntry.getMethodName());
        assertEquals("crn://confluent.cloud/kafka=pkc-12345", auditLogEntry.getResourceName());

        AuthenticationInfo authenticationInfo = auditLogEntry.getAuthenticationInfo();
        assertEquals(principalString, authenticationInfo.getPrincipal());
        assertEquals(authMechanism, authenticationInfo.getMetadata().getMechanism());
        assertEquals(identifier, authenticationInfo.getMetadata().getIdentifier());

        Result result = auditLogEntry.getResult();
        assertEquals(status, result.getStatus());
        assertEquals(message, result.getMessage());
    }

    private CloudEvent<AttributesImpl, AuditLogEntry> getCloudEvent(final DefaultAuthenticationEvent authenticationEvent) throws CrnSyntaxException {

        ConfluentAuthenticationEvent confluentAuthenticationEvent =
            new ConfluentAuthenticationEvent(authenticationEvent, TEST_SCOPE);
        AuditLogEntry auditLogEntry =
            AuditLogUtils.authenticationEvent(confluentAuthenticationEvent, new ConfluentServerCrnAuthority("confluent.cloud", 10));

        return Event.<AuditLogEntry>newBuilder()
            .setType(AUTHENTICATION_MESSAGE_TYPE)
            .setSource(auditLogEntry.getServiceName())
            .setSubject(auditLogEntry.getResourceName())
            .setDataContentType(DEFAULT_CLOUD_EVENT_ENCODING_CONFIG)
            .setData(auditLogEntry)
            .build();
    }

    private static class DummyPrincipal implements Principal {
        private final String name;

        private DummyPrincipal(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
    }
}
