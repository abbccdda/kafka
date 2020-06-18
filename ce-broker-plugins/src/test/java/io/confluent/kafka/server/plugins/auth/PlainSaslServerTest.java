// (Copyright) [2017 - 2017] Confluent, Inc.
package io.confluent.kafka.server.plugins.auth;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.TenantMetadata;
import io.confluent.kafka.server.plugins.auth.stats.AuthenticationStats;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.server.audit.AuditEventStatus;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.SaslException;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PlainSaslServerTest {
  private List<AppConfigurationEntry> jaasEntries;
  private SaslAuthenticator mockSaslAuth;
  private PlainSaslServer saslServer;
  private static AuthenticationStats stats = AuthenticationStats.getInstance();

  @Before
  public void setUp() throws Exception {
    jaasEntries = Collections.emptyList();
    mockSaslAuth = mock(SaslAuthenticator.class);
    when(mockSaslAuth.clusterId(any())).thenReturn(Optional.of("test-cluster"));
    saslServer = new PlainSaslServer(jaasEntries, mockSaslAuth);
    stats.reset();
  }

  @Test
  public void shouldNotAllowImpersonation() throws Exception {
    final String username = "foo";
    final String password = "bar";
    final String authString = "impersonating\u0000" + username + "\u0000" + password;
    try {
      saslServer.evaluateResponse(authString.getBytes());
      fail();
    } catch (SaslAuthenticationException e) {
      assertTrue(e.getMessage().contains("Client requested an authorization id that is different from username"));
      verifyErrorInfo(e, AuditEventStatus.UNAUTHENTICATED, "foo");
    }
  }

  @Test
  public void authSucceedsWithMetrics() throws Exception {
    final String username = "foo";
    final String password = "bar";
    final String authString = "\u0000" + username + "\u0000" + password;
    configureUser(username, password, "tenant1");
    saslServer.evaluateResponse(authString.getBytes());
    assertEquals(username, saslServer.getAuthorizationID());
    assertEquals(1L, stats.getSucceeded());
    assertEquals(0L, stats.getFailed());
    assertEquals(1L, stats.getTotal());
  }

  @Test
  public void authFailsWithMetrics() throws Exception {
    final String username = "foo";
    final String password = "bar";
    final String authString = "\u0000" + username + "\u0000" + password;
    Mockito.doThrow(new SaslException("Top level msg", new Exception("Detailed cause")))
        .when(mockSaslAuth).authenticate(username, password);
    try {
      saslServer.evaluateResponse(authString.getBytes());
      fail();
    } catch (SaslException e) { }
    assertEquals(0L, stats.getSucceeded());
    assertEquals(1L, stats.getFailed());
    assertEquals(1L, stats.getTotal());
  }

  @Test
  public void nullCauseIsOK() throws Exception {
    final String username = "foo";
    final String password = "bar";
    final String authString = "\u0000" + username + "\u0000" + password;
    Mockito.doThrow(new SaslException("Top level msg", null))
        .when(mockSaslAuth).authenticate(username, password);
    try {
      saslServer.evaluateResponse(authString.getBytes());
      fail();
    } catch (SaslException e) { }
    assertEquals(0L, stats.getSucceeded());
    assertEquals(1L, stats.getFailed());
    assertEquals(1L, stats.getTotal());
  }

  @Test
  public void parseFailsWithMetrics() throws Exception {
    try {
      saslServer.evaluateResponse("garbage".getBytes());
      fail();
    } catch (SaslAuthenticationException e) { }
    assertEquals(0L, stats.getSucceeded());
    assertEquals(1L, stats.getFailed());
    assertEquals(1L, stats.getTotal());
  }

  @Test
  public void metricsInJMX() throws Exception {
    final String username = "foo";
    final String password = "bar";
    final String authString = "\u0000" + username + "\u0000" + password;
    final long successes = 7;

    configureUser(username, password, "tenant1");
    for (int i = 0; i < successes; i++) {
      saslServer.evaluateResponse(authString.getBytes());
    }

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    String objectName = "io.confluent.kafka.server.plugins:type=Authentication";
    Set<ObjectInstance> instances = mBeanServer.queryMBeans(new ObjectName(objectName), null);
    assertEquals(1, instances.size());
    ObjectInstance instance = (ObjectInstance) instances.toArray()[0];
    MBeanInfo info = mBeanServer.getMBeanInfo(instance.getObjectName());
    Map<String, Object> attrMap = new HashMap<>();
    for (MBeanAttributeInfo attrInfo : info.getAttributes()) {
      attrMap.put(attrInfo.getName(), mBeanServer.getAttribute(instance.getObjectName(), attrInfo.getName()));
    }
    assertEquals(successes, attrMap.get("Succeeded"));
    assertEquals(0L, attrMap.get("Failed"));
    assertEquals(successes, attrMap.get("Total"));
  }

  @Test
  public void emptyTokens() {
    SaslAuthenticationException e = assertThrows(SaslAuthenticationException.class, () ->
        saslServer.evaluateResponse(saslMessage("", "", "")));
    assertEquals("Authentication failed: username not specified", e.getMessage());
    verifyErrorInfo(e, AuditEventStatus.UNKNOWN_USER_DENIED, "");

    e = assertThrows(SaslAuthenticationException.class, () ->
        saslServer.evaluateResponse(saslMessage("", "", "p")));
    assertEquals("Authentication failed: username not specified", e.getMessage());
    verifyErrorInfo(e, AuditEventStatus.UNKNOWN_USER_DENIED, "");

    e = assertThrows(SaslAuthenticationException.class, () ->
        saslServer.evaluateResponse(saslMessage("", "u", "")));
    assertEquals("Authentication failed: password not specified", e.getMessage());
    verifyErrorInfo(e, AuditEventStatus.UNAUTHENTICATED, "u");


    e = assertThrows(SaslAuthenticationException.class, () ->
        saslServer.evaluateResponse(saslMessage("a", "", "")));
    assertEquals("Authentication failed: username not specified", e.getMessage());
    verifyErrorInfo(e, AuditEventStatus.UNKNOWN_USER_DENIED, "");

    e = assertThrows(SaslAuthenticationException.class, () ->
        saslServer.evaluateResponse(saslMessage("a", "", "p")));
    assertEquals("Authentication failed: username not specified", e.getMessage());
    verifyErrorInfo(e, AuditEventStatus.UNKNOWN_USER_DENIED, "");

    e = assertThrows(SaslAuthenticationException.class, () ->
        saslServer.evaluateResponse(saslMessage("a", "u", "")));
    assertEquals("Authentication failed: password not specified", e.getMessage());
    verifyErrorInfo(e, AuditEventStatus.UNAUTHENTICATED, "u");

    String nul = "\u0000";

    e = assertThrows(SaslAuthenticationException.class, () ->
        saslServer.evaluateResponse(
            String.format("%s%s%s%s%s%s", "a", nul, "u", nul, "p", nul).getBytes(
                StandardCharsets.UTF_8)));
    assertEquals("Invalid SASL/PLAIN response: expected 3 tokens, got 4", e.getMessage());
    verifyErrorInfo(e, AuditEventStatus.UNKNOWN_USER_DENIED, "");

    e = assertThrows(SaslAuthenticationException.class, () ->
        saslServer.evaluateResponse(
            String.format("%s%s%s", "", nul, "u").getBytes(StandardCharsets.UTF_8)));
    assertEquals("Invalid SASL/PLAIN response: expected 3 tokens, got 2", e.getMessage());
    verifyErrorInfo(e, AuditEventStatus.UNKNOWN_USER_DENIED, "");
  }

  private void verifyErrorInfo(final SaslAuthenticationException e,
                               final AuditEventStatus auditEventStatus,
                               final String identifier) {
    assertEquals(auditEventStatus, e.errorInfo().auditEventStatus());
    assertEquals(identifier, e.errorInfo().identifier());
    if (AuditEventStatus.UNAUTHENTICATED == auditEventStatus) {
      assertEquals("test-cluster", e.errorInfo().clusterId());
    }
  }

  private void configureUser(final String username,
                             final String password,
                             final String tenant) throws SaslException {
    Mockito.doAnswer(new Answer<MultiTenantPrincipal>() {
      @Override
      public MultiTenantPrincipal answer(InvocationOnMock invocation) throws Throwable {
        TenantMetadata tenantMetadata = new TenantMetadata(tenant, tenant);
        return new MultiTenantPrincipal(username, tenantMetadata);
      }
     }).when(mockSaslAuth).authenticate(username, password);
  }

  private byte[] saslMessage(String authorizationId, String userName, String password) {
    String nul = "\u0000";
    String message = String.format("%s%s%s%s%s", authorizationId, nul, userName, nul, password);
    return message.getBytes(StandardCharsets.UTF_8);
  }
}
