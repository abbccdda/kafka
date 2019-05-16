// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.ldap;

import static org.junit.Assert.fail;

import io.confluent.security.minikdc.MiniKdcWithLdapService;
import io.confluent.security.test.utils.LdapTestUtils;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.naming.Context;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.security.plain.PlainAuthenticateCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LdapAuthenticateCallbackHandlerTest {

  private MiniKdcWithLdapService miniKdcWithLdapService;
  private LdapAuthenticateCallbackHandler ldapCallbackHandler;

  @Before
  public void setUp() throws Exception {
    miniKdcWithLdapService = LdapTestUtils.createMiniKdcWithLdapService(null, null);
    miniKdcWithLdapService.createPrincipal("admin", "admin-secret");
    miniKdcWithLdapService.createPrincipal("alice", "alice-secret");
  }

  @After
  public void tearDown() throws IOException {
    if (ldapCallbackHandler != null) {
      ldapCallbackHandler.close();
    }
    if (miniKdcWithLdapService != null) {
      miniKdcWithLdapService.shutdown();
    }
  }

  @Test
  public void testSuccessfulAuthentication() throws Exception {
    createLdapCallbackHandler(Collections.emptyMap());
    authenticate("alice", "alice-secret");
  }

  @Test
  public void testSuccessfulAuthenticationWithAnonymousSearch() throws Exception {
    Map<String, Object> overrideProps = new HashMap<>();
    overrideProps.put(LdapConfig.CONFIG_PREFIX + Context.SECURITY_AUTHENTICATION, null);
    overrideProps.put(LdapConfig.CONFIG_PREFIX + Context.SECURITY_PRINCIPAL, null);
    overrideProps.put(LdapConfig.CONFIG_PREFIX + Context.SECURITY_CREDENTIALS, null);
    createLdapCallbackHandler(overrideProps);
    authenticate("alice", "alice-secret");
  }

  @Test
  public void testSuccessfulAuthenticationWithPasswordSearch() throws Exception {
    Map<String, Object> overrideProps = new HashMap<>();
    overrideProps.put(LdapConfig.USER_PASSWORD_ATTRIBUTE_PROP, "userPassword");
    createLdapCallbackHandler(overrideProps);
    authenticate("alice", "alice-secret");
  }

  @Test(expected = AuthenticationException.class)
  public void testInvalidPassword() throws Exception {
    createLdapCallbackHandler(Collections.emptyMap());
    authenticate("alice", "invalid-secret");
  }

  @Test(expected = AuthenticationException.class)
  public void testMissingUserName() throws Exception {
    createLdapCallbackHandler(Collections.emptyMap());
    authenticate(null, "alice-secret");
  }

  @Test(expected = AuthenticationException.class)
  public void testMissingPassword() throws Exception {
    createLdapCallbackHandler(Collections.emptyMap());
    authenticate("alice", null);
  }

  @Test(expected = AuthenticationException.class)
  public void testLdapNotAvailable() throws Exception {
    createLdapCallbackHandler(Collections.emptyMap());
    miniKdcWithLdapService.stopLdap();
    authenticate("alice", "alice-secret");
  }

  @Test
  public void testLdapConnectionFailure() throws Exception {
    createLdapCallbackHandler(Collections.emptyMap());
    verifyLdapConnectionFailure();
  }

  @Test
  public void testLdapConnectionFailureWithPasswordSearch() throws Exception {
    createLdapCallbackHandler(Collections.singletonMap(LdapConfig.USER_PASSWORD_ATTRIBUTE_PROP, "userPassword"));
    verifyLdapConnectionFailure();
  }

  private void verifyLdapConnectionFailure() throws Exception {
    authenticate("alice", "alice-secret");
    miniKdcWithLdapService.stopLdap();
    verifyAuthenticationFailure("alice", "alice-secret");
    verifyAuthenticationFailure("alice", "bad-secret");
    LdapTestUtils.restartLdapServer(miniKdcWithLdapService);
    authenticate("alice", "alice-secret");
    verifyAuthenticationFailure("alice", "bad-secret");
  }

  private void createLdapCallbackHandler(Map<String, Object> overrideProps) {
    Map<String, Object> props = new HashMap<>();
    props.putAll(LdapTestUtils.ldapAuthorizerConfigs(miniKdcWithLdapService, 0));
    props.put(LdapConfig.CONFIG_PREFIX + "com.sun.jndi.ldap.read.timeout", "5000");
    props.put(LdapConfig.CONFIG_PREFIX + Context.SECURITY_AUTHENTICATION, "simple");
    props.put(LdapConfig.CONFIG_PREFIX + Context.SECURITY_PRINCIPAL, "uid=admin,ou=users,dc=example,dc=com");
    props.put(LdapConfig.CONFIG_PREFIX + Context.SECURITY_CREDENTIALS, "admin-secret");
    props.putAll(overrideProps);
    ldapCallbackHandler = new LdapAuthenticateCallbackHandler();
    ldapCallbackHandler.configure(props, "PLAIN", Collections.emptyList());
  }

  private void authenticate(String userName, String password) throws Exception {
    NameCallback nameCallback = userName == null ? new NameCallback("Name:") : new NameCallback("Name:", userName);
    char[] passwordChars = password == null ? null : password.toCharArray();
    PlainAuthenticateCallback plainCallback = new PlainAuthenticateCallback(passwordChars);
    ldapCallbackHandler.handle(new Callback[] {nameCallback, plainCallback});
    if (!plainCallback.authenticated())
      throw new AuthenticationException("LDAP authentication failed");
  }

  private void verifyAuthenticationFailure(String userName, String password) throws Exception {
    try {
      authenticate(userName, password);
      fail("Did not fail authentication");
    } catch (AuthenticationException e) {
      // Expected exception
    }
  }
}

