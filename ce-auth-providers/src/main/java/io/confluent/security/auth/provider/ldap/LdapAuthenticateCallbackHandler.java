// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.ldap;

import io.confluent.security.auth.metadata.PasswordVerifier;
import io.confluent.security.auth.provider.ldap.LdapConfig.SearchMode;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import javax.naming.CommunicationException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapContext;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.plain.PlainAuthenticateCallback;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LDAP authentication callback handler that can be used for simple username/password
 * authentication. Authentication may be performed using one of the following modes:
 * <ul>
 *   <li>Use broker's LDAP credentials to obtain DN for username and bind using (userDn, password).
 *       This will be an anonymous search if broker is not configured with credentials.</li>
 *   <li>Use broker's LDAP credentials to obtain (possibly encrypted) password for username
 *       and compare passwords.</li>
 * </ul>
 *
 * This is currently used for BASIC authentication in REST servers. It is also designed to be used
 * for SASL/PLAIN authentication using LDAP for Kafka clients, but we need to propagate custom configs
 * to callback handlers for enabling that.
 *
 * NOTE: Any AuthenticationException thrown by the callback handler should not leak information.
 */
public class LdapAuthenticateCallbackHandler implements AuthenticateCallbackHandler, Closeable {

  private static final Logger log = LoggerFactory.getLogger(LdapAuthenticateCallbackHandler.class);
  private static final String AUTH_FAILED_MESSAGE = "LDAP authentication failed";

  enum UserSearchMode {
    DN_SEARCH,
    PASSWORD_SEARCH,
  }

  private final SearchControls searchControls;
  private final List<PasswordVerifier> passwordVerifiers;
  private volatile LdapConfig config;
  private volatile UserSearchMode userSearchMode;
  private volatile LdapContextCreator searchContextCreator;
  private volatile LdapContext context;

  public LdapAuthenticateCallbackHandler() {
    this.searchControls = new SearchControls();
    this.passwordVerifiers = new ArrayList<>();
  }

  @Override
  public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
    if (!"PLAIN".equals(saslMechanism))
      throw new ConfigException("SASL mechanism not supported: " + saslMechanism);

    config = new LdapConfig(configs);
    if (config.userPasswordAttribute != null)
      userSearchMode = UserSearchMode.PASSWORD_SEARCH;
    else
      userSearchMode = UserSearchMode.DN_SEARCH;

    if (config.userNameAttribute == null || config.userNameAttribute.isEmpty())
      throw new ConfigException("User name attribute not specified");

    searchContextCreator = new LdapContextCreator(config);
    if (userSearchMode == UserSearchMode.DN_SEARCH) {
      searchControls.setReturningAttributes(new String[]{config.userNameAttribute});
    } else {
      searchControls.setReturningAttributes(
          new String[]{config.userNameAttribute, config.userPasswordAttribute});
      ServiceLoader<PasswordVerifier> verifiers = ServiceLoader.load(PasswordVerifier.class);
      for (PasswordVerifier verifier : verifiers)
        passwordVerifiers.add(verifier);
      passwordVerifiers.add(new DefaultPasswordVerifier());
      passwordVerifiers.forEach(verifier -> verifier.configure(configs));
    }
    searchControls.setSearchScope(config.userSearchScope);
  }

  @Override
  public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    String userName = null;
    for (Callback callback: callbacks) {
      if (callback instanceof NameCallback)
        userName = ((NameCallback) callback).getDefaultName();
      else if (callback instanceof PlainAuthenticateCallback) {
        PlainAuthenticateCallback plainCallback = (PlainAuthenticateCallback) callback;
        if (userName == null || userName.isEmpty())
          throw new AuthenticationException("User name not specified");
        char[] password = plainCallback.password();
        if (password == null || password.length == 0)
          throw new AuthenticationException("Password not specified");
        try {
          boolean authenticated;
          switch (userSearchMode) {
            case DN_SEARCH:
              authenticated = authenticateUsingSimpleBind(ldapUser(userName).dn, plainCallback.password());
              break;
            case PASSWORD_SEARCH:
              authenticated = authenticateUsingPasswordSearch(userName, plainCallback.password());
              break;
            default:
              throw new IllegalStateException("Unknown search mode " + userSearchMode);
          }
          plainCallback.authenticated(authenticated);
        } catch (AuthenticationException e) {
          plainCallback.authenticated(false);
        }
      } else
        throw new UnsupportedCallbackException(callback);
    }
  }

  private boolean authenticateUsingSimpleBind(String userDn, char[] password) {
    Hashtable<String, String> env = new Hashtable<>(config.ldapContextEnvironment);
    env.put(LdapContext.SECURITY_AUTHENTICATION, "simple");
    env.put(LdapContext.SECURITY_PRINCIPAL, userDn);
    env.put(LdapContext.SECURITY_CREDENTIALS, new String(password));
    try {
      InitialDirContext context = new InitialDirContext(env);
      context.close();
      return true;
    } catch (NamingException e) {
      log.trace("LDAP bind failed for user DN {} with specified password", userDn);
      return false;
    }
  }

  private boolean authenticateUsingPasswordSearch(String userName, char[] password) {
    LdapUser ldapUser = ldapUser(userName);
    NameCallback nameCallback = new NameCallback("Name: ");
    nameCallback.setName(userName);
    char[] expectedPassword = new String(ldapUser.password, StandardCharsets.UTF_8).toCharArray();
    for (PasswordVerifier verifier : passwordVerifiers) {
      PasswordVerifier.Result result = verifier.verify(expectedPassword, password);
      switch (result) {
        case MATCH:
          return true;
        case MISMATCH:
          return false;
        default:
          break;
      }
    }
    return false;
  }

  @Override
  public void close() {
    if (context != null) {
      try {
        context.close();
      } catch (NamingException e) {
        log.error("Failed to close LDAP context", e);
      }
    }
    passwordVerifiers.forEach(verifier -> Utils.closeQuietly(verifier, "passwordVerifier"));
  }

  private LdapUser ldapUser(String userName) {
    LdapUser userMetadata = Subject.doAs(searchContextCreator.subject(),
        (PrivilegedAction<LdapUser>) () -> searchForLdapUser(userName));
    if (userMetadata == null)
      throw new AuthenticationException(AUTH_FAILED_MESSAGE);
    return userMetadata;
  }

  private LdapUser searchForLdapUser(String userName) {
    boolean hasContext = context != null;
    try {
      if (!hasContext)
        context = searchContextCreator.createLdapContext();
      try {
        return ldapSearch(context, userName);
      } catch (IOException | CommunicationException e) {
        if (hasContext) {
          context = searchContextCreator.createLdapContext();
          return ldapSearch(context, userName);
        } else
          return null;
      }
    } catch (IOException | NamingException | LdapException e) {
      log.error("Failed to obtain user DN for " + userName, e);
      throw new AuthenticationException(AUTH_FAILED_MESSAGE);
    }
  }

  private LdapUser ldapSearch(LdapContext context, String userName) throws IOException, NamingException {
    Object[] filterArgs = new Object[] {userName};
    log.trace("Searching for user {} with base {} filter {}: ", userName,
          config.userSearchBase, config.userDnSearchFilter);
    NamingEnumeration<SearchResult> enumeration =
        context.search(config.userSearchBase, config.userDnSearchFilter, filterArgs, searchControls);
    if (!enumeration.hasMore()) {
      log.trace("User not found {}", userName);
      return null;
    }
    SearchResult result = enumeration.next();
    if (enumeration.hasMore()) {
      log.error("Found multiple user entries with user name {}", userName);
      return null;
    }
    Attributes attributes = result.getAttributes();
    Attribute nameAttr = attributes.get(config.userNameAttribute);
    if (nameAttr != null) {
      String name = LdapGroupManager.attributeValue(nameAttr.get(), config.userNameAttributePattern, "",
          "user name", SearchMode.USERS);
      if (name == null) {
        return null;
      }
    } else {
      log.trace("User name attribute not found in search result");
      return null;
    }
    String userDn = result.getNameInNamespace();
    byte[] password = userSearchMode == UserSearchMode.DN_SEARCH ? null :
        (byte[]) result.getAttributes().get(config.userPasswordAttribute).get();
    return new LdapUser(userDn, password);
  }

  private static class LdapUser {
    final String dn;
    final byte[] password;

    LdapUser(String dn) {
      this(dn, null);
    }

    LdapUser(String dn, byte[] password) {
      this.dn = dn;
      this.password = password;
    }
  }

  private static class DefaultPasswordVerifier implements PasswordVerifier {

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public Result verify(char[] expectedPassword, char[] actualPassword) {
      log.debug("Verifying passwords using string comparison since no password verifier was found:");
      boolean match = Arrays.equals(expectedPassword, actualPassword);
      return match ? Result.MATCH : Result.MISMATCH;
    }

    @Override
    public void close() throws IOException {
    }
  }
}
