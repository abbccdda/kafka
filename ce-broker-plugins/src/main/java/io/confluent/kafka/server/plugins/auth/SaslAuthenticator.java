// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.SaslException;
import java.util.List;
import java.util.Optional;

interface SaslAuthenticator {

  void initialize(List<AppConfigurationEntry> jaasContextEntries);

  /**
   * Performs PLAIN authentication of username/password and returns principal containing
   * authorization id and tenant
   */
  MultiTenantPrincipal authenticate(String username, String password) throws SaslException;

  /**
   *
   * return cluster Id for the given username/APIKey
   */
  Optional<String> clusterId(String username) throws SaslException;

}
