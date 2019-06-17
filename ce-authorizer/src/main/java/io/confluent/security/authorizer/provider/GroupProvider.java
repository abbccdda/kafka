// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer.provider;

import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Interface used by providers of user to group mapping used for authorization.
 */
public interface GroupProvider extends Provider {

  /**
   * Returns the groups of the provided user principal.
   * @param sessionPrincipal User principal of the Session.
   * @return Set of group principals of the user, which may be empty.
   */
  Set<KafkaPrincipal> groups(KafkaPrincipal sessionPrincipal);

  /**
   * Returns true if minimal configs of this provider are included in the provided configs.
   */
  boolean providerConfigured(Map<String, ?> configs);
}
