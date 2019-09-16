// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import io.confluent.security.authorizer.utils.AuthorizerUtils;
import java.io.Closeable;
import java.util.List;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Cross-component authorizer API.
 */
public interface Authorizer extends Configurable, Closeable {

  /**
   * Performs authorization for each of the provided `actions` and returns the result of each
   * authorization.
   *
   * @param sessionPrincipal Authenticated principal of the client being authorized.
   *                         For embedded authorizer used in brokers, this must be the session principal
   *                         created by {@link org.apache.kafka.common.security.auth.KafkaPrincipalBuilder}.
   *                         For remote authorization, e.g using Metadata Service, this is a KafkaPrincipal
   *                         instance that represents the principal of the remote client being authorized.
   * @param host             The host IP of the client performing the actions being authorized.
   *                         This may be null if host-based access control is not enabled.
   * @param actions          List of actions being authorized including the resource and operation
   *                         for each action.
   *
   * @return List of authorization results, one for each of the provided actions, in the order
   *         they appear in `actions`.
   */
  default List<AuthorizeResult> authorize(KafkaPrincipal sessionPrincipal, String host, List<Action> actions) {
    return authorize(AuthorizerUtils.newRequestContext(RequestContext.MDS, sessionPrincipal, host), actions);
  }

  /**
   * Performs authorization for each of the provided `actions` and returns the result of each
   * authorization.
   *
   * @param requestContext Request context including principal and additional context for auditing
   * @param actions        List of actions being authorized including the resource and operation
   *                       for each action.
   *
   * @return List of authorization results, one for each of the provided actions, in the order
   *         they appear in `actions`.
   */
  List<AuthorizeResult> authorize(RequestContext requestContext, List<Action> actions);
}
