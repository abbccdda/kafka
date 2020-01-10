// (Copyright) [2020 - 2020] Confluent, Inc.

package io.confluent.security.authorizer.provider;

import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.RequestContext;
import io.confluent.security.authorizer.Scope;

public class AuthorizationLogData {
  public final Scope sourceScope;
  public final RequestContext requestContext;
  public final Action action;
  public final AuthorizeResult authorizeResult;
  public final AuthorizePolicy authorizePolicy;

  /**
   * @param sourceScope     Scope of the cluster making the authorization decision
   * @param requestContext  Request context that contains details of the request that was being
   *                        authorized. This includes the user principal.
   * @param action          The action that was being authorized including resource and operation.
   * @param authorizeResult Result of the authorization indicating if access was granted.
   * @param authorizePolicy Details of the authorization policy that granted or denied access.
   *                        This includes any ACL/Role binding that produced the result.
   */
  public AuthorizationLogData(Scope sourceScope,
      RequestContext requestContext, Action action,
      AuthorizeResult authorizeResult,
      AuthorizePolicy authorizePolicy) {
    this.sourceScope = sourceScope;
    this.requestContext = requestContext;
    this.action = action;
    this.authorizeResult = authorizeResult;
    this.authorizePolicy = authorizePolicy;
  }

  @Override
  public String toString() {
    return "AuthorizationLogData(" +
        "sourceScope='" + sourceScope + '\'' +
        ", requestSource='" + requestContext.requestSource() + '\'' +
        ", principal='" + requestContext.principal() + '\'' +
        ", resource='" + action.resourcePattern() + '\'' +
        ", scope='" + action.scope() + '\'' +
        ", operation='" + action.operation() + '\'' +
        ", authorizeResult='" + authorizeResult + '\'' +
        ", authorizePolicy='" + authorizePolicy.policyType() + '\'' +
        ')';
  }
}
