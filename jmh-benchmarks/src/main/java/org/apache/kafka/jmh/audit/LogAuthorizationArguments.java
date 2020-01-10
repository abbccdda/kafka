package org.apache.kafka.jmh.audit;

import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.RequestContext;
import io.confluent.security.authorizer.Scope;

class LogAuthorizationArguments {

  public final Scope sourceScope;
  public final RequestContext requestContext;
  public final Action action;
  public final AuthorizeResult authorizeResult;
  public final AuthorizePolicy authorizePolicy;

  LogAuthorizationArguments(
      Scope sourceScope,
      RequestContext requestContext,
      Action action,
      AuthorizeResult authorizeResult,
      AuthorizePolicy authorizePolicy) {
    this.sourceScope = sourceScope;
    this.requestContext = requestContext;
    this.action = action;
    this.authorizeResult = authorizeResult;
    this.authorizePolicy = authorizePolicy;
  }
}
