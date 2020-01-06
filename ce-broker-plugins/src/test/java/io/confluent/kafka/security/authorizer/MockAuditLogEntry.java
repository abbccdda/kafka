/*
 * Copyright [2019 - 2020] Confluent Inc.
 */
package io.confluent.kafka.security.authorizer;

import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.RequestContext;
import io.confluent.security.authorizer.Scope;

public class MockAuditLogEntry {
  public final Scope sourceScope;
  public final RequestContext requestContext;
  public final Action action;
  public final AuthorizeResult authorizeResult;
  public final AuthorizePolicy authorizePolicy;

  MockAuditLogEntry(Scope sourceScope, RequestContext requestContext, Action action,
      AuthorizeResult authorizeResult, AuthorizePolicy authorizePolicy) {
    this.sourceScope = sourceScope;
    this.requestContext = requestContext;
    this.action = action;
    this.authorizeResult = authorizeResult;
    this.authorizePolicy = authorizePolicy;
  }

}
