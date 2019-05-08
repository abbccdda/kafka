// (Copyright) [2017 - 2019] Confluent, Inc.

package io.confluent.security.auth.common;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;

import java.util.Collections;
import java.util.Set;

public class JwtBearerToken implements OAuthBearerToken {

  private final String jwtId;
  private final String value;
  private final String principalName;
  private final Set<String> scope;
  private final long lifetimeMs;
  private final Long startTimeMs;
  private final JwtClaims jwtClaims;

  public JwtBearerToken(String value, Set<String> scope, long lifetimeMs,
                        String principalName, Long startTimeMs, String jwtId) {
    this.value = value;
    this.principalName = principalName;
    this.scope = scope;
    this.lifetimeMs = lifetimeMs;
    this.startTimeMs = startTimeMs;
    this.jwtId = jwtId;
    jwtClaims = new JwtClaims();
  }

  /* Constructs JwtBearerToken without validating claims */
  public JwtBearerToken(String value) {
    JwtConsumer jwtConsumer = new JwtConsumerBuilder()
            .setSkipAllValidators()
            .setSkipSignatureVerification()
            .build();

    try {
      jwtClaims = jwtConsumer.processToClaims(value);
      this.value = value;
      this.principalName = jwtClaims.getSubject();
      this.scope = Collections.emptySet();
      this.lifetimeMs = jwtClaims.getExpirationTime().getValueInMillis();
      this.startTimeMs = jwtClaims.getIssuedAt().getValueInMillis();
      this.jwtId = jwtClaims.getJwtId();
    } catch (MalformedClaimException | InvalidJwtException e) {
      throw new ConfigException("Failed to construct login token", e);
    }
  }

  @Override
  public String value() {
    return value;
  }

  @Override
  public Set<String> scope() {
    return scope;
  }

  @Override
  public long lifetimeMs() {
    return lifetimeMs;
  }

  @Override
  public String principalName() {
    return principalName;
  }

  @Override
  public Long startTimeMs() {
    return startTimeMs;
  }

  public String jwtId() {
    return jwtId;
  }

  public JwtClaims jwtClaims() {
    return jwtClaims;
  }
}