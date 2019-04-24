// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AuthenticationResponse {

  private final String authenticationToken;
  private final String tokenType;
  private final long expiresIn;

  @JsonCreator
  public AuthenticationResponse(@JsonProperty("auth_token") String authenticationToken,
                                @JsonProperty("token_type") String tokenType,
                                @JsonProperty("expires_in") long expiresIn) {

    this.authenticationToken = authenticationToken;
    this.tokenType = tokenType;
    this.expiresIn = expiresIn;
  }

  @JsonProperty("auth_token")
  public String authenticationToken() {
    return this.authenticationToken;
  }

  @JsonProperty("token_type")
  public String tokenType() {
    return this.tokenType;
  }

  @JsonProperty("expires_in")
  public long lifetime() {
    return this.expiresIn;
  }

}
