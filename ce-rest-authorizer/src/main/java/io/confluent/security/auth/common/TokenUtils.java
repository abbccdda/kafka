// (Copyright) [2017 - 2019] Confluent, Inc.

package io.confluent.security.auth.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.security.PublicKey;

import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;

public class TokenUtils {
  private static final Charset US_ASCII = Charset.forName("US-ASCII");

  /* Reads a PEM encoded stream into PublicKey */
  public static PublicKey loadPublicKey(InputStream inputStream) throws IOException {
    try (InputStreamReader reader = new InputStreamReader(inputStream, US_ASCII)) {
      PEMParser pemParser = new PEMParser(new BufferedReader(reader));
      SubjectPublicKeyInfo keyInfo = SubjectPublicKeyInfo.getInstance(pemParser.readObject());
      return new JcaPEMKeyConverter().getPublicKey(keyInfo);
    }
  }

  private static final String JWT_ISSUER = "Confluent";

  /**
   * returns a common {@link JwtConsumer} for building/validating JWS tokens for
   *  OAuth 2 authentication across CCloud
   * The JWS token must have:
   *  - an issuer `iss` claim representing the principal that issued the token
   *  - a subject `sub` claim representing the principal who will use the token
   *  - a JWT ID `jti` claim which serves as a unique identifier of the token
   *  - a issued at `iat` claim which is the time the token was created
   *  - an expiration time `exp` claim showing the time after which this token
   *    should be considered expired
   */
  public static JwtConsumer createJwtConsumer(PublicKey publicKey) {
    return new JwtConsumerBuilder()
            .setExpectedIssuer(JWT_ISSUER) // whom the JWT needs to have been issued by
            .setVerificationKey(publicKey) // verify the signature with the public key
            .setRequireExpirationTime()
            .setRequireIssuedAt()
            .setRequireJwtId()
            .setRequireSubject()
            .build();
  }
}
