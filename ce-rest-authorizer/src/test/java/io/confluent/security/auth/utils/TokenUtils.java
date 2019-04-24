// (Copyright) [2017 - 2019] Confluent, Inc.

package io.confluent.security.auth.utils;

import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.NumericDate;
import org.jose4j.lang.JoseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;

import static org.apache.kafka.test.TestUtils.tempFile;

public class TokenUtils {
  private static final Logger log = LoggerFactory.getLogger(TokenUtils.class);

  public static class JwsContainer {
    private final String jwsToken;
    private final File publicKeyFile;

    JwsContainer(String jwsToken, File publicKeyFile) {
      this.jwsToken = jwsToken;
      this.publicKeyFile = publicKeyFile;
    }

    public File getPublicKeyFile() {
      return publicKeyFile;
    }

    public String getJwsToken() {
      return jwsToken;
    }
  }

  /**
   *  Create the public/private key pair, create a JWS signed with the private key
   *    and write the public key in the expected path
   */
  public static JwsContainer setUpJws(Integer expiration, String issuer, String subject) throws Exception {
    KeyPair keyPair = generateKeyPair();
    String jws = sign(keyPair.getPrivate(), expiration, issuer, subject);
    File publicKeyFile = tempFile();
    writePemFile(publicKeyFile, keyPair.getPublic());
    return new JwsContainer(jws, publicKeyFile);
  }

  public static void writePemFile(File publicKeyFile, PublicKey publicKey) throws IOException  {
    JcaPEMWriter pemWriter = new JcaPEMWriter(new FileWriter(publicKeyFile));
    pemWriter.writeObject(publicKey);
    pemWriter.close();
  }

  public static KeyPair generateKeyPair() throws Exception {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
    keyGen.initialize(2048);
    return keyGen.genKeyPair();
  }

  private static String sign(PrivateKey key, Integer expiration, String issuer, String subject) {
    try {
      JwtClaims claims = new JwtClaims();
      claims.setIssuer(issuer);  // who creates the token and signs it
      if (expiration != null) {
        NumericDate expirationTime = NumericDate.now();
        expirationTime.addSeconds(expiration / 1000);
        claims.setExpirationTime(expirationTime);
      }
      claims.setGeneratedJwtId(); // a unique identifier for the token
      claims.setIssuedAtToNow();  // when the token was issued/created (now)
      claims.setNotBeforeMinutesInThePast(2);
      if (subject != null) {
        claims.setSubject(subject); // the subject/principal is whom the token is about
      }
      claims.setClaim("monitoring", true);
      JsonWebSignature jws = new JsonWebSignature();
      jws.setPayload(claims.toJson());
      jws.setKey(key);
      jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);
      return jws.getCompactSerialization();
    } catch (JoseException e) {
      log.error("Error creating JWS for test");
    }
    return null;
  }
}
