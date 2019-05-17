/*
* Copyright 2018 Confluent Inc.
*/

package io.confluent.kafka.security.config.provider;

import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* DecryptionEngine Engine performs Key loading and decryption operations.
*/

public class DecryptionEngine {
  private final Logger log = LoggerFactory.getLogger(getClass());
  private byte[] dataEncryptionKey;
  private String masterKey;
  private int dataKeyLength;

  public static final Pattern CIPHER_PATTERN = Pattern.compile("ENC\\[(.*?),data:(.*?),iv:(.*?),type:(.*?)(.*?)\\]");

  public DecryptionEngine(String masterKeyEnvVar, String dataKeyCipher, String keyLength) throws Exception {
    try {
      masterKey = loadMasterKey(masterKeyEnvVar);
      dataEncryptionKey = loadDataKey(dataKeyCipher);
      dataKeyLength = Integer.parseInt(keyLength);
    } catch (Exception e) {
      log.error("Failed to initialize the encryption engine");
      throw new ConfigException("Failed to initialize the encryption engine", e);
    }
  }

  private byte[] base64Decode(String data) throws Exception {
    return Base64.getDecoder().decode(data.getBytes());
  }

  private Key convertByteArrKeyToAESKey(byte[] key) {
    return new SecretKeySpec(key, "AES");
  }

  protected String loadMasterKey(String environmentVariable) {
    // Load the master key from environment variable.
    String masterKey = System.getenv(environmentVariable);
    // If environment variable not set throw an exception.
    if (masterKey == null) {
      log.error("Failed to load master key from environment variable.");
      throw new ConfigException("Failed to load master key from environment variable.");
    }
    return masterKey;
  }

  private byte[] loadDataKey(String dataKeyCipher) throws Exception {
    try {
      String dataKeyEnc = decryptWithMasterKey(dataKeyCipher);
      return base64Decode(dataKeyEnc);
    } catch (Exception e) {
      log.error("Failed to unwrap the data key", e);
      throw new ConfigException("Failed to unwrap the data key", e);
    }
  }

  private AlgorithmParameterSpec getAlgorithmSpec(String algorithm, byte[] iv, int keyLength) throws Exception {
    AlgorithmParameterSpec algoSpec;
    switch (algorithm) {
      case "AES/CBC/PKCS5Padding":
      case "AES/CFB/PKCS5Padding":
      case "AES/OFB/PKCS5Padding":
        algoSpec = new IvParameterSpec(iv);
        break;
      case "AES/GCM/PKCS5Padding":
        algoSpec = new GCMParameterSpec(keyLength, iv);
        break;
      default:
        throw new NoSuchAlgorithmException("Algorithm not supported !!!");
    }
    return algoSpec;
  }

  public String decryptWithMasterKey(String encryptedData) throws Exception {
    byte[] masterKeyByte = base64Decode(masterKey);
    return decrypt(encryptedData, masterKeyByte);
  }

  public String decryptWithDEK(String encryptedData) throws Exception {
    return decrypt(encryptedData, dataEncryptionKey);
  }

  private String decrypt(String encryptedData, byte[] key) throws Exception {
    String data, iv, algo;
    Matcher matcher = CIPHER_PATTERN.matcher(encryptedData);
    if (matcher.matches()) {
      algo = matcher.group(1);
      data = matcher.group(2);
      iv = matcher.group(3);
    } else {
      throw new ConfigException("Invalid cipher.");
    }

    Cipher cipher = Cipher.getInstance(algo);
    byte[] byteIv = base64Decode(iv);
    Key aesKey = convertByteArrKeyToAESKey(key);
    AlgorithmParameterSpec algoSpec = getAlgorithmSpec(algo, byteIv, dataKeyLength);
    cipher.init(Cipher.DECRYPT_MODE, aesKey, algoSpec);

    byte[] decodeData = base64Decode(data);
    return new String(cipher.doFinal(decodeData));
  }
}
