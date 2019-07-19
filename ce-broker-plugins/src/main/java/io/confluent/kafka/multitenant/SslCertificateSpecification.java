package io.confluent.kafka.multitenant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Represents specifications for ssl certificates - pem and pkcs
 */
public class SslCertificateSpecification {
    private final String sslKeystoreType;
    private final String sslKeystorePassword;
    private final String pkcsCertFilename;
    private final Integer secretId;
    private final String sslPemFullchainFilename;
    private final String sslPemPrivkeyFilename;


    @JsonCreator
    SslCertificateSpecification(
        @JsonProperty("ssl_certificate_encoding") String sslKeystoreType,
        @JsonProperty("ssl_keystore_password") String sslKeystorePassword,
        @JsonProperty("ssl_keystore_filename") String pkcsCertFilename,
        @JsonProperty("secret_id") Integer secretId,
        @JsonProperty("ssl_pem_fullchain_filename") String sslPemFullchainFilename,
        @JsonProperty("ssl_pem_privkey_filename") String sslPemPrivkeyFilename
    ) {
        this.sslKeystoreType = sslKeystoreType;
        this.sslKeystorePassword = sslKeystorePassword;
        this.pkcsCertFilename = pkcsCertFilename;
        this.secretId = secretId;
        this.sslPemFullchainFilename = sslPemFullchainFilename;
        this.sslPemPrivkeyFilename = sslPemPrivkeyFilename;
    }

    @JsonProperty
    String sslKeystoreType() {
        return this.sslKeystoreType;
    }

    @JsonProperty
    String getSslKeystorePassword() {
        return this.sslKeystorePassword;
    }

    @JsonProperty
    String pkcsCertFilename() {
        return this.pkcsCertFilename;
    }

    @JsonProperty
    Integer secretId() {
        return this.secretId;
    }

    @JsonProperty
    String sslPemFullchainFilename() {
        return this.sslPemFullchainFilename;
    }

    @JsonProperty
    String sslPemPrivkeyFilename() {
        return this.sslPemPrivkeyFilename;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SslCertificateSpecification that = (SslCertificateSpecification) o;
        return Objects.equals(sslKeystoreType, that.sslKeystoreType) &&
               Objects.equals(sslKeystorePassword, that.sslKeystorePassword) &&
               Objects.equals(pkcsCertFilename, that.pkcsCertFilename) &&
               Objects.equals(secretId, that.secretId) &&
               Objects.equals(sslPemFullchainFilename, that.sslPemFullchainFilename) &&
               Objects.equals(sslPemPrivkeyFilename, that.sslPemPrivkeyFilename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sslKeystoreType, sslKeystorePassword, pkcsCertFilename, secretId, sslPemFullchainFilename,
                sslPemPrivkeyFilename);
    }
}