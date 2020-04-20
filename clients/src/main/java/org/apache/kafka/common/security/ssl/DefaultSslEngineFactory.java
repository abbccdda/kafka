/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.security.ssl;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SslClientAuth;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.apache.kafka.common.utils.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.UnrecoverableEntryException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public final class DefaultSslEngineFactory implements SslEngineFactory {

    private static final Logger log = LoggerFactory.getLogger(DefaultSslEngineFactory.class);

    private Map<String, ?> configs;
    private String protocol;
    private String provider;
    private String kmfAlgorithm;
    private String tmfAlgorithm;
    private SecurityStore keystore;
    private SecurityStore truststore;
    private String[] cipherSuites;
    private String[] enabledProtocols;
    private SecureRandom secureRandomImplementation;
    private SSLContext sslContext;
    private SslClientAuth sslClientAuth;
    private NettySslEngineBuilder nettySslEngineBuilder;
    private final boolean nettyAllowed;

    public DefaultSslEngineFactory(boolean nettyAllowed) {
        this.nettyAllowed = nettyAllowed;
    }
    @Override
    public SSLEngine createClientSslEngine(String peerHost, int peerPort, String endpointIdentification) {
        return createSslEngine(Mode.CLIENT, peerHost, peerPort, endpointIdentification);
    }

    @Override
    public SSLEngine createServerSslEngine(String peerHost, int peerPort) {
        return createSslEngine(Mode.SERVER, peerHost, peerPort, null);
    }

    @Override
    public boolean shouldBeRebuilt(Map<String, Object> nextConfigs) {
        if (!nextConfigs.equals(configs)) {
            return true;
        }
        if (truststore != null && truststore.modified()) {
            return true;
        }
        if (keystore != null && keystore.modified()) {
            return true;
        }
        return false;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return SslConfigs.RECONFIGURABLE_CONFIGS;
    }

    @Override
    public KeyStore keystore() {
        return this.keystore != null ? this.keystore.get() : null;
    }

    @Override
    public KeyStore truststore() {
        return this.truststore != null ? this.truststore.get() : null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs) {
        this.configs = Collections.unmodifiableMap(configs);
        this.protocol = (String) configs.get(SslConfigs.SSL_PROTOCOL_CONFIG);
        this.provider = (String) configs.get(SslConfigs.SSL_PROVIDER_CONFIG);
        SecurityUtils.addConfiguredSecurityProviders(this.configs);

        List<String> cipherSuitesList = (List<String>) configs.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
        if (cipherSuitesList != null && !cipherSuitesList.isEmpty()) {
            this.cipherSuites = cipherSuitesList.toArray(new String[cipherSuitesList.size()]);
        } else {
            this.cipherSuites = null;
        }

        List<String> enabledProtocolsList = (List<String>) configs.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
        if (enabledProtocolsList != null && !enabledProtocolsList.isEmpty()) {
            this.enabledProtocols = enabledProtocolsList.toArray(new String[enabledProtocolsList.size()]);
        } else {
            this.enabledProtocols = null;
        }

        this.secureRandomImplementation = createSecureRandom((String)
                configs.get(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG));

        this.sslClientAuth = createSslClientAuth((String) configs.get(
                BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG));

        this.kmfAlgorithm = (String) configs.get(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG);
        this.tmfAlgorithm = (String) configs.get(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG);

        this.keystore = createKeystore((String) configs.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG),
                (String) configs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
                (Password) configs.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG),
                (Password) configs.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG));

        this.truststore = createTruststore((String) configs.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG),
                (String) configs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG),
                (Password) configs.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));

        this.sslContext = createSSLContext();
        if (!nettyAllowed) {
            this.nettySslEngineBuilder = null;
        } else if (!sslEngineBuilderClassIsNetty((String)
                configs.get(SslConfigs.SSL_ENGINE_BUILDER_CLASS_CONFIG))) {
            this.nettySslEngineBuilder = null;
        } else if (this.keystore == null) {
            log.warn("Disabling netty because no keystore is configured.");
            this.nettySslEngineBuilder = null;
        } else {
            this.nettySslEngineBuilder = NettySslEngineBuilder.maybeCreate(this);
        }
    }

    private static boolean sslEngineBuilderClassIsNetty(String engineBuilderClass) {
        if (engineBuilderClass == null ||
                engineBuilderClass.equals(SslConfigs.KAFKA_SSL_ENGINE_BUILDER_CLASS)) {
            return false;
        } else if (engineBuilderClass.equals(SslConfigs.NETTY_SSL_ENGINE_BUILDER_CLASS)) {
            return true;
        } else {
            throw new RuntimeException("Invalid configuration value for " +
                    SslConfigs.SSL_ENGINE_BUILDER_CLASS_CONFIG);
        }
    }

    @Override
    public void close() throws IOException {
        this.sslContext = null;
    }

    //For Test only
    public SSLContext sslContext() {
        return this.sslContext;
    }

    private SSLEngine createSslEngine(Mode mode, String peerHost, int peerPort, String endpointIdentification) {
        if (mode == Mode.SERVER && nettySslEngineBuilder != null) {
            return nettySslEngineBuilder.newEngine(peerHost, peerPort);
        }

        SSLEngine sslEngine = sslContext.createSSLEngine(peerHost, peerPort);
        if (cipherSuites != null) sslEngine.setEnabledCipherSuites(cipherSuites);
        if (enabledProtocols != null) sslEngine.setEnabledProtocols(enabledProtocols);

        if (mode == Mode.SERVER) {
            sslEngine.setUseClientMode(false);
            switch (sslClientAuth) {
                case REQUIRED:
                    sslEngine.setNeedClientAuth(true);
                    break;
                case REQUESTED:
                    sslEngine.setWantClientAuth(true);
                    break;
                case NONE:
                    break;
            }
            sslEngine.setUseClientMode(false);
        } else {
            sslEngine.setUseClientMode(true);
            SSLParameters sslParams = sslEngine.getSSLParameters();
            // SSLParameters#setEndpointIdentificationAlgorithm enables endpoint validation
            // only in client mode. Hence, validation is enabled only for clients.
            sslParams.setEndpointIdentificationAlgorithm(endpointIdentification);
            sslEngine.setSSLParameters(sslParams);
        }
        return sslEngine;
    }
    private static SslClientAuth createSslClientAuth(String key) {
        SslClientAuth auth = SslClientAuth.forConfig(key);
        if (auth != null) {
            return auth;
        }
        log.warn("Unrecognized client authentication configuration {}.  Falling " +
                "back to NONE.  Recognized client authentication configurations are {}.",
                key, String.join(", ", SslClientAuth.VALUES.stream().
                        map(a -> a.name()).collect(Collectors.toList())));
        return SslClientAuth.NONE;
    }

    private static SecureRandom createSecureRandom(String key) {
        if (key == null) {
            return null;
        }
        try {
            return SecureRandom.getInstance(key);
        } catch (GeneralSecurityException e) {
            throw new KafkaException(e);
        }
    }

    private SSLContext createSSLContext() {
        try {
            SSLContext sslContext;
            if (provider != null)
                sslContext = SSLContext.getInstance(protocol, provider);
            else
                sslContext = SSLContext.getInstance(protocol);

            KeyManager[] keyManagers = null;
            if (keystore != null || kmfAlgorithm != null) {
                String kmfAlgorithm = this.kmfAlgorithm != null ?
                        this.kmfAlgorithm : KeyManagerFactory.getDefaultAlgorithm();
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(kmfAlgorithm);
                if (keystore != null) {
                    KeyStore ks = keystore.get();
                    Password keyPassword = keystore.keyPassword != null ? keystore.keyPassword : keystore.password;
                    kmf.init(ks, keyPassword.value().toCharArray());
                } else {
                    kmf.init(null, null);
                }
                keyManagers = kmf.getKeyManagers();
            }

            String tmfAlgorithm = this.tmfAlgorithm != null ? this.tmfAlgorithm : TrustManagerFactory.getDefaultAlgorithm();
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
            KeyStore ts = truststore == null ? null : truststore.get();
            tmf.init(ts);

            sslContext.init(keyManagers, tmf.getTrustManagers(), this.secureRandomImplementation);
            log.debug("Created SSL context with keystore {}, truststore {}, provider {}.",
                    keystore, truststore, sslContext.getProvider().getName());
            return sslContext;
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    private static SecurityStore createKeystore(String type, String path, Password password, Password keyPassword) {
        if (path == null && password != null) {
            throw new KafkaException("SSL key store is not specified, but key store password is specified.");
        } else if (path != null && password == null) {
            throw new KafkaException("SSL key store is specified, but key store password is not specified.");
        } else if (path != null && password != null) {
            return new SecurityStore(type, path, password, keyPassword);
        } else
            return null; // path == null, clients may use this path with brokers that don't require client auth
    }

    private static SecurityStore createTruststore(String type, String path, Password password) {
        if (path == null && password != null) {
            throw new KafkaException("SSL trust store is not specified, but trust store password is specified.");
        } else if (path != null) {
            return new SecurityStore(type, path, password, null);
        } else
            return null;
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> configs() {
        return (Map<String, Object>) configs;
    }

    String[] cipherSuites() {
        return cipherSuites;
    }

    String[] enabledProtocols() {
        return enabledProtocols;
    }

    SslClientAuth sslClientAuth() {
        return sslClientAuth;
    }

    SecurityStore securityKeyStore() {
        return keystore;
    }

    SecurityStore securityTrustStore() {
        return truststore;
    }

    class CloseableSslEngine implements Closeable {
        private final SSLEngine engine;

        CloseableSslEngine(SSLEngine engine) {
            this.engine = engine;
        }

        @Override
        public void close() throws IOException {
            if (nettySslEngineBuilder != null) {
                nettySslEngineBuilder.closeEngine(engine);
            }
        }
    }

    public Closeable sslEngineCloser(SSLEngine engine) {
        return new CloseableSslEngine(engine);
    }

    static class PrivateKeyData {
        private final PrivateKey key;
        private final X509Certificate[] certificateChain;

        PrivateKeyData(PrivateKey key, X509Certificate[] certificateChain) {
            this.key = key;
            this.certificateChain = certificateChain;
        }

        PrivateKey key() {
            return key;
        }

        X509Certificate[] certificateChain() {
            return certificateChain;
        }
    }

    // package access for testing
    static class SecurityStore {
        private final String type;
        private final String path;
        private final Password password;
        private final Password keyPassword;
        private final Long fileLastModifiedMs;
        private final KeyStore keyStore;

        SecurityStore(String type, String path, Password password, Password keyPassword) {
            Objects.requireNonNull(type, "type must not be null");
            this.type = type;
            this.path = path;
            this.password = password;
            this.keyPassword = keyPassword;
            fileLastModifiedMs = lastModifiedMs(path);
            this.keyStore = load();
        }

        KeyStore get() {
            return keyStore;
        }

        /**
         * Loads this keystore
         * @return the keystore
         * @throws KafkaException if the file could not be read or if the keystore could not be loaded
         *   using the specified configs (e.g. if the password or keystore type is invalid)
         */
        private KeyStore load() {
            try (InputStream in = Files.newInputStream(Paths.get(path))) {
                KeyStore ks = KeyStore.getInstance(type);
                // If a password is not set access to the truststore is still available, but integrity checking is disabled.
                char[] passwordChars = password != null ? password.value().toCharArray() : null;
                ks.load(in, passwordChars);
                return ks;
            } catch (GeneralSecurityException | IOException e) {
                throw new KafkaException("Failed to load SSL keystore " + path + " of type " + type, e);
            }
        }

        private Long lastModifiedMs(String path) {
            try {
                return Files.getLastModifiedTime(Paths.get(path)).toMillis();
            } catch (IOException e) {
                log.error("Modification time of key store could not be obtained: " + path, e);
                return null;
            }
        }

        boolean modified() {
            Long modifiedMs = lastModifiedMs(path);
            return modifiedMs != null && !Objects.equals(modifiedMs, this.fileLastModifiedMs);
        }

        PrivateKeyData loadPrivateKeyData() {
            KeyStore store = load();
            KeyStore.PasswordProtection keyProtection = (keyPassword == null) ? null :
                    new KeyStore.PasswordProtection(keyPassword.value().toCharArray());
            try {
                Enumeration<String> aliases = store.aliases();
                while (aliases.hasMoreElements()) {
                    String alias = aliases.nextElement();
                    if (store.isKeyEntry(alias)) {
                        try {
                            KeyStore.Entry entry = store.getEntry(alias, keyProtection);
                            if (entry instanceof KeyStore.PrivateKeyEntry) {
                                KeyStore.PrivateKeyEntry privateKeyEntry = (KeyStore.PrivateKeyEntry) entry;
                                PrivateKey privateKey = privateKeyEntry.getPrivateKey();
                                Certificate[] certs = privateKeyEntry.getCertificateChain();
                                if (!(certs instanceof X509Certificate[])) {
                                    // TODO: can we convert this?
                                    throw new RuntimeException("Expected a certificate chain of type " +
                                            "X509Certificate for alias " + alias);
                                }
                                return new PrivateKeyData(privateKey, (X509Certificate[]) certs);
                            }
                        } catch (NoSuchAlgorithmException e) {
                            log.info("can't find the algorithm for recovering the {} entry.", alias);
                        } catch (UnrecoverableEntryException e) {
                            log.trace("ignoring alias {}, since the password doesn't match.", alias);
                        }
                    }
                }
            } catch (KeyStoreException e) {
                throw new KafkaException(e);
            }
            throw new RuntimeException("No private key found protected with the given password in " + path);
        }

        X509Certificate[] loadAllCertificates() {
            KeyStore store = load();
            List<X509Certificate> all = new ArrayList<>();
            try {
                Enumeration<String> aliases = store.aliases();
                while (aliases.hasMoreElements()) {
                    String alias = aliases.nextElement();
                    if (store.isCertificateEntry(alias)) {
                        Certificate cert = store.getCertificate(alias);
                        if (!(cert instanceof X509Certificate)) {
                            // TODO: can we convert this?
                            throw new RuntimeException("Expected a certificate of type " +
                                    "X509Certificate for alias " + alias);
                        }
                        all.add((X509Certificate) cert);
                    }
                }
            } catch (KeyStoreException e) {
                throw new KafkaException(e);
            }
            return all.toArray(new X509Certificate[0]);
        }

        @Override
        public String toString() {
            return "SecurityStore(" +
                    "path=" + path +
                    ", modificationTime=" + (fileLastModifiedMs == null ? null : new Date(fileLastModifiedMs)) + ")";
        }
    }

    public static DefaultSslEngineFactory castOrThrow(SslEngineFactory factory) {
        if (factory instanceof DefaultSslEngineFactory) {
            return (DefaultSslEngineFactory) factory;
        } else {
            throw new RuntimeException("Expected ssl engine factory to be DefaultSslEngineFactory, but saw " + factory.getClass());
        }
    }
}
