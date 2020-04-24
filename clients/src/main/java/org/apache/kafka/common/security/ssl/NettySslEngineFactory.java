package org.apache.kafka.common.security.ssl;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.ReferenceCountedOpenSslEngine;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4JLoggerFactory;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import java.io.Closeable;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableEntryException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class NettySslEngineFactory extends DefaultSslEngineFactory implements SslEngineFactory {
    private static final Logger log = LoggerFactory.getLogger(NettySslEngineFactory.class);

    static {
        InternalLoggerFactory.setDefaultFactory(Log4JLoggerFactory.INSTANCE);
    }

    private SslContext nettySslContext;
    private boolean configured = false;

    /**
     * For server-side only, use the Netty SSLEngine
     * @param peerHost               The peer host to use. This is a hint and not used for validation.
     * @param peerPort               The peer port to use. This is a hint and not used for validation.
     * @return An SSLEngine provided by Netty's {@link SslContextBuilder}
     */
    @Override
    public SSLEngine createServerSslEngine(String peerHost, int peerPort) {
        if (!configured) {
            throw new RuntimeException("Cannot create SSLEngine since this factory has not yet been configured");
        } else if (nettySslContext == null) {
            throw new RuntimeException("Cannot create SSLEngine since this factory could not be configured");
        } else {
            return nettySslContext.newEngine(ByteBufAllocator.DEFAULT, peerHost, peerPort);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        if (!OpenSsl.isAvailable()) {
            this.nettySslContext = null;
        } else {
            this.nettySslContext = createNettySslServerContext(this);
        }
        this.configured = true;
    }

    static boolean isConfigurable(Map<String, ?> configs, Mode mode) {
        if (mode != Mode.SERVER) {
            log.warn("Cannot configure Netty because the SSL mode is " + mode + " instead of " + Mode.SERVER);
            return false;
        } else if (!configs.containsKey(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG) ||
            !configs.containsKey(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG) ||
            !configs.containsKey(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)) {
            log.warn("Cannot configure Netty because keystore is not configured.");
            return false;
        } else if (!OpenSsl.isAvailable()) {
            log.warn("Cannot configure Netty because no OpenSSL is available.");
            return false;
        } else {
            return true;
        }
    }

    class CloseableSslEngine implements Closeable {
        private final SSLEngine engine;

        CloseableSslEngine(SSLEngine engine) {
            this.engine = engine;
        }

        @Override
        public void close() throws IOException {
            if (engine instanceof ReferenceCountedOpenSslEngine) {
                ((ReferenceCountedOpenSslEngine) engine).release();
            }
        }
    }

    public Closeable sslEngineCloser(SSLEngine engine) {
        return new CloseableSslEngine(engine);
    }

    public static Optional<NettySslEngineFactory> maybeCast(SslEngineFactory factory) {
        if (factory instanceof NettySslEngineFactory) {
            return Optional.of((NettySslEngineFactory) factory);
        } else {
            return Optional.empty();
        }
    }

    PrivateKeyData loadPrivateKeyData() {
        SecurityStore keyStore = securityKeyStore();
        KeyStore store = keyStore.load();
        KeyStore.PasswordProtection keyProtection = (keyStore.keyPassword() == null) ? null :
                new KeyStore.PasswordProtection(keyStore.keyPassword().value().toCharArray());
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
        throw new RuntimeException("No private key found protected with the given password in " + keyStore.path());
    }

    X509Certificate[] loadAllCertificates() {
        KeyStore store = securityTrustStore().load();
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

    private static SslContext createNettySslServerContext(NettySslEngineFactory factory) {
        try {
            if (factory.keystore() == null) {
                throw new KafkaException("Whe using netty in server mode, a keystore must be configured.");
            }
            // The keystore should contain the private key as well as the
            // certificate chain for the server.
            DefaultSslEngineFactory.PrivateKeyData keystorePrivateKeyData = factory.loadPrivateKeyData();
            X509Certificate[] truststoreCerts = factory.truststore() == null ?
                    null : factory.loadAllCertificates();

            SslContextBuilder builder = SslContextBuilder.
                    forServer(keystorePrivateKeyData.key(), keystorePrivateKeyData.certificateChain()).
                    applicationProtocolConfig(ApplicationProtocolConfig.DISABLED).
                    sslProvider(SslProvider.OPENSSL_REFCNT).
                    trustManager(truststoreCerts);
            if (factory.enabledProtocols() != null) {
                builder.protocols(factory.enabledProtocols());
            }
            if (factory.cipherSuites() != null) {
                builder.ciphers(Arrays.asList(factory.cipherSuites()));
            }
            switch (factory.sslClientAuth()) {
                case NONE:
                    builder.clientAuth(ClientAuth.NONE);
                    break;
                case REQUIRED:
                    builder.clientAuth(ClientAuth.REQUIRE);
                    break;
                case REQUESTED:
                    builder.clientAuth(ClientAuth.OPTIONAL);
                    break;
            }
            // Note: we ignore endpointIdentificationAlgorithm here.
            // It is only relevant for client mode, and we are in server mode.
            log.info("netty is enabled for SSL context with keystore {}, truststore {}.",
                    factory.keystore(), factory.truststore());
            return builder.build();
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }
}
