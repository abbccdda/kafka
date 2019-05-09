// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.provider;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

public class BuiltInAuthProviders {

    public enum BasicAuthCredentialProviders {
        USER_INFO, // UserInfo credential provider
    }

    public static Set<String> builtInBasicAuthCredentialProviders() {
        return Utils.mkSet(BasicAuthCredentialProviders.values()).stream()
                .map(BasicAuthCredentialProviders::name).collect(Collectors.toSet());
    }

    public static BasicAuthCredentialProvider loadBasicAuthCredentialProvider(String name) {
        ServiceLoader<BasicAuthCredentialProvider> providers = ServiceLoader.load(
            BasicAuthCredentialProvider.class,
            BuiltInAuthProviders.class.getClassLoader()
        );
        for (BasicAuthCredentialProvider provider : providers) {
            if (provider.providerName().equals(name)) {
                return provider;
            }
        }
        throw new ConfigException("BasicAuthCredentialProvider not found for " + name);
    }

    public enum HttpCredentialProviders {
        BASIC, // HTTP Basic credential provider
        BEARER, // HTTP Bearer token credential provider
    }

    public static Set<String> builtInHttpCredentialProviders() {
        return Utils.mkSet(HttpCredentialProviders.values()).stream()
                .map(HttpCredentialProviders::name).collect(Collectors.toSet());
    }

    public static HttpCredentialProvider loadHttpCredentialProviders(String name) {
        ServiceLoader<HttpCredentialProvider> providers = ServiceLoader.load(
            HttpCredentialProvider.class,
            BasicAuthCredentialProvider.class.getClassLoader()
        );
        for (HttpCredentialProvider provider : providers) {
            if (provider.getScheme().equalsIgnoreCase(name)) {
                return provider;
            }
        }
        throw new ConfigException("HttpCredentialProvider not found for " + name);
    }

}