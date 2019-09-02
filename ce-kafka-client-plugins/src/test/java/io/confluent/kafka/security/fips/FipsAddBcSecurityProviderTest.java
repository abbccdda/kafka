
package io.confluent.kafka.security.fips;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.hamcrest.MatcherAssert;

import java.security.Provider;
import java.security.Security;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.security.SecurityProviderCreator;
import org.apache.kafka.common.utils.SecurityUtils;
import io.confluent.kafka.security.fips.provider.BcFipsJsseProviderCreator;
import io.confluent.kafka.security.fips.provider.BcFipsProviderCreator;

public class FipsAddBcSecurityProviderTest {
    private static final String BCFIPS_PROVIDER_NAME = "BCFIPS";
    private static final String BCFIPS_JSSE_PROVIDER_NAME = "BCJSSE";
    private SecurityProviderCreator testBcFipsProviderCreator = new BcFipsProviderCreator();
    private SecurityProviderCreator testBcFipsJsseProviderCreator = new BcFipsJsseProviderCreator();

    private void clearTestProviders() {
        if (Security.getProvider(BCFIPS_JSSE_PROVIDER_NAME) != null)
            Security.removeProvider(BCFIPS_JSSE_PROVIDER_NAME);
        if (Security.getProvider(BCFIPS_PROVIDER_NAME) != null)
            Security.removeProvider(BCFIPS_PROVIDER_NAME);
    }

    @Before
    // Remove the providers if already added
    public void setUp() {
        clearTestProviders();
    }

    // Remove the providers after running test cases
    @After
    public void tearDown() {
        clearTestProviders();
    }

    private int getProviderIndexFromName(String providerName, Provider[] providers) {
        for (int index = 0; index < providers.length; index++) {
            if (providers[index].getName().equals(providerName)) {
                return index;
            }
        }
        return -1;
    }

    // Tests if the custom providers configured are being added to the JVM correctly. These providers are
    // expected to be added at the start of the list of available providers and with the relative ordering maintained
    @Test
    public void testAddCustomSecurityProvider() {
        String bcProviderClasses = testBcFipsProviderCreator.getClass().getName() + "," +
                testBcFipsJsseProviderCreator.getClass().getName();
        Map<String, String> configs = new HashMap<>();
        configs.put(SecurityConfig.SECURITY_PROVIDERS_CONFIG, bcProviderClasses);
        SecurityUtils.addConfiguredSecurityProviders(configs);

        Provider[] providers = Security.getProviders();
        int testBcFipsrProviderIndex = getProviderIndexFromName(BCFIPS_PROVIDER_NAME, providers);
        int testBcFipsJsseProviderIndex = getProviderIndexFromName(BCFIPS_JSSE_PROVIDER_NAME, providers);

        // validations
        MatcherAssert.assertThat(BCFIPS_PROVIDER_NAME + " testProvider not found at expected index", testBcFipsrProviderIndex == 0);
        MatcherAssert.assertThat(BCFIPS_JSSE_PROVIDER_NAME + " testProvider not found at expected index", testBcFipsJsseProviderIndex == 1);
    }
}
