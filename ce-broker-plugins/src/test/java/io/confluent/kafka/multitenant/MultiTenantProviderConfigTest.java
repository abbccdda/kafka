package io.confluent.kafka.multitenant;

import io.confluent.kafka.multitenant.authorizer.MultiTenantAuthorizer;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Test;

public class MultiTenantProviderConfigTest {

    @Test
    public void testUnconfigured() {
        MultiTenantAuthorizer mta = new MultiTenantAuthorizer();

        HashMap<String, Object> config = new HashMap<>();
        mta.configureAccessRuleProviders(config);
        Assert.assertEquals(AccessRuleProviders.MULTI_TENANT.name(),
                config.get(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP));
    }

    @Test
    public void testConfiguredWithoutMT() {
        MultiTenantAuthorizer mta = new MultiTenantAuthorizer();

        HashMap<String, Object> config = new HashMap<>();
        config.put(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, String.join(",",
                AccessRuleProviders.ZK_ACL.name(),
                AccessRuleProviders.CONFLUENT.name()));
        mta.configureAccessRuleProviders(config);
        Assert.assertEquals(AccessRuleProviders.MULTI_TENANT.name(),
                config.get(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP));
    }

    @Test
    public void testConfiguredAsMT() {
        MultiTenantAuthorizer mta = new MultiTenantAuthorizer();

        HashMap<String, Object> config = new HashMap<>();
        config.put(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP,
                AccessRuleProviders.MULTI_TENANT.name());
        mta.configureAccessRuleProviders(config);
        Assert.assertEquals(AccessRuleProviders.MULTI_TENANT.name(),
                config.get(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP));
    }

    @Test
    public void testConfiguredWithMTAndOther() {
        MultiTenantAuthorizer mta = new MultiTenantAuthorizer();

        HashMap<String, Object> config = new HashMap<>();
        String original = String.join(",",
                AccessRuleProviders.MULTI_TENANT.name(),
                AccessRuleProviders.CONFLUENT.name());
        config.put(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, original);
        mta.configureAccessRuleProviders(config);
        Assert.assertEquals(original,
                config.get(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP));
    }
}
