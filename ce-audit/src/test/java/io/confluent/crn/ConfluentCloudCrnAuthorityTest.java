package io.confluent.crn;

import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import java.util.Arrays;
import java.util.Collections;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.utils.Utils;
import org.junit.Assert;
import org.junit.Test;

public class ConfluentCloudCrnAuthorityTest {

    private ConfluentCloudCrnAuthority authority = new ConfluentCloudCrnAuthority(100);

    @Test
    public void testResolveScopedTopic() throws CrnSyntaxException {
        String crnString = "crn://confluent.cloud/organization=1234/environment=t55/cloud-cluster=lkc-abc123/kafka=lkc-abc123/topic=topic_1";
        ConfluentResourceName crn = authority.canonicalCrn(crnString);

        ScopedResourcePattern expected = new ScopedResourcePattern(
                new Scope(Arrays.asList("organization=1234", "environment=t55", "cloud-cluster=lkc-abc123"),
                        Utils.mkMap(Utils.mkEntry("kafka-cluster", "lkc-abc123"))),
                new ResourcePattern("Topic", "topic_1", PatternType.LITERAL));

        ScopedResourcePattern scopedResourcePattern = authority.resolveScopePattern(crn);
        Assert.assertEquals(expected, scopedResourcePattern);
        Assert.assertEquals(authority.canonicalCrn(expected.scope(), expected.resourcePattern()), crn);
    }

    @Test
    public void testResolveCloudSRCluster() throws CrnSyntaxException {
        String crnString = "crn://confluent.cloud/organization=1234/environment=t55/schema-registry=lsrc-abc123";
        ConfluentResourceName crn = authority.canonicalCrn(crnString);

        ScopedResourcePattern expected = new ScopedResourcePattern(
            new Scope(Arrays.asList("organization=1234", "environment=t55"),
                Utils.mkMap(Utils.mkEntry("schema-registry-cluster", "lsrc-abc123"))),
            new ResourcePattern("SchemaRegistryCluster", "schema-registry-cluster", PatternType.LITERAL));

        ScopedResourcePattern scopedResourcePattern = authority.resolveScopePattern(crn);
        Assert.assertEquals(expected, scopedResourcePattern);
        Assert.assertEquals(authority.canonicalCrn(expected.scope(), expected.resourcePattern()), crn);
    }

    @Test
    public void testResolveCloudKsqlCluster() throws CrnSyntaxException {
        String crnString = "crn://confluent.cloud/organization=1234/environment=t55/cloud-cluster=lkc-123/ksql=lksqlc-abc123";
        ConfluentResourceName crn = authority.canonicalCrn(crnString);

        ScopedResourcePattern expected = new ScopedResourcePattern(
            new Scope(Arrays.asList("organization=1234", "environment=t55", "cloud-cluster=lkc-123"),
                Utils.mkMap(Utils.mkEntry("ksql-cluster", "lksqlc-abc123"))),
            new ResourcePattern("KsqlCluster", "ksql-cluster", PatternType.LITERAL));

        ScopedResourcePattern scopedResourcePattern = authority.resolveScopePattern(crn);
        Assert.assertEquals(expected, scopedResourcePattern);
        Assert.assertEquals(authority.canonicalCrn(expected.scope(), expected.resourcePattern()), crn);
    }

    @Test
    public void testResolveUser() throws CrnSyntaxException {
        String crnString = "crn://confluent.cloud/organization=1234/user=123";
        ConfluentResourceName crn = authority.canonicalCrn(crnString);

        ScopedResourcePattern expected = new ScopedResourcePattern(
                new Scope(Arrays.asList("organization=1234"), Collections.emptyMap()),
                new ResourcePattern("User", "123", PatternType.LITERAL));

        ScopedResourcePattern scopedResourcePattern = authority.resolveScopePattern(crn);
        Assert.assertEquals(expected, scopedResourcePattern);
        Assert.assertEquals(authority.canonicalCrn(expected.scope(), expected.resourcePattern()), crn);
    }

    @Test
    public void testMissingOrg() throws CrnSyntaxException {
        String[] tests = {
                "crn://confluent.cloud/environment=t55/cloud-cluster=lkc-abc123/kafka=lkc-abc123/topic=topic_1",
                "crn://confluent.cloud/user=123"
        };

        for (String t: tests) {
            Assert.assertThrows(CrnSyntaxException.class, () -> {
                authority.canonicalCrn(t);
            });
        }
    }


    @Test
    public void testMissingEnv() throws CrnSyntaxException {
        String[] tests = {
                "crn://confluent.cloud/organization=1234/cloud-cluster=lkc-abc123/kafka=lkc-abc123",
                "crn://confluent.cloud/organization=1234/cloud-cluster=lkc-abc123/kafka=lkc-abc123/topic=topic_1",
        };

        for (String t: tests) {
            Assert.assertThrows(CrnSyntaxException.class, () -> {
                authority.canonicalCrn(t);
            });
        }
    }

    @Test
    public void testMissingName() throws CrnSyntaxException {
        String[] tests = {
            "crn://confluent.cloud/organization=/environment=t55/cloud-cluster=lkc-abc123/kafka=lkc-abc123/topic=topic_1",
            "crn://confluent.cloud/organization=1234/environment=/cloud-cluster=lkc-abc123/kafka=lkc-abc123/topic=topic_1",
            "crn://confluent.cloud/organization=1234/environment=t55/cloud-cluster=/kafka=lkc-abc123/topic=topic_1",
        };

        for (String t: tests) {
            Assert.assertThrows(CrnSyntaxException.class, () -> {
                authority.canonicalCrn(t);
            });
        }

        Scope[] scopes = {
            new Scope(Arrays.asList("organization=", "environment=t55"),
                Utils.mkMap(Utils.mkEntry("kafka-cluster", "lkc-abc123"))),
            new Scope(Arrays.asList("organization=1234", "environment="),
                Utils.mkMap(Utils.mkEntry("kafka-cluster", "lkc-abc123"))),
            new Scope(Arrays.asList("organization=1234", "environment=t55", "cloud-cluster="),
                Utils.mkMap(Utils.mkEntry("kafka-cluster", "lkc-abc123"))),
        };
        ResourcePattern resourcePattern = new ResourcePattern("Topic", "topic_1", PatternType.LITERAL);

        for (Scope scope : scopes) {
            Assert.assertThrows(CrnSyntaxException.class, () -> {
                authority.canonicalCrn(scope, resourcePattern);
            });
        }

    }

}
