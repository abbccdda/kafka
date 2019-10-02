/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.crn;

import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import java.util.Arrays;
import java.util.Collections;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.utils.Utils;
import org.junit.Assert;
import org.junit.Test;

public class ConfluentServerCrnAuthorityTest {

  private ConfluentServerCrnAuthority authority = new ConfluentServerCrnAuthority(
      "mds.example.com", 100);

  @Test
  public void testResolveTopic() throws CrnSyntaxException {

    ScopedResourcePattern scopedResourcePattern = authority.resolveScopePattern(
        ConfluentResourceName.fromString("crn://mds.example.com/kafka=abc123/topic=topic_1"));

    ScopedResourcePattern expected = new ScopedResourcePattern(
        new Scope(Collections.emptyList(), Utils.mkMap(Utils.mkEntry("kafka-cluster", "abc123"))),
        new ResourcePattern("Topic", "topic_1", PatternType.LITERAL));

    Assert.assertEquals(expected, scopedResourcePattern);

  }

  @Test
  public void testResolveScopedtopic() throws CrnSyntaxException {

    ScopedResourcePattern scopedResourcePattern = authority.resolveScopePattern(
        ConfluentResourceName.fromString(
            "crn://mds.example.com/environment=prod/kafka=abc123/topic=topic_1"));

    ScopedResourcePattern expected = new ScopedResourcePattern(
        new Scope(Collections.singletonList("prod"),
            Utils.mkMap(Utils.mkEntry("kafka-cluster", "abc123"))),
        new ResourcePattern("Topic", "topic_1", PatternType.LITERAL));

    Assert.assertEquals(expected, scopedResourcePattern);

  }

  @Test
  public void testResolveDeeplyScopedTopic() throws CrnSyntaxException {

    ScopedResourcePattern scopedResourcePattern = authority.resolveScopePattern(
        ConfluentResourceName.fromString(
            "crn://mds.example.com/organization=company/organization=dept/environment=prod/kafka=abc123/topic=topic_1"));

    ScopedResourcePattern expected = new ScopedResourcePattern(
        new Scope(Arrays.asList("company", "dept", "prod"),
            Utils.mkMap(Utils.mkEntry("kafka-cluster", "abc123"))),
        new ResourcePattern("Topic", "topic_1", PatternType.LITERAL));

    Assert.assertEquals(expected, scopedResourcePattern);

  }

  @Test
  public void testCrnForTopic() throws CrnSyntaxException {

    Assert.assertEquals(
        "crn://mds.example.com/kafka=abc123/topic=topic_1",
        authority.canonicalCrn(
            new Scope(Collections.emptyList(),
                Utils.mkMap(Utils.mkEntry("kafka-cluster", "abc123"))),
            new ResourcePattern("Topic", "topic_1", PatternType.LITERAL)
        ).toString()
    );

  }

  @Test
  public void testCrnForQuery() throws CrnSyntaxException {

    Assert.assertEquals(
        "crn://mds.example.com/kafka=abc123/ksql=_default/query=clicks",
        authority.canonicalCrn(
            new Scope(Collections.emptyList(),
                Utils.mkMap(Utils.mkEntry("kafka-cluster", "abc123"),
                    Utils.mkEntry("ksql-cluster", "_default"))),
            new ResourcePattern("Query", "clicks", PatternType.LITERAL)
        ).toString()
    );

  }

  @Test
  public void testCrnForCluster() throws CrnSyntaxException {

    Assert.assertEquals(
        "crn://mds.example.com/kafka=abc123",
        authority.canonicalCrn(
            new Scope(Collections.emptyList(),
                Utils.mkMap(Utils.mkEntry("kafka-cluster", "abc123"))),
            new ResourcePattern("Cluster", "kafka-cluster", PatternType.LITERAL)
        ).toString()
    );

  }

  @Test
  public void testCrnForScope() throws CrnSyntaxException {

    Assert.assertEquals(
        "crn://mds.example.com/kafka=abc123",
        authority.canonicalCrn(
            new Scope(Collections.emptyList(),
                Utils.mkMap(Utils.mkEntry("kafka-cluster", "abc123")))
        ).toString()
    );

  }

  @Test
  public void testCanonicalOrgEnv() throws CrnSyntaxException {
    Assert.assertEquals(
        authority.canonicalCrn(
            "crn://mds.example.com/organization=mycompany/organization=prod/kafka=abc123/topic=topic_1"),
        ConfluentResourceName.fromString(
            "crn://mds.example.com/organization=mycompany/environment=prod/kafka=abc123/topic=topic_1")
    );
    Assert.assertEquals(
        authority.canonicalCrn(
            "crn://mds.example.com/environment=mycompany/environment=prod/kafka=abc123/topic=topic_1"),
        ConfluentResourceName.fromString(
            "crn://mds.example.com/organization=mycompany/environment=prod/kafka=abc123/topic=topic_1")
    );
    Assert.assertEquals(
        authority.canonicalCrn(
            "crn://mds.example.com/environment=mycompany/organization=prod/kafka=abc123/topic=topic_1"),
        ConfluentResourceName.fromString(
            "crn://mds.example.com/organization=mycompany/environment=prod/kafka=abc123/topic=topic_1")
    );
    Assert.assertEquals(
        authority.canonicalCrn(
            "crn://mds.example.com/environment=mycompany/organization=europe/organization=prod/kafka=abc123/topic=topic_1"),
        ConfluentResourceName.fromString(
            "crn://mds.example.com/organization=mycompany/organization=europe/environment=prod/kafka=abc123/topic=topic_1")
    );
  }

  @Test
  public void testEquivalent() throws CrnSyntaxException {

    // This resolver doesn't care about the distinction between Org and Env
    Assert.assertTrue(
        authority.areEquivalent(
            ConfluentResourceName.fromString(
                "crn://mds.example.com/environment=prod/kafka=abc123/topic=topic_1"),
            ConfluentResourceName.fromString(
                "crn://mds.example.com/organization=prod/kafka=abc123/topic=topic_1")
        )
    );

    Assert.assertFalse(
        authority.areEquivalent(
            ConfluentResourceName.fromString(
                "crn://mds.example.com/environment=prod/kafka=abc123/topic=topic_1"),
            ConfluentResourceName.fromString(
                "crn://mds.example.com/organization=prod/kafka=abc123/topic=topic_2")
        )
    );

    Assert.assertFalse(
        authority.areEquivalent(
            ConfluentResourceName.fromString(
                "crn://mds.example.com/environment=stag/kafka=abc123/topic=topic_1"),
            ConfluentResourceName.fromString(
                "crn://mds.example.com/organization=prod/kafka=abc123/topic=topic_1")
        )
    );

  }

  @Test
  public void testCrnDeeplyScopedTopic() throws CrnSyntaxException {

    Assert.assertEquals(
        "crn://mds.example.com/organization=company/organization=dept/environment=prod/kafka=abc123/topic=topic_1",
        authority.canonicalCrn(
            new Scope(Arrays.asList("company", "dept", "prod"),
                Utils.mkMap(Utils.mkEntry("kafka-cluster", "abc123"))),
            new ResourcePattern("Topic", "topic_1", PatternType.LITERAL)
        ).toString());

  }

  @Test
  public void testResolveTopicPrefix() throws CrnSyntaxException {

    ScopedResourcePattern scopedResourcePattern = authority.resolveScopePattern(
        ConfluentResourceName.fromString("crn://mds.example.com/kafka=abc123/topic=topic_*"));

    ScopedResourcePattern expected = new ScopedResourcePattern(
        new Scope(Collections.emptyList(), Utils.mkMap(Utils.mkEntry("kafka-cluster", "abc123"))),
        new ResourcePattern("Topic", "topic_", PatternType.PREFIXED));

    Assert.assertEquals(expected, scopedResourcePattern);

  }

  @Test
  public void testResolveTopicAny() throws CrnSyntaxException {

    ScopedResourcePattern scopedResourcePattern = authority.resolveScopePattern(
        ConfluentResourceName.fromString("crn://mds.example.com/kafka=abc123/topic=*"));

    ScopedResourcePattern expected = new ScopedResourcePattern(
        new Scope(Collections.emptyList(), Utils.mkMap(Utils.mkEntry("kafka-cluster", "abc123"))),
        new ResourcePattern("Topic", "", PatternType.ANY));

    Assert.assertEquals(expected, scopedResourcePattern);

  }

  @Test
  public void testCrnEncoding() throws CrnSyntaxException {

    Assert.assertEquals(
        "crn://mds.example.com/kafka=abc123/group=annoying%3Dgroup%2Fname",
        authority.canonicalCrn(
            new Scope(Collections.emptyList(),
                Utils.mkMap(Utils.mkEntry("kafka-cluster", "abc123"))),
            new ResourcePattern("Group", "annoying=group/name", PatternType.LITERAL)
        ).toString()
    );
  }

  @Test
  public void testCrnDecoding() throws CrnSyntaxException {

    ScopedResourcePattern scopedResourcePattern = authority.resolveScopePattern(
        ConfluentResourceName
            .fromString("crn://mds.example.com/kafka=abc123/group=annoying%3Dgroup%2Fname"));

    ScopedResourcePattern expected = new ScopedResourcePattern(
        new Scope(Collections.emptyList(),
            Utils.mkMap(Utils.mkEntry("kafka-cluster", "abc123"))),
        new ResourcePattern("Group", "annoying=group/name", PatternType.LITERAL));

    Assert.assertEquals(expected, scopedResourcePattern);
  }

  @Test
  public void testCrnEncodingWildcard() throws CrnSyntaxException {

    Assert.assertEquals(
        "crn://mds.example.com/kafka=abc123/group=annoying%3Dgroup%2Fname*",
        authority.canonicalCrn(
            new Scope(Collections.emptyList(),
                Utils.mkMap(Utils.mkEntry("kafka-cluster", "abc123"))),
            new ResourcePattern("Group", "annoying=group/name", PatternType.PREFIXED)
        ).toString()
    );
  }

  @Test
  public void testCrnDecodingWildcard() throws CrnSyntaxException {

    ScopedResourcePattern scopedResourcePattern = authority.resolveScopePattern(
        ConfluentResourceName
            .fromString("crn://mds.example.com/kafka=abc123/group=annoying%3Dgroup%2Fname*"));

    ScopedResourcePattern expected = new ScopedResourcePattern(
        new Scope(Collections.emptyList(),
            Utils.mkMap(Utils.mkEntry("kafka-cluster", "abc123"))),
        new ResourcePattern("Group", "annoying=group/name", PatternType.PREFIXED));

    Assert.assertEquals(expected, scopedResourcePattern);
  }

  @Test
  public void testCrnEncodingWildcardInName() throws CrnSyntaxException {

    Assert.assertEquals(
        "crn://mds.example.com/kafka=abc123/group=annoying%3Dgroup%2Fname%2A",
        authority.canonicalCrn(
            new Scope(Collections.emptyList(),
                Utils.mkMap(Utils.mkEntry("kafka-cluster", "abc123"))),
            new ResourcePattern("Group", "annoying=group/name*", PatternType.LITERAL)
        ).toString()
    );
  }

  @Test
  public void testCrnDecodingWildcardInName() throws CrnSyntaxException {

    ScopedResourcePattern scopedResourcePattern = authority.resolveScopePattern(
        ConfluentResourceName
            .fromString("crn://mds.example.com/kafka=abc123/group=annoying%3Dgroup%2Fname%2A"));

    ScopedResourcePattern expected = new ScopedResourcePattern(
        new Scope(Collections.emptyList(),
            Utils.mkMap(Utils.mkEntry("kafka-cluster", "abc123"))),
        new ResourcePattern("Group", "annoying=group/name*", PatternType.LITERAL));

    Assert.assertEquals(expected, scopedResourcePattern);
  }

  @Test
  public void testWrongAuthority() {
    Assert.assertThrows(CrnSyntaxException.class, () ->
        authority.resolveScopePattern(
            ConfluentResourceName
                .fromString("crn://confluent.cloud/kafka=abc123/topic=topic_1"))
    );
  }


  @Test
  public void testTopicTopic() {
    Assert.assertThrows(CrnSyntaxException.class, () ->
        authority.resolveScopePattern(
            ConfluentResourceName
                .fromString("crn://confluent.cloud/kafka=abc123/topic=topic_1/topic=topic_2"))
    );
  }

  @Test
  public void testMissingKafkaCluster() throws CrnSyntaxException {

    Assert.assertThrows(
        CrnSyntaxException.class,
        () -> authority.canonicalCrn(
            new Scope(Arrays.asList("company", "dept", "prod"),
                Utils.mkMap(Utils.mkEntry("ksql-cluster", "_default"))),
            new ResourcePattern("Topic", "topic_1", PatternType.LITERAL)
        ).toString());
  }

  @Test
  public void testUnknownClusterType() throws CrnSyntaxException {

    Assert.assertThrows(
        CrnSyntaxException.class,
        () -> authority.canonicalCrn(
            new Scope(Arrays.asList("company", "dept", "prod"),
                Utils.mkMap(Utils.mkEntry("kafka-cluster", "abc123"),
                    Utils.mkEntry("weird-cluster", "strange"))),
            new ResourcePattern("Topic", "topic_1", PatternType.LITERAL)
        ).toString());
  }

  @Test
  public void testCrnDecodingWildcardCluster() throws CrnSyntaxException {

    ScopedResourcePattern scopedResourcePattern = authority.resolveScopePattern(
        ConfluentResourceName
            .fromString("crn://mds.example.com/kafka=*/group=annoying%3Dgroup%2Fname%2A"));

    ScopedResourcePattern expected = new ScopedResourcePattern(
        new Scope(Collections.emptyList(),
            Utils.mkMap(Utils.mkEntry("kafka-cluster", "*"))),
        new ResourcePattern("Group", "annoying=group/name*", PatternType.LITERAL));

    Assert.assertEquals(expected, scopedResourcePattern);
  }

  @Test
  public void testMatchesLiteral() throws CrnSyntaxException {
    Assert.assertTrue(
        ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=abc123/topic=topic_1")
            .matches(
                ConfluentResourceName.fromString(
                    "crn://confluent.cloud/kafka=abc123/topic=topic_1"))
    );
    Assert.assertFalse(
        ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=abc123/topic=topic_1")
            .matches(
                ConfluentResourceName.fromString(
                    "crn://confluent.cloud/kafka=abc123/topic=topic_2"))
    );
    Assert.assertFalse(
        ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=abc123/topic=topic_1")
            .matches(
                ConfluentResourceName.fromString(
                    "crn://confluent.cloud/kafka=abc456/topic=topic_1"))
    );
    Assert.assertFalse(
        ConfluentResourceName.fromString(
            "crn://mds.example.com/kafka=abc123/topic=topic_1")
            .matches(
                ConfluentResourceName.fromString(
                    "crn://confluent.cloud/kafka=abc123/topic=topic_1"))
    );
    Assert.assertFalse(
        ConfluentResourceName.fromString(
            "crn://mds.example.com/kafka=abc123/topic=topic_1")
            .matches(
                ConfluentResourceName.fromString(
                    "crn://confluent.cloud/organization=777/kafka=abc123/topic=topic_1"))
    );
  }


  @Test
  public void testMatchesPrefix() throws CrnSyntaxException {
    Assert.assertTrue(
        ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=abc123/topic=topic_*")
            .matches(
                ConfluentResourceName.fromString(
                    "crn://confluent.cloud/kafka=abc123/topic=topic_1"))
    );
    Assert.assertTrue(
        ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=abc123/topic=topic_*")
            .matches(
                ConfluentResourceName.fromString(
                    "crn://confluent.cloud/kafka=abc123/topic=topic_2"))
    );
    Assert.assertFalse(
        ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=abc123/topic=topic_1")
            .matches(
                ConfluentResourceName.fromString(
                    "crn://confluent.cloud/kafka=abc123/topic=topic_*"))
    );
    Assert.assertFalse(
        ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=abc123/topic=topic_*")
            .matches(
                ConfluentResourceName.fromString(
                    "crn://confluent.cloud/kafka=abc456/topic=topic_1"))
    );
    Assert.assertFalse(
        ConfluentResourceName.fromString(
            "crn://mds.example.com/kafka=abc123/topic=topic_*")
            .matches(
                ConfluentResourceName.fromString(
                    "crn://confluent.cloud/kafka=abc123/topic=topic_1"))
    );
    Assert.assertFalse(
        ConfluentResourceName.fromString(
            "crn://mds.example.com/kafka=abc123/topic=topic_*")
            .matches(
                ConfluentResourceName.fromString(
                    "crn://confluent.cloud/organization=777/kafka=abc123/topic=topic_1"))
    );
  }

  @Test
  public void testMatchesWildcardCluster() throws CrnSyntaxException {
    Assert.assertTrue(
        ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=*/topic=topic_1")
            .matches(
                ConfluentResourceName.fromString(
                    "crn://confluent.cloud/kafka=abc123/topic=topic_1"))
    );
    Assert.assertTrue(
        ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=abc*/topic=topic_1")
            .matches(
                ConfluentResourceName.fromString(
                    "crn://confluent.cloud/kafka=abc456/topic=topic_1"))
    );
    Assert.assertFalse(
        ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=*/topic=topic_1")
            .matches(
                ConfluentResourceName.fromString(
                    "crn://confluent.cloud/kafka=abc123/topic=topic_*"))
    );
    Assert.assertFalse(
        ConfluentResourceName.fromString(
            "crn://confluent.cloud/kafka=*/topic=topic_1")
            .matches(
                ConfluentResourceName.fromString(
                    "crn://confluent.cloud/kafka=abc456/topic=topic_2"))
    );
    Assert.assertFalse(
        ConfluentResourceName.fromString(
            "crn://mds.example.com/kafka=*/topic=topic_1")
            .matches(
                ConfluentResourceName.fromString(
                    "crn://confluent.cloud/kafka=abc123/topic=topic_1"))
    );
    Assert.assertFalse(
        ConfluentResourceName.fromString(
            "crn://mds.example.com/kafka=*/topic=topic_1")
            .matches(
                ConfluentResourceName.fromString(
                    "crn://confluent.cloud/organization=777/kafka=abc123/topic=topic_1"))
    );
  }

}
