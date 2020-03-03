package io.confluent.kafka.multitenant.schema;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.TenantMetadata;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TenantContextTest {

  @Test(expected = IllegalArgumentException.class)
  public void testTenantNameCannotIncludePrefixDelimeter() {
    new TenantContext(new MultiTenantPrincipal("user",
        new TenantMetadata("foo" + TenantContext.DELIMITER + "bar", "cluster")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTenantNameIsValidTopicPrefix() {
    // '|' is not a permitted character in topic names
    new TenantContext(new MultiTenantPrincipal("user",
            new TenantMetadata("foo|bar", "cluster")));
  }

  @Test
  public void shouldExtractValidPrefix() throws Exception {
    assertEquals("confluent_", TenantContext.extractTenantPrefix("confluent_foo"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailExtractingMissingPrefix() throws Exception {
    TenantContext.extractTenantPrefix("blah");
  }

  @Test
  public void shouldIdentifyPrefixedTopics() throws Exception {
    assertTrue(TenantContext.isTenantPrefixed("confluent_foo"));
    assertFalse(TenantContext.isTenantPrefixed("blah"));
    assertFalse(TenantContext.isTenantPrefixed(null));
    assertFalse(TenantContext.isTenantPrefixed("__consumer_offsets"));
    assertFalse(TenantContext.isTenantPrefixed("_confluent_metrics"));
  }

  @Test
  public void shouldIdentifyTopicsWithTenantPrefix() throws Exception {
    assertTrue(TenantContext.hasTenantPrefix("confluent_", "confluent_foo"));
    assertFalse(TenantContext.hasTenantPrefix("xyz_", "confluent_foo"));
    assertFalse(TenantContext.hasTenantPrefix("blah", "blah"));
    assertFalse(TenantContext.hasTenantPrefix("confluent_", null));
  }

  @Test
  public void shouldRemoveAllTenantPrefixes() throws Exception {
    TenantContext context = new TenantContext(
        new MultiTenantPrincipal("user", new TenantMetadata("tenant", "cluster")));
    String uncleaned = "Failed to create topics tenant_foo, tenant_tenantbar, and tenant_tenant_blah";
    assertEquals("tenant_".length() * 4, context.sizeOfRemovedPrefixes(uncleaned));
    String cleaned = context.removeAllTenantPrefixes(uncleaned);
    assertEquals("Failed to create topics foo, tenantbar, and blah", cleaned);
  }

  @Test
  public void shouldQuoteTenantNameInPattern() throws Exception {
    // Contrived test case to ensure we have proper quoting of the tenant prefix
    TenantContext context = new TenantContext(
        new MultiTenantPrincipal("user", new TenantMetadata("bl.h", "cluster")));
    String uncleaned = "This matches 'bl.h_foo'. This does not: blah_foo";
    String cleaned = context.removeAllTenantPrefixes(uncleaned);
    assertEquals("This matches 'foo'. This does not: blah_foo", cleaned);
  }

  @Test
  public void shouldPermitEmptyGroupId() throws Exception {
    TenantContext context = new TenantContext(
        new MultiTenantPrincipal("user", new TenantMetadata("tenant", "cluster")));
    String groupId = context.addTenantPrefix("");
    assertEquals("tenant_", groupId);
    assertEquals("", context.removeTenantPrefix("tenant_"));

    String message = "This is a message referring to the empty group 'tenant_'";
    assertTrue(message.contains(context.prefix()));

    String cleanedMessage = context.removeAllTenantPrefixes(message);
    assertFalse(cleanedMessage.contains(context.prefix()));
  }

}