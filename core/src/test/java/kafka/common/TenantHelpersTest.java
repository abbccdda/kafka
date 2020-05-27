/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TenantHelpersTest {

    @Test
    public void testIsTenantPrefixed() {
        assertTrue(TenantHelpers.isTenantPrefixed("lkc-8vgl6g2_console-consumer"));
        assertTrue(TenantHelpers.isTenantPrefixed("lkc-8vgl6g2_bla"));
        assertFalse(TenantHelpers.isTenantPrefixed("lkc-8vg"));
        assertFalse(TenantHelpers.isTenantPrefixed("bla"));
    }

    @Test
    public void testExtractTenantPrefix() {
        assertEquals("lkc-8vgl6g2_",
            TenantHelpers.extractTenantPrefix("lkc-8vgl6g2_console-consumer"));
        assertEquals("lkc-8vgl6g2_",
            TenantHelpers.extractTenantPrefix("lkc-8vgl6g2_bla"));
        assertThrows(IllegalArgumentException.class,
            () -> TenantHelpers.extractTenantPrefix("lkc-8vg"));
        assertThrows(IllegalArgumentException.class,
            () -> TenantHelpers.extractTenantPrefix("bla"));
    }

}
