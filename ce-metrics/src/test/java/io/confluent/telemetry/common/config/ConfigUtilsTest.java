package io.confluent.telemetry.common.config;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

// adapted from https://github.com/confluentinc/common/blob/master/config/src/test/java/io/confluent/common/config/ConfigUtilsTest.java
public class ConfigUtilsTest {

    @Test
    public void testTranslateDeprecated() {
        Map<String, Object> props = new HashMap<>();
        props.put("foo.bar", "baz");
        props.put("foo.bar.deprecated", "quux");
        props.put("chicken", "1");
        props.put("rooster", "2");
        props.put("hen", "3");
        props.put("heifer", "moo");
        props.put("blah", "blah");
        props.put("unexpected.non.string.object", 42);
        Map<String, Object> newProps = ConfigUtils.translateDeprecated(props, new String[][]{
            {"foo.bar", "foo.bar.deprecated"},
            {"chicken", "rooster", "hen"},
            {"cow", "beef", "heifer", "steer"}
        });
        assertEquals("baz", newProps.get("foo.bar"));
        assertEquals(null, newProps.get("foobar.deprecated"));
        assertEquals("1", newProps.get("chicken"));
        assertEquals(null, newProps.get("rooster"));
        assertEquals(null, newProps.get("hen"));
        assertEquals("moo", newProps.get("cow"));
        assertEquals(null, newProps.get("beef"));
        assertEquals(null, newProps.get("heifer"));
        assertEquals(null, newProps.get("steer"));
        assertEquals(null, props.get("cow"));
        assertEquals("blah", props.get("blah"));
        assertEquals("blah", newProps.get("blah"));
        assertEquals(42, newProps.get("unexpected.non.string.object"));
        assertEquals(42, props.get("unexpected.non.string.object"));

    }

    @Test
    public void testAllowsNewKey() {
        Map<String, String> props = new HashMap<>();
        props.put("foo.bar", "baz");
        Map<String, String> newProps = ConfigUtils.translateDeprecated(props, new String[][]{
            {"foo.bar", "foo.bar.deprecated"},
            {"chicken", "rooster", "hen"},
            {"cow", "beef", "heifer", "steer"}
        });
        assertNotNull(newProps);
        assertEquals("baz", newProps.get("foo.bar"));
        assertNull(newProps.get("foo.bar.deprecated"));
    }

    @Test
    public void testAllowDeprecatedNulls() {
        Map<String, String> props = new HashMap<>();
        props.put("foo.bar.deprecated", null);
        props.put("foo.bar", "baz");
        Map<String, String> newProps = ConfigUtils.translateDeprecated(props, new String[][]{
            {"foo.bar", "foo.bar.deprecated"}
        });
        assertNotNull(newProps);
        assertEquals("baz", newProps.get("foo.bar"));
        assertNull(newProps.get("foo.bar.deprecated"));
    }

    @Test
    public void testAllowNullOverride() {
        Map<String, String> props = new HashMap<>();
        props.put("foo.bar.deprecated", "baz");
        props.put("foo.bar", null);
        Map<String, String> newProps = ConfigUtils.translateDeprecated(props, new String[][]{
            {"foo.bar", "foo.bar.deprecated"}
        });
        assertNotNull(newProps);
        assertNull(newProps.get("foo.bar"));
        assertNull(newProps.get("foo.bar.deprecated"));
    }

    @Test
    public void testNullMapEntriesWithoutAliasesDoNotThrowNPE() {
        Map<String, String> props = new HashMap<>();
        props.put("other", null);
        Map<String, String> newProps = ConfigUtils.translateDeprecated(props, new String[][]{
            {"foo.bar", "foo.bar.deprecated"}
        });
        assertNotNull(newProps);
        assertNull(newProps.get("other"));
    }

    @Test
    public void testDuplicateSynonyms() {
        Map<String, String> props = new HashMap<>();
        props.put("foo.bar", "baz");
        props.put("foo.bar.deprecated", "derp");
        Map<String, String> newProps = ConfigUtils.translateDeprecated(props, new String[][]{
            {"foo.bar", "foo.bar.deprecated"},
            {"chicken", "foo.bar.deprecated"}
        });
        assertNotNull(newProps);
        assertEquals("baz", newProps.get("foo.bar"));
        assertEquals("derp", newProps.get("chicken"));
        assertNull(newProps.get("foo.bar.deprecated"));
    }

    @Test
    public void testMultipleDeprecations() {
        Map<String, String> props = new HashMap<>();
        props.put("foo.bar.deprecated", "derp");
        props.put("foo.bar.even.more.deprecated", "very old configuration");
        Map<String, String> newProps = ConfigUtils.translateDeprecated(props, new String[][]{
            {"foo.bar", "foo.bar.deprecated", "foo.bar.even.more.deprecated"}
        });
        assertNotNull(newProps);
        assertEquals("derp", newProps.get("foo.bar"));
        assertNull(newProps.get("foo.bar.deprecated"));
        assertNull(newProps.get("foo.bar.even.more.deprecated"));
    }
}
