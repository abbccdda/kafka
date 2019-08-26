package io.confluent.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import com.google.common.collect.ImmutableMap;
import io.confluent.telemetry.ConfigPropertyTranslater.Builder;
import org.junit.Test;

public class ConfigPropertyTranslaterTest {

  @Test
  public void test() {
    ConfigPropertyTranslater translater = new Builder()
        .withTranslation("prop1.old", "prop1.new")
        .withTranslation("prop2.old", "prop2.new")
        .withTranslation("prop3.old", "prop3.new")
        .withTranslation("oldPrefix.prop3.old", "newPrefix.prop3.new")
        .withPrefixTranslation("oldPrefix.", "newPrefix.")
        .build();

    assertThat(translater.translate(ImmutableMap.<String, String>builder()
        .put("untranslated.key", "untranslated-value")
        .put("prop1.old", "prop1-value")
        .put("prop2.new", "prop2-value")
        .put("prop3.old", "prop3-value-old")
        .put("prop3.new", "prop3-value-new")
        .put("oldPrefix.prop3.old", "prop3-value")
        .put("oldPrefix.nested.value", "prefix-nested-value")
        .put("oldPrefix.value", "prefix-value")
        .build()
    )).containsOnly(
        entry("untranslated.key", "untranslated-value"),
        entry("prop1.new", "prop1-value"),
        entry("prop2.new", "prop2-value"),
        entry("prop3.new", "prop3-value-new"),
        entry("newPrefix.prop3.new", "prop3-value"),
        entry("newPrefix.value", "prefix-value"),
        entry("newPrefix.nested.value", "prefix-nested-value")
    );
  }

}