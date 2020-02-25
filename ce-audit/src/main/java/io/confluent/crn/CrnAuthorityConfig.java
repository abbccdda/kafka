package io.confluent.crn;

import static org.apache.kafka.common.config.internals.ConfluentConfigs.CRN_AUTHORITY_NAME_DEFAULT;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.CRN_AUTHORITY_NAME_DOC;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.CRN_AUTHORITY_NAME_CONFIG;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.CRN_AUTHORITY_PREFIX;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class CrnAuthorityConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String CACHE_ENTRIES_CONFIG = CRN_AUTHORITY_PREFIX + "cache.entries";
  public static final int CACHE_ENTRIES_DEFAULT = 10000;
  public static final String CACHE_ENTRIES_DOC = "Number of entries in the CRN ";


  static {
    CONFIG = new ConfigDef()
        .define(CRN_AUTHORITY_NAME_CONFIG, Type.STRING, CRN_AUTHORITY_NAME_DEFAULT,
            Importance.HIGH, CRN_AUTHORITY_NAME_DOC)
        .define(CACHE_ENTRIES_CONFIG, Type.INT, CACHE_ENTRIES_DEFAULT,
            Importance.LOW, CACHE_ENTRIES_DOC);
  }

  public CrnAuthorityConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }
}
