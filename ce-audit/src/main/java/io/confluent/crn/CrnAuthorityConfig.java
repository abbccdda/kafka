package io.confluent.crn;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class CrnAuthorityConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String CRN_AUTHORITY_PREFIX = "confluent.authorizer.authority.";

  public static final String AUTHORITY_NAME_PROP = CRN_AUTHORITY_PREFIX + "name";
  private static final String AUTHORITY_NAME_DOC = "The DNS name of the authority that this cluster"
      + "uses to authorize. This should be a name for the cluster hosting metadata topics.";
  private static final String AUTHORITY_NAME_DEFAULT = "";

  public static final String CACHE_ENTRIES_PROP = CRN_AUTHORITY_PREFIX + "cache.entries";
  public static final int CACHE_ENTRIES_DEFAULT = 10000;
  public static final String CACHE_ENTRIES_DOC = "Number of entries in the CRN ";


  static {
    CONFIG = new ConfigDef()
        .define(AUTHORITY_NAME_PROP, Type.STRING, AUTHORITY_NAME_DEFAULT,
            Importance.HIGH, AUTHORITY_NAME_DOC)
        .define(CACHE_ENTRIES_PROP, Type.INT, CACHE_ENTRIES_DEFAULT,
            Importance.LOW, CACHE_ENTRIES_DOC);
  }

  public CrnAuthorityConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }
}
