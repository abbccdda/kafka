package io.confluent.telemetry.exporter.file;

import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.serde.ProtoToFlatJson;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class FileExporterConfig extends AbstractConfig {

  public static final String PREFIX = ConfluentTelemetryConfig.PREFIX_EXPORTER + "file.";

  public static final String DIR_CONFIG = PREFIX + "dir";
  public static final String DIR_DOC = "Directory to which telemetry records will be written.";

  public static final String DESERIALIZER_CONFIG = PREFIX + "deserializer";
  public static final String DESERIALIZER_DOC = "Deserializer that converts OpenCensus protobuf binary format to String.";
  public static final Class<?> DESERIALIZER_DEFAULT = ProtoToFlatJson.class;

  private static final ConfigDef CONFIG = new ConfigDef()
      .define(
          DIR_CONFIG,
          ConfigDef.Type.STRING,
          ConfigDef.Importance.MEDIUM,
          DIR_DOC
      ).define(
          DESERIALIZER_CONFIG,
          ConfigDef.Type.CLASS,
          DESERIALIZER_DEFAULT,
          ConfigDef.Importance.LOW,
          DESERIALIZER_DOC
      );

  public FileExporterConfig(Map<String, ?> originals) {
    super(CONFIG, originals);
  }

  public static void main(String[] args) {
        System.out.println(CONFIG.toRst());
    }

}
