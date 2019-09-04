package io.confluent.telemetry.exporter.file;

import io.confluent.telemetry.exporter.Exporter;
import io.opencensus.proto.metrics.v1.Metric;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File exporter indended for debugging purposes only.
 *
 * <b>This class is not threadsafe</b>
 */
public class FileExporter implements Exporter {

    private static final Logger log = LoggerFactory.getLogger(FileExporter.class);

    private final Path directory;

    private final Deserializer<String> deserializer;

    private int count = 0;

    private FileExporter(Builder builder) {
        directory = builder.directory;
        deserializer = builder.deserializer;
    }

    @Override
    public void export(Collection<Metric> metrics) throws IOException {
        if (!directory.toFile().mkdirs()) {
            throw new IOException("Unable to create directory: " + directory.toAbsolutePath());
        }

        Path file = directory.resolve(Integer.toString(count++));

        try (PrintWriter printWriter = new PrintWriter(Files.newBufferedWriter(file))) {
          for (Metric m : metrics) {
              printWriter.println(this.deserializer.deserialize(null, m.toByteArray()));
          }
        }
    }

    @Override
    public void close() throws Exception {
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @SuppressWarnings("unchecked")
    public static Builder newBuilder(FileExporterConfig config) {
        return new Builder()
            .withDeserializer(config.getConfiguredInstance(FileExporterConfig.DESERIALIZER_CONFIG, Deserializer.class))
            .withDirectory(Paths.get(config.getString(FileExporterConfig.DIR_CONFIG)));
    }

    public static final class Builder {
        private Path directory;
        private Deserializer<String> deserializer;

        private Builder() {
        }

        public Builder withDirectory(Path directory) {
            this.directory = directory;
            return this;
        }

        public Builder withDeserializer(Deserializer<String> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public FileExporter build() {
            return new FileExporter(this);
        }
    }
}
