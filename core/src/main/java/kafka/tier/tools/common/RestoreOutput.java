package kafka.tier.tools.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.tier.domain.TierPartitionForceRestore;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RestoreOutput {
    public static class ComparatorOutput {
        private static final ObjectMapper JSON_SERDE;
        static {
            JSON_SERDE = new ObjectMapper();
            JSON_SERDE.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        }

        private final Map<String, ComparatorReplicaInfo> replicas;
        private final ComparatorReplicaInfo choice;
        private final ComparatorReplicaInfo rematerialized;
        private final FenceEventInfo input;
        private TierPartitionForceRestore restore = null;

        public ComparatorOutput(@JsonProperty("replicas") final Map<String, ComparatorReplicaInfo> replicas,
                                @JsonProperty("rematerialized") final ComparatorReplicaInfo rematerialized,
                                @JsonProperty("choice") final ComparatorReplicaInfo choice,
                                @JsonProperty("input") final FenceEventInfo input) {
            this.replicas = replicas;
            this.choice = choice;
            this.input = input;
            this.rematerialized = rematerialized;
        }

        @JsonProperty(value = "choice", required = true)
        public ComparatorReplicaInfo choice() {
            return choice;
        }

        @JsonProperty(value = "replicas", required = true)
        public Map<String, ComparatorReplicaInfo> replicas() {
            return replicas;
        }

        @JsonProperty(value = "input", required = true)
        public FenceEventInfo input() {
            return input;
        }

        @JsonProperty(value = "rematerialized", required = true)
        public ComparatorReplicaInfo rematerialized() {
            return rematerialized;
        }

        public void setTierPartitionForceRestore(TierPartitionForceRestore restore) {
            this.restore = restore;
        }

        @JsonProperty(value = "restored", required = false)
        public String restored() {
            return restore.toString();
        }

        @Override
        public String toString() {
            return "ComparatorOutput{" +
                    "replicas=" + replicas +
                    ", choice='" + choice + '\'' +
                    ", input='" + input + '\'' +
                    ", rematerialized=" + rematerialized +
                    ", restore=" + Optional.ofNullable(restore) +
                    '}';
        }

        public static void writeJsonToFile(List<ComparatorOutput> outputList, OutputStream outputStream) throws IOException {
            JSON_SERDE.writeValue(outputStream, outputList);
        }

        public static List<ComparatorOutput> readJsonFromFile(Path inputJsonFile) throws IOException {
            return JSON_SERDE.readValue(inputJsonFile.toFile(),
                    new TypeReference<List<ComparatorOutput>>() {
                    });
        }
    }

    public static class ComparatorReplicaInfo {
        private final String replica;
        private final Path path;
        public final String header;
        private final boolean validationSuccess;

        public ComparatorReplicaInfo(@JsonProperty("replica") final String replica,
                                     @JsonProperty("path") final String path,
                                     @JsonProperty("header") final String header,
                                     @JsonProperty("validationSuccess") final boolean validationSuccess) {
            this.replica = replica;
            this.path = Paths.get(path);
            this.header = header;
            this.validationSuccess = validationSuccess;
        }
        public Path path() {
            return path;
        }

        public String replica() {
            return replica;
        }

        public boolean validationSuccess() {
            return validationSuccess;
        }

        public String header() {
            return header;
        }

        @Override
        public String toString() {
            return "ComparatorReplicaInfo{" +
                    "header=" + header +
                    ", replica='" + replica + "'" +
                    ", path=" + path +
                    ", validationSuccess=" + validationSuccess +
                    '}';
        }
    }
}
