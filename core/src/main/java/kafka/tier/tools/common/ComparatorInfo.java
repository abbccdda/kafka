/*
 Copyright 2020 Confluent Inc.
 */

package kafka.tier.tools.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.tier.TopicIdPartition;
import kafka.tier.state.Header;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class will act as a placeholder class for all the DAOs that will be used by TierMetadataComparator tool
 * All of the inner classes follow the specific pattern in terms of accessors:
 * 1. package-private constructors to ensure protected creation
 * 2. Public getters to reflect which fields should be emitted by Jackson mapper.
 *    Please note that public getters should only provide serializable data types.
 * 3. package-private getters for use within the Comparator and similar other recovery tools
 */
public class ComparatorInfo {
    // This is the special replicaId for the output generated from validating the rematerialized tierstate files
    public static final String REMATERIALIZED_REPLICA_ID = "rematerialized";

    private static final ObjectMapper JSON_SERDE;

    static {
        JSON_SERDE = new ObjectMapper();
        JSON_SERDE.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    }

    private ComparatorInfo() {
    }

    /**
     * This class represents a single output from the Comparator tool
     * For every fenced topic partition details, we would generate one such distinct output object
     */
    public static class ComparatorOutput {
        private final Map<String, ComparatorReplicaInfo> replicas;
        private final ComparatorReplicaInfo choice;
        private final FenceEventInfo input;

        public String toJson() {
            try {
                return JSON_SERDE.writeValueAsString(this);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String toString() {
            return "ComparatorOutput{" +
                    "replicas=" + replicas +
                    ", choice=" + choice +
                    ", input=" + input +
                    '}';
        }

        public ComparatorOutput(final Map<String, ComparatorReplicaInfo> replicas, final ComparatorReplicaInfo choice,
                         final FenceEventInfo input) {
            this.replicas = replicas;
            this.choice = choice;
            this.input = input;
        }

        public FenceEventInfo getInput() {
            return input;
        }

        public Map<String, ComparatorReplicaInfo> getReplicas() {
            final HashMap<String, ComparatorReplicaInfo> copiedInfoMap = new HashMap<>(replicas);
            copiedInfoMap.remove(REMATERIALIZED_REPLICA_ID);
            return copiedInfoMap;
        }

        public ComparatorReplicaInfo getChoice() {
            return choice;
        }

        public ComparatorReplicaInfo getRematerialized() {
            return replicas.get(REMATERIALIZED_REPLICA_ID);
        }

        public static void writeJsonToFile(List<ComparatorOutput> outputList, Path outputJsonFile) throws IOException {
            JSON_SERDE.writeValue(outputJsonFile.toFile(), outputList);
        }

        public static List<ComparatorOutput> readJsonFromFile(Path inputJsonFile) throws IOException {
            return JSON_SERDE.readValue(inputJsonFile.toFile(),
                    new TypeReference<List<ComparatorOutput>>() {
                    });
        }
    }

    /**
     * This class represents the replica info which is the core unit of the comparator tool
     * For every n fenced topic details and m bootstrap server, there should be n*(m+1) replicas
     * The +1 is because we would also generate one replica directly from rematerialized events
     */
    public static class ComparatorReplicaInfo {
        private final String replicaId;
        private final Path tierStateFile;
        private final TopicIdPartition topicIdPartition;
        // exposed for tests
        public Header header;
        private boolean validationSuccess;
        private Exception exception;

        public ComparatorReplicaInfo(final String replicaId, final Path tierStateFile, final TopicIdPartition topicIdPartition) {
            this.replicaId = replicaId;
            this.tierStateFile = tierStateFile;
            this.topicIdPartition = topicIdPartition;
        }

        public String getHeader() {
            if (header == null) {
                return "";
            }
            return header.toString();
        }

        public long lastOffset() {
            if (header == null) {
                return -1L;
            }
            return header.localMaterializedOffsetAndEpoch().offset();
        }

        public TopicIdPartition topicIdPartition() {
            return topicIdPartition;
        }

        public Path tierStateFile() {
            return tierStateFile;
        }

        public void setHeader(final Header header) {
            this.header = header;
        }

        public String getReplica() {
            return replicaId;
        }

        public String getPath() {
            return tierStateFile.toString();
        }

        public boolean isValidationSuccess() {
            return validationSuccess;
        }

        public void setValidationSuccess(final boolean validationSuccess) {
            this.validationSuccess = validationSuccess;
        }

        public void setException(final Exception exception) {
            this.exception = exception;
        }

        @Override
        public String toString() {
            return "ComparatorReplicaInfo{" +
                    "header=" + header +
                    ", replicaId='" + replicaId + '\'' +
                    ", tierStateFile=" + tierStateFile +
                    ", topicIdPartition=" + topicIdPartition +
                    ", validationSuccess=" + validationSuccess +
                    ", exception=" + exception +
                    '}';
        }

        public String toJson() {
            try {
                return JSON_SERDE.writeValueAsString(this);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
