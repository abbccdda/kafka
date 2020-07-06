/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.store;

import kafka.server.KafkaConfig;
import scala.compat.java8.OptionConverters;

import java.util.Optional;

public class AzureBlockBlobTierObjectStoreConfig extends TierObjectStoreConfig {
    public Optional<String> azureCredFilePath;
    public Optional<String> endpoint;
    public String container;
    public String azureBlobPrefix;
    public int drainThreshold;

    public AzureBlockBlobTierObjectStoreConfig(Optional<String> clusterIdOpt, KafkaConfig config) {
        super(clusterIdOpt, config);
        validateConfig(config);
        azureCredFilePath = OptionConverters.toJava(config.tierAzureBlockBlobCredFilePath());
        endpoint = OptionConverters.toJava(config.tierAzureBlockBlobEndpoint());
        container = config.tierAzureBlockBlobContainer();
        azureBlobPrefix = config.tierAzureBlockBlobPrefix();
        drainThreshold = config.tierAzureBlockBlobConnectionDrainSize();
    }

    // used for testing
    AzureBlockBlobTierObjectStoreConfig(String clusterId, Integer brokerId, String prefix, String credFilePath, String container) {
        super(clusterId, brokerId);
        this.azureBlobPrefix = prefix;
        this.azureCredFilePath = Optional.ofNullable(credFilePath);
        this.container = container;
    }

    private void validateConfig(KafkaConfig config) {
        if (config.tierAzureBlockBlobCredFilePath().isEmpty() && config.tierAzureBlockBlobEndpoint().isEmpty())
            throw new IllegalArgumentException(
                    KafkaConfig.TierAzureBlockBlobCredFilePathProp() + " or " + KafkaConfig.TierAzureBlockBlobEndpointProp() +
                    " must be set if " + KafkaConfig.TierBackendProp() + " property is set to Azure."
            );

        if (config.tierAzureBlockBlobContainer() == null)
            throw new IllegalArgumentException(KafkaConfig.TierAzureBlockBlobContainerProp() +
                    " must be set if " + KafkaConfig.TierBackendProp() + " property is set to Azure.");
    }
}
