/*
 Copyright 2020 Confluent Inc.
 */

package kafka.tier.tools;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import kafka.server.Defaults;
import kafka.server.KafkaConfig;
import kafka.tier.store.TierObjectStore;

import java.util.Properties;

/**
 * This class should contain all the helper methods necessary to create and connect to the cloud backend tier object store
 * The visibility of this class is package-private because we don't currently envision the usage beyond the tier validation tools
 */
class TierCloudBackendUtils {


    /**
     * This method can be used to augment the option maybeExistingParser with validator
     * tool opts
     * In case no maybeExistingParser is passed, this method will create a new parser instance
     *
     * @param maybeExistingParser the existing maybeExistingParser to which the options would be added
     * @return the updated maybeExistingParser instance capable of parsing the backend options
     */
    static OptionParser augmentParserWithValidatorOpts(OptionParser maybeExistingParser) {
        if (maybeExistingParser == null) {
            maybeExistingParser = new OptionParser();
        }
        // The following args are specific to the backend tier store verification
        maybeExistingParser.accepts(TierTopicMaterializationToolConfig.TIER_STORAGE_VALIDATION,
                TierTopicMaterializationToolConfig.TIER_STORAGE_VALIDATION_DOC)
                .withRequiredArg()
                .describedAs(TierTopicMaterializationToolConfig.TIER_STORAGE_VALIDATION_DOC)
                .ofType(Boolean.class)
                .defaultsTo(true);
        maybeExistingParser.accepts(TierTopicMaterializationToolConfig.TIER_STORAGE_OFFSET_VALIDATION,
                TierTopicMaterializationToolConfig.TIER_STORAGE_OFFSET_VALIDATION_DOC)
                .withRequiredArg()
                .describedAs(TierTopicMaterializationToolConfig.TIER_STORAGE_OFFSET_VALIDATION_DOC)
                .ofType(Boolean.class)
                .defaultsTo(false);
        maybeExistingParser.accepts(TierTopicMaterializationToolConfig.CLUSTER_ID,
                TierTopicMaterializationToolConfig.CLUSTER_ID_DOC)
                .withRequiredArg()
                .describedAs(TierTopicMaterializationToolConfig.CLUSTER_ID_DOC)
                .ofType(String.class)
                .defaultsTo("kafka");
        maybeExistingParser.accepts(KafkaConfig.BrokerIdProp(), KafkaConfig.BrokerIdDoc())
                .withRequiredArg()
                .describedAs(KafkaConfig.BrokerIdDoc())
                .ofType(Integer.class);

        return maybeExistingParser;
    }

    /**
     * This method can be used to augment the option maybeExistingParser with the S3 specific backend options
     * In case no maybeExistingParser is passed, this method will create a new parer instance
     *
     * @param maybeExistingParser the existing maybeExistingParser to which the options would be added
     * @return the updated maybeExistingParser instance capable of parsing the backend options
     */
    static OptionParser augmentParserWithTierBackendOpts(OptionParser maybeExistingParser) {
        if (maybeExistingParser == null) {
            maybeExistingParser = new OptionParser();
        }

        // For now, we are defaulting to S3 as the backend
        maybeExistingParser.accepts(KafkaConfig.TierBackendProp(), KafkaConfig.TierBackendDoc())
                .withRequiredArg()
                .describedAs(KafkaConfig.TierBackendDoc())
                .ofType(TierObjectStore.Backend.class)
                .defaultsTo(TierObjectStore.Backend.S3);

        // The following options are specific to the S3 Backend
        maybeExistingParser.accepts(KafkaConfig.TierS3BucketProp(), KafkaConfig.TierS3BucketDoc())
                .withRequiredArg()
                .describedAs(KafkaConfig.TierS3BucketDoc())
                .ofType(String.class);
        maybeExistingParser.accepts(KafkaConfig.TierS3RegionProp(), KafkaConfig.TierS3RegionDoc())
                .withRequiredArg()
                .describedAs(KafkaConfig.TierS3RegionDoc())
                .ofType(String.class)
                .defaultsTo("us-west-2");
        maybeExistingParser.accepts(KafkaConfig.TierS3CredFilePathProp(), KafkaConfig.TierS3CredFilePathDoc())
                .withRequiredArg()
                .describedAs(KafkaConfig.TierS3CredFilePathDoc())
                .ofType(String.class);
        maybeExistingParser.accepts(KafkaConfig.TierS3EndpointOverrideProp(), KafkaConfig.TierS3EndpointOverrideDoc())
                .withRequiredArg()
                .describedAs(KafkaConfig.TierS3EndpointOverrideDoc())
                .ofType(String.class);
        maybeExistingParser.accepts(KafkaConfig.TierS3SignerOverrideProp(), KafkaConfig.TierS3SignerOverrideDoc())
                .withRequiredArg()
                .describedAs(KafkaConfig.TierS3SignerOverrideDoc())
                .ofType(String.class);
        maybeExistingParser.accepts(KafkaConfig.TierS3SseAlgorithmProp(), KafkaConfig.TierS3SseAlgorithmDoc())
                .withRequiredArg()
                .describedAs(KafkaConfig.TierS3SseAlgorithmDoc())
                .ofType(String.class)
                .defaultsTo(Defaults.TierS3SseAlgorithm());
        maybeExistingParser.accepts(KafkaConfig.TierS3AutoAbortThresholdBytesProp(), KafkaConfig.TierS3AutoAbortThresholdBytesDoc())
                .withRequiredArg()
                .describedAs(KafkaConfig.TierS3AutoAbortThresholdBytesDoc())
                .ofType(Integer.class)
                .defaultsTo(Defaults.TierS3AutoAbortThresholdBytes());
        maybeExistingParser.accepts(KafkaConfig.TierS3PrefixProp(), KafkaConfig.TierS3PrefixDoc())
                .withRequiredArg()
                .describedAs(KafkaConfig.TierS3PrefixDoc())
                .ofType(String.class)
                .defaultsTo(Defaults.TierS3Prefix());

        return maybeExistingParser;
    }

    /**
     * This method should be used in conjunction with TierCloudBackendUtils.augmentParserWithCloudBackendOpts for obtaining the
     * optionSet. This method can extract the S3 specific options and populate them in the properties provided.
     *
     * @param options            OptionSet that is augmented with backend specific properties
     * @param maybeExistingProps the Properties map that will be populated with the options
     * @return Updated properties map
     */
    static Properties addValidatorProps(OptionSet options, Properties maybeExistingProps) {
        if (options == null)
            throw new IllegalArgumentException("Options need to be instantiated. See "
                    + "TierCloudBackendUtils.augmentParserValidatorOpts.");
        if (maybeExistingProps == null) {
            maybeExistingProps = new Properties();
        }
        // The following are necessary for tier object store validation
        maybeExistingProps.put(TierTopicMaterializationToolConfig.TIER_STORAGE_VALIDATION, options.valueOf(TierTopicMaterializationToolConfig.TIER_STORAGE_VALIDATION));
        maybeExistingProps.put(TierTopicMaterializationToolConfig.TIER_STORAGE_OFFSET_VALIDATION, options.valueOf(TierTopicMaterializationToolConfig.TIER_STORAGE_OFFSET_VALIDATION));
        maybeExistingProps.put(TierTopicMaterializationToolConfig.CLUSTER_ID, options.valueOf(TierTopicMaterializationToolConfig.CLUSTER_ID));
        if (options.has(KafkaConfig.BrokerIdProp())) maybeExistingProps.put(KafkaConfig.BrokerIdProp(), options.valueOf(KafkaConfig.BrokerIdProp()));

        return maybeExistingProps;
    }

    /**
     * This method should be used in conjunction with TierCloudBackendUtils.augmentParserWithCloudBackendOpts for obtaining the
     * optionSet. This method can extract the S3 specific options and populate them in the properties provided.
     *
     * @param options            OptionSet that is augmented with backend specific properties
     * @param maybeExistingProps the Properties map that will be populated with the options
     * @return Updated properties map
     */
    static Properties addTierBackendProps(OptionSet options, Properties maybeExistingProps) {
        if (options == null)
            throw new IllegalArgumentException("Options need to be instantiated. See "
                    + "TierCloudBackendUtils.augmentParserWithTierBackendOpts!");
        if (maybeExistingProps == null) {
            maybeExistingProps = new Properties();
        }
        // The following are necessary for tier object store validation
        maybeExistingProps.put(KafkaConfig.TierBackendProp(), options.valueOf(KafkaConfig.TierBackendProp()));
        // The following are S3 specific properties
        if (options.has(KafkaConfig.TierS3BucketProp()))
            maybeExistingProps.put(KafkaConfig.TierS3BucketProp(), options.valueOf(KafkaConfig.TierS3BucketProp()));
        maybeExistingProps.put(KafkaConfig.TierS3RegionProp(), options.valueOf(KafkaConfig.TierS3RegionProp()));
        if (options.has(KafkaConfig.TierS3CredFilePathProp()))
            maybeExistingProps.put(KafkaConfig.TierS3CredFilePathProp(), options.valueOf(KafkaConfig.TierS3CredFilePathProp()));
        if (options.has(KafkaConfig.TierS3EndpointOverrideProp()))
            maybeExistingProps.put(KafkaConfig.TierS3EndpointOverrideProp(), options.valueOf(KafkaConfig.TierS3EndpointOverrideProp()));
        if (options.has(KafkaConfig.TierS3SignerOverrideProp()))
            maybeExistingProps.put(KafkaConfig.TierS3SignerOverrideProp(), options.valueOf(KafkaConfig.TierS3SignerOverrideProp()));
        maybeExistingProps.put(KafkaConfig.TierS3SseAlgorithmProp(), options.valueOf(KafkaConfig.TierS3SseAlgorithmProp()));
        maybeExistingProps.put(KafkaConfig.TierS3AutoAbortThresholdBytesProp(), options.valueOf(KafkaConfig.TierS3AutoAbortThresholdBytesProp()));
        maybeExistingProps.put(KafkaConfig.TierS3PrefixProp(), options.valueOf(KafkaConfig.TierS3PrefixProp()));

        return maybeExistingProps;
    }
}
