/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tools;

import kafka.tier.TopicIdPartition;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.TierSegmentDeleteInitiate;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparsers;

/**
 * A tool that injects a segment delete initiate event (generated at an epoch value of int32
 * maximum) into TierTopic. This is helpful in triggering fencing of broker's TierPartitionState
 * materializer.
 */
public class TierPartitionStateFencingTrigger {

    public static final String DANGEROUS_FENCE_VIA_DELETE_EVENT_CMD = "dangerous-fence-via-delete-event";
    public static final String DANGEROUS_FENCE_VIA_DELETE_EVENT_DOC =
        "Triggers fencing by injecting a TierSegmentDeleteInitiate event with epoch set to int32 maximum: 2147483647.";

    public static final String SELECTED_COMMAND = "selected_command";

    // Create the CLI argument parser.
    private static ArgumentParser createArgParser() {
        ArgumentParser rootParser = ArgumentParsers
            .newArgumentParser(TierPartitionStateFencingTrigger.class.getName())
            .defaultHelp(true)
            .description("Provides commands to fence TierTopic in certain ways.");
        RecoveryUtils.populateCommonCLIOptions(rootParser);

        Subparsers subparsers =
            rootParser.addSubparsers()
                .title("Valid subcommands")
                .dest(TierPartitionStateFencingTrigger.SELECTED_COMMAND);
        subparsers.addParser(TierPartitionStateFencingTrigger.DANGEROUS_FENCE_VIA_DELETE_EVENT_CMD)
            .help(TierPartitionStateFencingTrigger.DANGEROUS_FENCE_VIA_DELETE_EVENT_DOC);
        return rootParser;
    }

    // Main entry point for the CLI tool. This picks the sub-command logic to run. This may be
    // augmented in the future, with more sub-commands (if needed).
    private static void run(Namespace args) throws InterruptedException, ExecutionException {
        String selectedCommand = args.getString(TierPartitionStateFencingTrigger.SELECTED_COMMAND);
        switch (selectedCommand) {
            case DANGEROUS_FENCE_VIA_DELETE_EVENT_CMD:
                String tierTopicNamespace =
                    args.getString(RecoveryUtils.CommonCLIOptions.TIER_TOPIC_NAMESPACE_CONFIG);
                // When injected, this event is expected to cause fencing, because of the very
                // high epoch value that inject into the event.
                AbstractTierMetadata eventThatTriggersFencing =
                    getDeleteInitiateEvent(args, Integer.MAX_VALUE);
                String bootstrapServers =
                    args.getString(RecoveryUtils.CommonCLIOptions.BOOTSTRAP_SERVERS_CONFIG);
                RecoveryUtils.injectTierTopicEvent(eventThatTriggersFencing, bootstrapServers,
                    tierTopicNamespace, "unknown");
                break;
            default:
                throw new IllegalStateException("Unsupported command: " + selectedCommand);
        }
    }

    /**
     * Generates a TierSegmentDeleteInitiate event, with the epoch value set to provided value. The
     * partition number, topic name and the topic ID are extracted from the provided args.
     *
     * @param args  the CLI args useful to extract few required attributes
     * @param epoch the epoch to be injected into the generated TierSegmentDeleteInitiate event
     * @return the generated TierSegmentDeleteInitiate event
     */
    private static TierSegmentDeleteInitiate getDeleteInitiateEvent(Namespace args, int epoch) {
        String tieredPartitionTopicName =
            args.getString(RecoveryUtils.CommonCLIOptions.TIERED_PARTITION_TOPIC_NAME_CONFIG);
        UUID tieredPartitionTopicId =
            UUID.fromString(
                args.getString(RecoveryUtils.CommonCLIOptions.TIERED_PARTITION_TOPIC_ID_CONFIG));
        int tieredPartitionInt =
            args.getInt(RecoveryUtils.CommonCLIOptions.TIERED_PARTITION_NAME_CONFIG);
        TopicIdPartition tieredPartition = new TopicIdPartition(
            tieredPartitionTopicName, tieredPartitionTopicId, tieredPartitionInt);
        return new TierSegmentDeleteInitiate(
            tieredPartition, epoch, UUID.randomUUID());
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = TierPartitionStateFencingTrigger.createArgParser();
        Namespace parsedArgs = null;
        try {
            parsedArgs = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

        run(parsedArgs);
    }
}
