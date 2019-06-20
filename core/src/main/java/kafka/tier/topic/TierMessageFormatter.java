package kafka.tier.topic;

import java.io.PrintStream;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import kafka.common.MessageFormatter;
import kafka.tier.domain.AbstractTierMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;

// See README.md in kafka.tier.topic for usage notes
public class TierMessageFormatter implements MessageFormatter {

    @Override
    public void init(Properties props) {
    }

    @Override
    public void writeTo(ConsumerRecord<byte[], byte[]> consumerRecord, PrintStream output) {
        try {
            final Optional<AbstractTierMetadata> entry = AbstractTierMetadata
                    .deserialize(consumerRecord.key(), consumerRecord.value());
            if (entry.isPresent()) {
                output.printf("(%d, %d, %s): %s\n",
                        consumerRecord.partition(),
                        consumerRecord.offset(),
                        Instant.ofEpochMilli(consumerRecord.timestamp()),
                        entry.get());
            } else {
                output.printf("(%d, %d, %s): unknown tier metadata type %d\n",
                        consumerRecord.partition(),
                        consumerRecord.offset(),
                        Instant.ofEpochMilli(consumerRecord.timestamp()),
                        AbstractTierMetadata.getTypeId(consumerRecord.value()));
            }
        } catch (Exception ex) {
            output.printf("(%d, %d, %s): failed to deserialize tier metadata. Error message: %s. Record: %s\n",
                    consumerRecord.partition(),
                    consumerRecord.offset(),
                    Instant.ofEpochMilli(consumerRecord.timestamp()),
                    ex.getMessage(),
                    consumerRecord.toString());
        }
    }

    @Override
    public void close() {
    }
}
