package kafka.tier.exceptions;

public class TierTopicIncorrectPartitionCountException extends Exception {
    public TierTopicIncorrectPartitionCountException(final String message) {
        super(message);
    }

    public TierTopicIncorrectPartitionCountException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public TierTopicIncorrectPartitionCountException(final Throwable throwable) {
        super(throwable);
    }
}
