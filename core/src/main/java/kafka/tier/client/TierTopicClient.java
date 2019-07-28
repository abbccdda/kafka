package kafka.tier.client;

public class TierTopicClient {
    /**
     * The client id prefix. Changing this prefix or the corresponding validation in {@link #isTierTopicClient(String)}
     * may affect compatibility of existing tier topic clients.
     */
    private static final String CLIENT_ID_PREFIX = "__kafka.tiertopicmanager.";

    /**
     * Client id prefix to use for tier topic clients.
     * @param clientType The client type (consumer, producer, admin client)
     * @return Client id prefix to use
     */
    public static String clientIdPrefix(String clientType) {
        return CLIENT_ID_PREFIX + clientType;
    }

    /**
     * Check if the clientId is one used by tier topic clients.
     * @param clientId The client id to check
     * @return true if the client id is one used by tier topic clients; false otherwise
     */
    public static boolean isTierTopicClient(String clientId) {
        return clientId.startsWith(CLIENT_ID_PREFIX);
    }
}
