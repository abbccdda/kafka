// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.metadata;

import java.io.Closeable;
import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.Configurable;

/**
 * Store containing authorization and authentication metadata. This is backed by
 * a Kafka metadata topic.
 * <p>
 * The reader for this store is started when the store is configured. {@link #configure(Map)}
 * returns only after the cache associated with this store is populated with the existing contents
 * of this store. Note that broker does not start any external listeners until this store is
 * configured and the cache is initialized, so {@link #configure(Map)} should return immediately
 * after initialization completes. The cache must be initialized to contain existing LDAP and RBAC
 * metadata to avoid unexpected authorization exceptions during broker start up. In a single-cluster
 * set up, this store must be configured to use the inter-broker listener to read the auth topic
 * since external listeners are not started until after the cache is populated.
 * </p>
 */
public interface AuthStore extends Configurable, Closeable {

  /**
   * Starts the metadata reader and returns a completion stage that is completed when
   * existing metadata from the store has been loaded into the cache. This is used by
   * embedded authorizers in brokers as well as metadata service to start up the reader
   * for this store.
   */
  CompletionStage<Void> startReader();

  /**
   * Starts the metadata coordinator and writer. This is invoked only by embedded metadata server
   * plugin and is not used by embedded authorizers in brokers that dont host a metadata server.
   *
   * @param serverUrls The URLs of metadata server hosting this store. Metadata server URLs must be
   *                  unique across the cluster since they are used as node ids for master writer
   *                  election. The URLs are also used for redirection of update requests to the
   *                  current master writer of the metadata service.
   * @return Completion stage that completes when the service starts up and elects a master writer.
   */
  CompletionStage<Void> startService(Collection<URL> serverUrls);

  /**
   * Returns a cache that stores all data read from the auth topic.
   *
   * @return cache used for authentication and/or authorization
   */
  AuthCache authCache();

  /**
   * Returns a writer instance that can be used to update this store. Returns null
   * if writer is not enabled.
   *
   * @return writer instance for updating this store, which may be null if writing is not enabled.
   */
  AuthWriter writer();

  /**
   * Returns true if this node is currently the master writer.
   */
  boolean isMasterWriter();

  /**
   * Returns the URL of the master writer node for the specified protocol. Only the master writer
   * is allowed to perform writes. Other nodes should redirect write requests to the master writer.
   *
   * @param protocol The protocol for which master writer is requested, e.g. https
   * @return URL of current master writer. May be null if writer election is in progress.
   * @throws IllegalStateException if writing is not enabled on this store
   */
  URL masterWriterUrl(String protocol);

  /**
   * Returns the broker id of the master writer. AdminClient requests to update centralized metadata
   * is sent to this broker.
   *
   * @return Broker id of current master writer. May be null if writer election is in progress.
   * @throws IllegalStateException if writing is not enabled on this store
   */
  Integer masterWriterId();

  /**
   * Returns the collection of URLS of currently active nodes.
   *
   * @param protocol The protocol for which node urls are requested, e.g. https
   * @throws IllegalStateException if metadata service was not started using
   * {@link #startService(Collection)}
   */
  Collection<URL> activeNodeUrls(String protocol);
}
