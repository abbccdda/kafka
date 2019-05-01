// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.multitenant;

import com.google.common.collect.Sets;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.server.multitenant.MultiTenantMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.StandardWatchEventKinds;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.multitenant.quota.QuotaConfig;
import io.confluent.kafka.multitenant.quota.TenantQuotaCallback;
import kafka.server.KafkaConfig$;

/**
 * This holds metadata passed from CCloud related to this physical cluster
 */
public class PhysicalClusterMetadata implements MultiTenantMetadata {

  private static final Map<String, PhysicalClusterMetadata> INSTANCES = new HashMap<>();
  private static final Logger LOG = LoggerFactory.getLogger(PhysicalClusterMetadata.class);

  static final String DATA_DIR_NAME = "..data";
  private static final String LOGICAL_CLUSTER_FILE_EXT_WITH_DOT = ".json";
  private static final Long CLOSE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);

  private String logicalClustersDir;
  // logical cluster ID --> LogicalClusterMetadata
  private final Map<String, LogicalClusterMetadata> logicalClusterMap;

  final LogicalClustersChangeListener dirWatcher;
  private final Thread dirListenerThread;
  private final ScheduledExecutorService executorService;
  private long reloadDelaysMs;
  private volatile Future<?> reloadFuture = null;
  private final ReadWriteLock cacheLock;

  public enum State {
    NOT_READY,
    RUNNING,
    CLOSED
  }
  private final AtomicReference<State> state;

  // either newly created logical clusters that we failed to load (not in logicalClusterMap) or
  // logical clusters that were updated but we failed to load the updated file
  private final Set<String> staleLogicalClusters;

  // Public for testing
  public TenantLifecycleManager tenantLifecycleManager;

  public PhysicalClusterMetadata() {
    this.state = new AtomicReference<>(State.NOT_READY);
    this.cacheLock = new ReentrantReadWriteLock();
    this.logicalClusterMap = new ConcurrentHashMap<>();
    this.staleLogicalClusters = new CopyOnWriteArraySet<>();
    this.dirWatcher = new LogicalClustersChangeListener();
    this.dirListenerThread = new Thread(this.dirWatcher, "confluent-tenants-change-listener");
    this.executorService = Executors.newSingleThreadScheduledExecutor(runnable -> {
      final Thread thread = new Thread(runnable, "physical-cluster-metadata-retry");
      thread.setDaemon(true);
      return thread;
    });
  }

  /**
   * Loads the cache and starts listening for directory events in directory specified in
   * ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG config. Adds the instance for the given
   * broker session UUID, specified in KafkaConfig.BrokerSessionUuidProp, to the static
   * instance map. The caller of this method must call close() when done to remove the instance
   * from the static map.
   * In addition to events in the directory, we are scheduling a full reload of the directory
   * every MULTITENANT_METADATA_RELOAD_DELAY_MS to make sure nothing is missed
   * @param configs broker configuration
   * @throws ConfigException if KafkaConfig.BrokerSessionUuidProp is not set, or
   *         ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG is not set.
   * @throws UnsupportedOperationException if another instance of this class with the same broker
   *         session UUID was already configured.
   */
  @Override
  public void configure(Map<String, ?> configs) {
    String instanceKey = getInstanceKey(configs);
    tenantLifecycleManager = new TenantLifecycleManager(configs);

    Object dirConfigValue = configs.get(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG);
    if (dirConfigValue == null) {
      throw new ConfigException(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG + " is not set");
    }
    this.logicalClustersDir = dirConfigValue.toString();

    Object reloadDelayValue = configs.get(ConfluentConfigs.MULTITENANT_METADATA_RELOAD_DELAY_MS_CONFIG);
    if (reloadDelayValue == null)
      this.reloadDelaysMs = ConfluentConfigs.MULTITENANT_METADATA_RELOAD_DELAY_MS_DEFAULT;
    else
      this.reloadDelaysMs = (long) reloadDelayValue;

    synchronized (INSTANCES) {
      PhysicalClusterMetadata instance = INSTANCES.get(instanceKey);
      if (instance == null) {
        INSTANCES.put(instanceKey, this);
      } else if (this != instance) {
        // we don't want to silently start a different instance with the same broker session UUID
        // to avoid strange/silent bugs, since getting an instance will always return the first
        // instance configured for this broker session UUID.
        throw new UnsupportedOperationException(
            "PhysicalClusterMetadata instance already exists for broker session " + instanceKey);
      } else {
        LOG.info("Skipping configuring this instance (broker session {}): Already configured.",
                 instanceKey);
        return;
      }
    }

    try {
      start();
    } catch (IOException ioe) {
      close(instanceKey);
      throw new ConfigException("Failed to load PhysicalClusterMetadata: " + ioe.getMessage());
    }
    LOG.warn("Configured and started instance for broker session {}", instanceKey);
  }



  // used by unit test
  void configure(String logicalClustersDir, long reloadDelaysMs) {
    this.reloadDelaysMs = reloadDelaysMs;
    this.logicalClustersDir = logicalClustersDir;
    this.tenantLifecycleManager = new TenantLifecycleManager(0, null);
  }

  @Override
  public void close(String brokerSessionUuid) {
    synchronized (INSTANCES) {
      PhysicalClusterMetadata instance = INSTANCES.get(brokerSessionUuid);
      if (instance != null && instance == this) {
        INSTANCES.remove(brokerSessionUuid);
        LOG.info("Removed instance for broker session {}", brokerSessionUuid);
      } else if (instance != null) {
        LOG.info("Closing instance that doesn't match the instance in the static map with the same"
                 + " broker session {} will not remove that instance from the map.", brokerSessionUuid);
      }
    }
    shutdown();
  }

  public static PhysicalClusterMetadata getInstance(String brokerSessionUuid) {
    synchronized (INSTANCES) {
      return INSTANCES.get(brokerSessionUuid);
    }
  }

  /**
   * Loads the cache and starts listening for directory events
   * @throws IllegalStateException if PhysicalClusterMetadata was already shut down
   * @throws IOException if failed to register watcher for metadata updates, which likely means
   *         that the logical clusters directory is inaccessible.
   */
  void start() throws IOException {
    if (State.CLOSED.equals(state.get())) {
      throw new IllegalStateException("Physical Cluster Metadata Cache already shut down.");
    }

    if (state.compareAndSet(State.NOT_READY, State.RUNNING)) {
      try {
        dirWatcher.register(logicalClustersDir);
      } catch (IOException ioe) {
        state.compareAndSet(State.RUNNING, State.NOT_READY);
        LOG.error("Failed to register watcher for dir={}", logicalClustersDir, ioe);
        dirWatcher.close();
        throw ioe;
      }
      loadAllFiles();
      reloadFuture = executorService.scheduleWithFixedDelay(
          this::reloadCache, reloadDelaysMs, reloadDelaysMs, TimeUnit.MILLISECONDS);
      LOG.info(
          "Loaded logical cluster metadata from files in dir={} (known) stale logical clusters={}",
          logicalClustersDir, staleLogicalClusters);
      dirListenerThread.start();
    }
  }

  /**
   * After this method is called, querying the cache will throw IllegalStateException
   */
  void shutdown() {
    LOG.info("Shutting down");
    if (state.getAndSet(State.CLOSED) != State.CLOSED) {
      try {
        dirListenerThread.interrupt();
        dirListenerThread.join(CLOSE_TIMEOUT_MS);
      } catch (InterruptedException ie) {
        LOG.error("Shutting down tenant metadata listener thread was interrupted", ie);
      }

      if (reloadFuture != null) {
        reloadFuture.cancel(true);
      }
      executorService.shutdownNow();
      try {
        executorService.awaitTermination(CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.debug("Shutting down was interrupted", e);
      }

      tenantLifecycleManager.close();

      LOG.info("Closed Physical Cluster Metadata Cache");
    }
  }

  /**
   * Returns true if cache is loaded and listening for metadata, otherwise returns false.
   */
  public boolean isUp() {
    return State.RUNNING.equals(state.get());
  }

  /**
   * Returns all active logical clusters with up-to-date/valid metadata hosted by this physical
   * cluster
   * @return set of logical cluster IDs
   * @throws IllegalStateException if cache is not started or already shut down
   */
  public Set<String> logicalClusterIds() {
    ensureOpen();
    return Sets.difference(logicalClusterMap.keySet(), staleLogicalClusters).immutableCopy();
  }

  /**
   * Returns all active logical clusters hosted by this physical cluster, including logical clusters
   * with stale/invalid metadata
   * @return set of logical cluster IDs
   * @throws IllegalStateException if cache is not started or already shut down
   */
  public Set<String> logicalClusterIdsIncludingStale() {
    ensureOpen();
    return Sets.union(logicalClusterMap.keySet(), staleLogicalClusters).immutableCopy();
  }


  /**
   * Returns metadata of a given logical cluster ID
   * @param logicalClusterId logical cluster ID
   * @return logical cluster metadata or null if logical cluster does not exist or its metadata
   *         is stale
   * @throws IllegalStateException if cache is not started yet or already shut down
   */
  public LogicalClusterMetadata metadata(String logicalClusterId) {
    ensureOpen();
    if (!staleLogicalClusters.contains(logicalClusterId)) {
      return logicalClusterMap.get(logicalClusterId);
    }
    return null;
  }

  /* Private methods below */

  private static String getInstanceKey(Map<String, ?> configs) {
    Object uuidConfigValue = configs.get(KafkaConfig$.MODULE$.BrokerSessionUuidProp());
    if (uuidConfigValue == null) {
      throw new ConfigException(KafkaConfig$.MODULE$.BrokerSessionUuidProp() + " is not set");
    }
    return uuidConfigValue.toString();
  }

  private void ensureOpen() {
    State curState = state.get();
    if (State.NOT_READY.equals(curState)) {
      throw new IllegalStateException("Physical Cluster Metadata Cache not started.");
    }
    if (State.CLOSED.equals(curState)) {
      throw new IllegalStateException("Physical Cluster Metadata Cache already shutdown");
    }
  }

  /**
   * Loads every file from the logical clusters directory and updates the cache. This includes
   * removing logical clusters from the cache that don't have corresponding json files anymore.
   */
  private void loadAllFiles() {
    Path logicalClustersDataDir = logicalClustersDataDir();
    if (!Files.exists(logicalClustersDataDir)) {
      LOG.info("{} does not exist.", logicalClustersDataDir);
      return;
    }

    try (Stream<Path> fileStream = Files.list(logicalClustersDataDir)) {
      final Set<String> logicalClustersInDir = new HashSet<String>();
      fileStream.forEach(filePath -> {
        String logicalClusterId = loadLogicalClusterMetadata(filePath);
        if (logicalClusterId != null) {
          logicalClustersInDir.add(logicalClusterId);
        }
      });

      // since above does not remove any entries from the cache, logicalClusterIdsIncludingStale()
      // returns all logical clusters that existed before this load/update
      Set<String> removedLogicalClusters = new HashSet<>();
      removedLogicalClusters.addAll(Sets.difference(logicalClusterIdsIncludingStale(),
              logicalClustersInDir));

      for (String removedLogicalCluster : removedLogicalClusters) {
        logicalClusterMap.remove(removedLogicalCluster);
        markUpToDate(removedLogicalCluster);
        LOG.info("Removed logical cluster {}", removedLogicalCluster);
      }

      // treat all clusters that are deactivated as if they are completely gone
      // note that the JSON is not gone immediately, so we'll keep reloading the file and then
      // removing the cluster from the cache on every iteration, and therefore we are not
      // logging here.
      for (String deactivatedCluster : tenantLifecycleManager.inactiveClusters()) {
        logicalClusterMap.remove(deactivatedCluster);
        markUpToDate(deactivatedCluster);
      }

      // delete tenants that were marked for deletion. This will wait to get a list of topics
      // then create a task and have it executed on a different thread
      tenantLifecycleManager.deleteTenants();
    } catch (IOException ioe) {
      LOG.warn("Failed to read metadata files from dir={}", logicalClustersDataDir(), ioe);
    } finally {
      // even if we fail in the middle of updating metadata, worthwhile to update quotas based on
      // current state
      updateQuotas();
    }
  }

  /**
   * Loads all the files in the logical clusters directory and updates cache if any of the files
   * has a different content. This is done under the big lock, and dir watcher updates will be
   * queued until after the cache is re-loaded. It is possible, that file create/update event
   * will be processed after the file was already loaded by this method, but it does not break
   * correctness of the cache, since the most recent content of the file will be re-loaded.
   */
  private void reloadCache() {
    // this method is called in a separate thread and the only thread that will be waiting on
    // this lock is directory watcher thread (not main thread);
    cacheLock.writeLock().lock();
    try {
      if (!State.CLOSED.equals(state.get())) {
        if (!staleLogicalClusters.isEmpty()) {
          LOG.info(
              "Re-loading cache: (known) stale logical clusters={}", staleLogicalClusters);
        }
        loadAllFiles();
      }
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  /**
   * Mark given logical cluster ID as stale, which also makes cache state stale
   * @param logicalClusterId ID of the logical cluster that failed re-fresh
   */
  private void markStale(String logicalClusterId) {
    staleLogicalClusters.add(logicalClusterId);
  }

  private void markUpToDate(String logicalClusterId) {
    staleLogicalClusters.remove(logicalClusterId);
  }

  /**
   * Called by directory watcher thread when ..data dir is created or updated. This means that at
   * last one logical cluster metadata file was added or updated. This method reloads all files
   * from the ..data dir.
   */
  private void updateLogicalClusterMetadata() {
    // big cache lock to make sure that this update does not happen in the middle of the whole
    // cache re-load
    cacheLock.readLock().lock();
    try {
      loadAllFiles();
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  /**
   * Updates logical cluster metadata from given file
   * @param lcFile file to load
   * @return logical cluster Id corresponding to this file, or null if this is not a logical
   * cluster file
   */
  private String loadLogicalClusterMetadata(Path lcFile) {
    String logicalClusterId = logicalClusterId(lcFile);
    if (logicalClusterId == null) {
      // ignore directories or files with a non-json extension
      LOG.warn("Ignoring create/update of a non-json file {}", lcFile);
      return null;
    }

    try {
      ObjectMapper objectMapper = new ObjectMapper();
      LogicalClusterMetadata lcMeta = objectMapper.readValue(
          lcFile.toFile(), LogicalClusterMetadata.class);
      if (!logicalClusterId.equals(lcMeta.logicalClusterId()) || !lcMeta.isValid()) {
        LOG.warn("Logical cluster file {} has invalid metadata {}.", lcFile, lcMeta);
        markStale(logicalClusterId);
        return logicalClusterId;
      }
      LogicalClusterMetadata oldMeta = logicalClusterMap.put(lcMeta.logicalClusterId(), lcMeta);
      markUpToDate(logicalClusterId);
      tenantLifecycleManager.updateTenantState(lcMeta);

      if (!lcMeta.equals(oldMeta)) {
        LOG.info("Added/Updated logical cluster {}", lcMeta);
      }



    } catch (Exception e) {
      LOG.error("Failed to load metadata file for logical cluster {}", logicalClusterId, e);
      markStale(logicalClusterId);
    }
    return logicalClusterId;
  }

  private static QuotaConfig quotaConfig(LogicalClusterMetadata lcMeta) {
    double multiplier = 1 + lcMeta.networkQuotaOverhead() / 100.0;
    return new QuotaConfig((long) (multiplier * lcMeta.producerByteRate()),
          (long) (multiplier * lcMeta.consumerByteRate()),
          lcMeta.brokerRequestPercentage(),
          QuotaConfig.UNLIMITED_QUOTA);
  }

  private void updateQuotas() {
    Map<String, QuotaConfig> tenantQuotas = logicalClusterMap.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> quotaConfig(e.getValue())));
    TenantQuotaCallback.updateQuotas(tenantQuotas, QuotaConfig.UNLIMITED_QUOTA);
  }

  /**
   * Returns logical cluster ID from the given file path, which is a name of the file denoted by
   * given path without logical cluster file extension. If the name of the file does not end with
   * .json extension, the method returns null.
   */
  private static String logicalClusterId(Path lcFile) {
    String fileName = lcFile.getFileName().toString();
    int indexOfDot = fileName.lastIndexOf(LOGICAL_CLUSTER_FILE_EXT_WITH_DOT);
    return indexOfDot < 0 ? null : fileName.substring(0, indexOfDot);
  }

  private Path logicalClustersDataDir() {
    return Paths.get(logicalClustersDir, DATA_DIR_NAME);
  }

  class LogicalClustersChangeListener implements Runnable {

    private WatchService watchService = null;
    private Path logicalClustersDirPath = null;

    public void register(String watchDir) throws IOException {
      watchService = FileSystems.getDefault().newWatchService();
      logicalClustersDirPath = Paths.get(watchDir);
      if (!Files.exists(logicalClustersDirPath)) {
        Files.createDirectories(logicalClustersDirPath);
      }
      logicalClustersDirPath.register(watchService,
              StandardWatchEventKinds.ENTRY_CREATE,
              StandardWatchEventKinds.ENTRY_MODIFY,
              StandardWatchEventKinds.ENTRY_DELETE,
              StandardWatchEventKinds.OVERFLOW);
    }

    public void close() {
      if (watchService != null) {
        try {
          watchService.close();
          // this is to be able to verify the watch service is closed in unit tests
          watchService = null;
          LOG.info("Closed watcher for {}", logicalClustersDir);
        } catch (IOException ioe) {
          LOG.error("Failed to shutdown watcher for {}.", logicalClustersDir, ioe);
        }
      }
    }

    // used in unit tests
    boolean isRegistered() {
      return watchService != null;
    }

    public void run() {
      LOG.info("Starting listening for changes in {}", logicalClustersDir);
      try {
        runWatcher(watchService, logicalClustersDirPath);
      } catch (InterruptedException ie) {
        LOG.warn("Watching {} was interrupted.", logicalClustersDir);
      } finally {
        close();
      }
    }

    private void runWatcher(WatchService watchService, Path watchDir) throws InterruptedException {
      boolean valid = true;
      while (valid) {
        WatchKey watchKey = watchService.take();
        for (WatchEvent<?> event: watchKey.pollEvents()) {
          LOG.debug("Got event: {} {}", event.kind(), event.context());
          @SuppressWarnings("unchecked")
          Path filename = watchDir.resolve(((WatchEvent<Path>) event).context());
          // Logical metadata files in 'watchDir' that are visible to users are symbolic links into
          // the internal data directory 'watchDir/DATA_DIR_NAME'. For example,
          //     watchDir/lkc-abc.json         -> ..data/lkc-abc.json
          //     watchDir/lkc-abc.json         -> ..data/lkc-abc.json
          // The internal data directory itself is a link to a timestamped directory with actual
          // files:
          //    watchDir/DATA_DIR_NAME          -> ..2019_02_01_15_04_05.12345678/
          // When logical cluster files get synced, a new timestamped dir is created, payload is
          // written to this new directory, a temporary symlink is created to the new dir,
          // the new symlink is renamed (atomically in most cases) to DATA_DIR_NAME
          //
          // We are watching create/update events for DATA_DIR_NAME, because this is the last
          // change in the above sequence. Also, since we are watching the directory update, we
          // are reloading all metadata files on every metadata sync (even if the sync
          // updates/creates one json file).
          if (DATA_DIR_NAME.equals(filename.getFileName().toString())) {
            if (event.kind() == StandardWatchEventKinds.ENTRY_DELETE) {
              // The rename from ..data_tmp to ..data is atomic on most env, but on windows it is
              // delete ..data, create new symlink, and then delete ..data_tmp. We don't run on
              // windows, but still seems safer not to handle ..data dir delete case. The whole
              // dir delete should normally be handled by removing physical cluster. When empty
              // secrets are synced, we still get an empty ..data directory.
              LOG.warn("Directory with logical cluster metadata is removed. Ignoring.");
            } else {
              // create or update
              updateLogicalClusterMetadata();
            }
          }
        }
        valid = watchKey.reset();
      }
      LOG.warn("Watch key no longer registered for {}. Stopped watching.", watchDir);
    }
  }
}
