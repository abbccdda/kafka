// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.metrics.reporter;

import com.google.common.base.MoreObjects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class VolumeMetricsProvider {

  private static final Logger log = LoggerFactory.getLogger(VolumeMetricsProvider.class);

  public static class VolumeInfo {

    private final String name;
    private final long usableBytes;
    private final long totalBytes;
    private final Set<String> logDirs;

    private VolumeInfo(
        FileStore fileStore,
        Set<String> logDirs
    ) throws IOException {
      this.name = fileStore.name();
      this.usableBytes = fileStore.getUsableSpace();
      this.totalBytes = fileStore.getTotalSpace();
      this.logDirs = Collections.unmodifiableSet(logDirs);
    }

    public String name() {
      return name;
    }

    public long usableBytes() {
      return usableBytes;
    }

    public long totalBytes() {
      return totalBytes;
    }

    public Collection<String> logDirs() {
      return this.logDirs;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("name", name)
          .add("usableBytes", usableBytes)
          .add("totalBytes", totalBytes)
          .add("logDirs", logDirs)
          .toString();
    }
  }

  /**
   * The minimum time in milliseconds that we will go in between updates.
   */
  private final long updatePeriodMs;

  /**
   * The log directories to fetch information about.
   */
  private final String[] logDirs;

  /**
   * The last update time in monotonic nanoseconds.
   */
  private long lastUpdateNs;

  /**
   * The cached metrics.
   */
  private Map<String, VolumeInfo> cachedMetrics = null;

  /**
   * Create a volume metrics provider.
   *
   * @param updatePeriodMs The minimum period to update at, in milliseconds.
   * @param logDirs        The log directories to fetch metrics for.  These should be absolute.
   */
  public VolumeMetricsProvider(
      long updatePeriodMs,
      String[] logDirs
  ) {
    this.updatePeriodMs = updatePeriodMs;
    this.logDirs = logDirs;
  }

  public Map<String, VolumeInfo> getMetrics() {
    long curTimeNs = System.nanoTime();
    long deltaNs = curTimeNs - lastUpdateNs;
    long deltaMs = TimeUnit.MILLISECONDS.convert(deltaNs, TimeUnit.NANOSECONDS);
    if (deltaMs >= updatePeriodMs) {
      cachedMetrics = null;
    }
    if (cachedMetrics == null) {
      lastUpdateNs = curTimeNs;
      cachedMetrics = refreshCachedMetrics();
    }
    return Collections.unmodifiableMap(cachedMetrics);
  }

  private Map<String, VolumeInfo> refreshCachedMetrics() {
    Map<String, FileStore> fileStoreNameToObject = new HashMap<>();
    Map<String, Set<String>> fileStoreNameToLogDirs = new HashMap<>();
    for (String logDir : logDirs) {
      try {
        FileStore fileStore = pathToFileStore(logDir);
        fileStoreNameToObject.put(fileStore.name(), fileStore);
        if (!fileStoreNameToLogDirs.containsKey(fileStore.name())) {
          fileStoreNameToLogDirs.put(fileStore.name(), new TreeSet<String>());
        }
        fileStoreNameToLogDirs.get(fileStore.name()).add(logDir);
      } catch (IOException e) {
        log.error("Failed to resolve path to FileStore", e);
      }
    }
    Map<String, VolumeInfo> metrics = new TreeMap<>();
    for (FileStore fileStore : fileStoreNameToObject.values()) {
      try {
        VolumeInfo volumeInfo = new VolumeInfo(fileStore,
                                               fileStoreNameToLogDirs.get(fileStore.name()));
        if (log.isDebugEnabled()) {
          log.debug("Read {}", volumeInfo.toString());
        }
        metrics.put(volumeInfo.name(), volumeInfo);
      } catch (RuntimeException | IOException e) {
        log.error("Failed to retrieve VolumeInfo from FileStore", e);
      }
    }
    return metrics;
  }

  private FileStore pathToFileStore(String path) throws IOException {
    Path pathObj = Paths.get(path);
    return Files.getFileStore(pathObj);
  }
}
