/*
 * Copyright [2018  - 2018] Confluent Inc.
 */

package io.confluent.license;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A manager that schedules the license checks for a set of {@link LicenseManager} instances.
 * The preferred approach is to use {@link LicenseManager#start()} and
 * {@link LicenseManager#stop()}, which both use a {@link #INSTANCE single shared instance}
 * of this class.
 */
final class LicenseManagers {

  public static final LicenseManagers INSTANCE = new LicenseManagers(
      Executors::newSingleThreadScheduledExecutor
  );

  private static final Logger log = LoggerFactory.getLogger(LicenseManagers.class);

  private final AtomicReference<ScheduledExecutorService> executor = new AtomicReference<>(null);
  private final Map<LicenseManager, RunningLicenseManager> registered = new HashMap<>();
  private final Supplier<ScheduledExecutorService> execServiceFactory;

  // Visible for testing
  LicenseManagers(Supplier<ScheduledExecutorService> execServiceFactory) {
    Objects.requireNonNull(execServiceFactory);
    this.execServiceFactory = execServiceFactory;
  }

  /**
   * Begin running the license manager's check on a daily schedule, or more frequently if the
   * license is expired or has less than 1 day remaining. The first check will be scheduled for
   * 24 hours after this method is called, since the calling application probably explicitly
   * checked the license before this method is called.
   *
   * <p>Use this method if the LicenseManagers instance is not {@link #INSTANCE shared};
   * otherwise, use {@link LicenseManager#start()} instead.
   *
   * @param licenseManager the license manager that is to be run; may not be null
   * @return the future that can be used to cancel the checks and {@link LicenseManager#stop()}
   *         the license manager
   * @see LicenseManager#start()
   */
  synchronized ScheduledFuture<?> start(LicenseManager licenseManager) {
    Objects.nonNull(licenseManager);
    executor.updateAndGet(existing -> existing != null
                                      ? existing
                                      : execServiceFactory.get());
    RunningLicenseManager running = registered.get(licenseManager);
    if (running == null) {
      running = new RunningLicenseManager(licenseManager, executor.get(), this::cancelled);
      registered.put(licenseManager, running);
      licenseManager.doStart();
    }
    return running;
  }

  /**
   * Stop running the license manager's checks and {@link LicenseManager#stop()} the license
   * manager so it can close all resources.
   *
   * <p>Use this method if the LicenseManagers instance is not {@link #INSTANCE shared};
   * otherwise, use {@link LicenseManager#stop()} instead.
   *
   * @param licenseManager the license manager that is to be stopped; may not be null
   * @return true if the license manager was stopped, or false otherwise
   * @see LicenseManager#stop()
   */
  synchronized boolean stop(LicenseManager licenseManager) {
    boolean stopped = false;
    if (licenseManager != null) {
      RunningLicenseManager running = registered.remove(licenseManager);
      if (running != null) {
        running.cancel(true);
        stopped = true;
      }
    }
    if (registered.isEmpty()) {
      shutdown();
    }
    return stopped;
  }

  /**
   * Shutdown this license manager and stop all {@link LicenseManager} instances that have
   * been {@link #start(LicenseManager) started} but not already
   * {@link #stop(LicenseManager) stopped}.
   *
   * <p>This method does nothing if there are no registered {@link LicenseManager} instances
   * at that time.
   */
  synchronized void shutdown() {
    if (executor.get() != null) {
      // Stop each of the license managers, and the last one will shutdown the executor
      for (RunningLicenseManager running : new ArrayList<>(registered.values())) {
        stop(running.licenseManager);
      }
      registered.clear();
      // Ensure the executor service is properly shut down
      ScheduledExecutorService service = executor.getAndSet(null);
      if (service != null) {
        service.shutdownNow();
      }
    }
  }

  protected void cancelled(LicenseManager licenseManager) {
    if (licenseManager != null) {
      RunningLicenseManager runningLicenseManager = registered.remove(licenseManager);
      if (runningLicenseManager != null) {
        runningLicenseManager.executor.shutdownNow();
      }
    }
  }

  protected boolean isRunning() {
    return executor.get() != null;
  }

  protected static class RunningLicenseManager implements ScheduledFuture<Object> {
    private final ScheduledExecutorService executor;
    private final Consumer<LicenseManager> cancelListener;
    final LicenseManager licenseManager;
    final ScheduledFuture<?> future;
    private boolean loggedError = false;

    RunningLicenseManager(
        LicenseManager mgr,
        ScheduledExecutorService executor,
        Consumer<LicenseManager> cancelListener
    ) {
      this.licenseManager = mgr;
      this.executor = executor;
      this.cancelListener = cancelListener;
      // Schedule this to check the license every 24 hours no matter what
      this.future = executor.scheduleAtFixedRate(
          this::checkLicense,
          1,
          1,
          TimeUnit.DAYS
      );
    }

    protected void checkLicense() {
      try {
        if (executor.isTerminated()) {
          // no longer running
          return;
        }

        long timeRemainingMillis;
        License license = null;
        try {
          license = licenseManager.registerOrValidateLicense("");
          timeRemainingMillis = license.timeRemaining(TimeUnit.MILLISECONDS);
          loggedError = false;
        } catch (InvalidLicenseException e) {
          if (!loggedError) {
            log.error("Stored license was not valid. Please register a valid Confluent license. "
                      + "Will check again every 15 seconds until a valid license is found, "
                      + "but this log message will not repeat", e);
            loggedError = true;
          }
          timeRemainingMillis = TimeUnit.SECONDS.toMillis(15);
        }

        long millisBeforeNextCheck = millisForNextLicenseCheck(timeRemainingMillis);
        if (millisBeforeNextCheck > 0L) {
          try {
            log.debug(
                "Scheduling next license check in {}",
                durationOfMillis(millisBeforeNextCheck)
            );
            executor.schedule(this::checkLicense, millisBeforeNextCheck, TimeUnit.MILLISECONDS);
          } catch (RejectedExecutionException e) {
            log.error(
                "Unable to schedule next license check in {}",
                durationOfMillis(millisBeforeNextCheck)
            );
          }
        }
      } catch (Throwable t) {
        // Prevent the scheduler service from dying
        log.error("Failed to validate the Confluent license or schedule the next license check", t);
      }
    }

    protected String durationOfMillis(long millis) {
      long timeInSeconds = TimeUnit.MILLISECONDS.toSeconds(millis);
      long sec = timeInSeconds % 60;
      long minutes = timeInSeconds % 3600 / 60;
      long hours = timeInSeconds % 86400 / 3600;
      return String.format("%s:%s:%s", hours, minutes, sec);
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return future.getDelay(unit);
    }

    @Override
    public int compareTo(Delayed o) {
      return future.compareTo(o);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      boolean result = future.cancel(mayInterruptIfRunning);
      licenseManager.doStop();
      cancelListener.accept(this.licenseManager);
      return result;
    }

    @Override
    public boolean isCancelled() {
      return future.isCancelled();
    }

    @Override
    public boolean isDone() {
      return future.isDone();
    }

    @Override
    public Object get() throws InterruptedException, ExecutionException {
      return future.get();
    }

    @Override
    public Object get(
        long timeout,
        TimeUnit unit
    ) throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
      return future.get(timeout, unit);
    }

    @Override
    public int hashCode() {
      return future.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this || obj == future) {
        return true;
      }
      return false;
    }

    protected long millisForNextLicenseCheck(long timeRemainingMillis) {
      if (timeRemainingMillis <= 0L) {
        // Check every 15 seconds in case a new license becomes available
        return TimeUnit.SECONDS.toMillis(15);
      }

      // And maybe perform another check more frequently
      if (timeRemainingMillis < TimeUnit.MINUTES.toMillis(5L)) {
        return TimeUnit.SECONDS.toMillis(15);
      }
      if (timeRemainingMillis < TimeUnit.MINUTES.toMillis(60L)) {
        return TimeUnit.MINUTES.toMillis(5);
      }
      if (timeRemainingMillis < TimeUnit.HOURS.toMillis(24L)) {
        return TimeUnit.HOURS.toMillis(1);
      }
      // Otherwise it expires in more than 1 day, and the next daily check is all we need
      return -1L;
    }
  }
}
