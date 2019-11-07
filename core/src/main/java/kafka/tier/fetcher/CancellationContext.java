/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import java.io.Closeable;

/**
 * A tree to notify processes of cancellation in a thread safe way.
 * Parent element in the tree can result in cancellation of children.
 *
 * Useful for request chaining where we wish to maintain a root handle for
 * cancellation, or the ability to cancel sub-requests arbitrarily.
 */

public class CancellationContext implements Closeable {
    private boolean cancelled;
    private CancellationContext parent;

    private CancellationContext(CancellationContext parent) {
        this.parent = parent;
    }

    /**
     * Create a new root CancellationContext. For use when starting a request
     * chain. Generation of sub-contexts should be done through {@link #subContext()}
     *
     * @return A new root CancellationContext with no parent.
     */
    public static CancellationContext newContext() {
        return new CancellationContext(null);
    }

    /**
     * Create a new child CancellationContext.
     *
     * @return a new child CancellationContext
     */
    public CancellationContext subContext() {
        return new CancellationContext(this);
    }

    /**
     * Cancel this CancellationContext, causing all
     * sub-CancellationContexts to cancel.
     */
    public void cancel() {
        synchronized (this) {
            cancelled = true;
        }
    }

    /**
     * Get the cancellation status of this CancellationContext.
     * @return if this CancellationContext (or any parent) is canceled.
     */
    public boolean isCancelled() {
        synchronized (this) {
            return cancelled || parent != null && parent.isCancelled();
        }
    }

    // Implemented for use with try-with-resource
    @Override
    public void close() {
        cancel();
    }
}
