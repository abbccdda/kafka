/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import java.io.Closeable;
import java.util.ArrayList;

/**
 * A tree to notify processes of cancellation in a thread safe way.
 * Parent element in the tree can set the cancellation status of children.
 *
 * Useful for request chaining where we wish to maintain a root handle for
 * cancellation, or the ability to cancel sub-requests arbitrarily.
 */

public class CancellationContext implements Closeable {
    private boolean cancelled;
    private final ArrayList<CancellationContext> child;

    private CancellationContext(boolean cancelled) {
        this.child = new ArrayList<>();
        this.cancelled = cancelled;
    }

    /**
     * Create a new root CancellationContext. For use when starting a request
     * chain. Generation of sub-contexts should be done through {@link #subContext()}
     *
     * @return A new root CancellationContext with no parent.
     */
    public static CancellationContext newContext() {
        return new CancellationContext(false);
    }

    /**
     * Create a new child CancellationContext.
     *
     * @return a new child CancellationContext
     */
    public CancellationContext subContext() {
        CancellationContext newContext = new CancellationContext(this.cancelled);
        synchronized (this) {
            this.child.add(newContext);
        }
        return newContext;
    }

    /**
     * Cancel this CancellationContext, causing all
     * sub-CancellationContexts to cancel.
     */
    public void cancel() {
        synchronized (this) {
            cancelled = true;
            for (CancellationContext childContext : child) {
                childContext.cancel();
            }
        }
    }

    /**
     * Get the cancellation status of this CancellationContext.
     * @return if this CancellationContext (or any parent) is canceled.
     */
    public boolean isCancelled() {
        synchronized (this) {
            return cancelled;
        }
    }

    // Implemented for use with try-with-resource
    @Override
    public void close() {
        cancel();
    }
}
