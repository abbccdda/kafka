/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.fetcher;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CancellationContextTest {
    @Test
    public void cancellationChain() {
        CancellationContext rootContext = CancellationContext.newContext();
        CancellationContext l1 = rootContext.subContext();
        CancellationContext l2 = l1.subContext();
        CancellationContext l3 = l2.subContext();

        l3.cancel();
        assertTrue("Canceling a context works", l3.isCancelled());
        assertFalse("Canceling the lowest context does not cancel the higher contexts", l2.isCancelled());
    }

    @Test
    public void testCancellationWithMultipleChildren() {
        CancellationContext rootContext = CancellationContext.newContext();
        CancellationContext l1 = rootContext.subContext();
        CancellationContext l2 = rootContext.subContext();
        CancellationContext l3 = rootContext.subContext();

        // cancel one of the leaf contexts
        l2.cancel();
        assertFalse(rootContext.isCancelled());
        assertFalse(l1.isCancelled());
        assertTrue(l2.isCancelled());
        assertFalse(l3.isCancelled());

        // cancel the root context
        rootContext.cancel();

        // all contexts should now be cancelled
        assertTrue(rootContext.isCancelled());
        assertTrue(l1.isCancelled());
        assertTrue(l2.isCancelled());
        assertTrue(l2.isCancelled());
    }
}
