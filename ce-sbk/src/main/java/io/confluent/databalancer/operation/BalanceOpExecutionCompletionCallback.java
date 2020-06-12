/*
 * Copyright (c) 2020. Confluent, Inc.
 */

package io.confluent.databalancer.operation;

import javax.annotation.Nullable;

/**
 * A callback routine to be invoked when a CruiseControl ProposalExecution (rebalance plan) completes execution,
 * whether successfully or otherwise.
 */
public interface BalanceOpExecutionCompletionCallback {
    /**
     *
     * @param operationSuccess -- if the operation succeeded or did not complete. A successful operation completed all the
     *                         work it intended to do.
     * @param t --  Exception, if any, raised during execution of the operation. If an exception is non-null, the
     *          DataBalancer code will fail the removal operation with it
     */
    void accept(boolean operationSuccess, @Nullable Throwable t);
}
