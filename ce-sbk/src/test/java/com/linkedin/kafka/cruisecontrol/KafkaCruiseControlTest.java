package com.linkedin.kafka.cruisecontrol;


import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback;
import com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalFuture;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetector;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.server.BrokerShutdownManager;
import io.confluent.databalancer.operation.BalanceOpExecutionCompletionCallback;
import io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.BalancerOperationFailedException;
import org.apache.kafka.common.errors.PlanComputationException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaCruiseControlTest {

    public static final int CONCURRENT_INTER_BROKER_PARTITION_MOVEMENTS = 1;
    public static final long REPLICATION_THROTTLE = 0L;
    public static final int CONCURRENT_LEADER_MOVEMENTS = 1;
    public static final int CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS = 1;
    public static final String UUID = "uuid";
    public static final ModelCompletenessRequirements REQUIREMENTS = new ModelCompletenessRequirements(
            1, 0.5, false);
    public static final ModelCompletenessRequirements STRONGER_REQUIREMENTS = new ModelCompletenessRequirements(
            2, 0.9, true);
    public static final Set<Integer> EMPTY_REQUESTED_DESTINATION_BROKER_IDS = Collections.emptySet();
    public static final List<String> EMPTY_GOALS = Collections.emptyList();

    private static final int BROKER_ID_TO_REMOVE = 3;
    private static final Optional<Long> BROKER_EPOCH_TO_REMOVE = Optional.of(4L);
    private static final Long MOCK_TIME_NOW = 0L;
    private static final Duration REMOVAL_TIMEOUT = Duration.ofSeconds(1);

    @Mock
    private KafkaCruiseControlConfig cruiseControlConfig;

    @Mock
    private BrokerRemovalCallback mockRemovalCallback;

    @Mock
    private LoadMonitor loadMonitor;

    @Mock
    private ExecutorService executorService;

    @Mock
    private Executor executor;

    @Mock
    private ExecutorState executorState;

    @Mock
    private ClusterModel clusterModel;

    @Mock
    private AnomalyDetector anomalyDetector;

    @Mock
    private GoalOptimizer goalOptimizer;

    @Mock
    private BrokerShutdownManager mockShutdownManager;

    @Mock
    private Time time;

    @Mock
    private OperationProgress operationProgress;

    @Mock
    private OptimizerResult optimizerResult;

    @Mock
    private ExecutionProposal executionProposal;

    @Mock
    private BalanceOpExecutionCompletionCallback mockExecutionCompletionCb;

    @InjectMocks
    private KafkaCruiseControl kafkaCruiseControl;

    @Before
    public void setUp() {
        when(time.milliseconds()).thenReturn(0L);

        when(mockRemovalCallback.currentState())
                .thenReturn(BrokerRemovalState.INITIAL_PLAN_COMPUTATION_INITIATED)
                .thenReturn(BrokerRemovalState.BROKER_SHUTDOWN_INITIATED)
                .thenReturn(BrokerRemovalState.PLAN_COMPUTATION_INITIATED)
                .thenReturn(BrokerRemovalState.PLAN_EXECUTION_INITIATED);
    }

    @Test
    public void rebalance_hasProposalToExecute_dryRun() throws Exception {
        when(goalOptimizer.defaultModelCompletenessRequirements()).thenReturn(STRONGER_REQUIREMENTS);
        when(goalOptimizer.modelCompletenessRequirementsForPrecomputing()).thenReturn(REQUIREMENTS);

        when(goalOptimizer.optimizations(any(OperationProgress.class), anyBoolean())).thenReturn(optimizerResult);

        OptimizerResult actualResult = kafkaCruiseControl.rebalance(
                EMPTY_GOALS, true /* dryRun */, REQUIREMENTS, operationProgress, false,
                CONCURRENT_INTER_BROKER_PARTITION_MOVEMENTS, CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS,
                CONCURRENT_LEADER_MOVEMENTS, false, null, null,
                REPLICATION_THROTTLE, UUID, false, false,
                false, false, EMPTY_REQUESTED_DESTINATION_BROKER_IDS, false);

        assertEquals(optimizerResult, actualResult);
        verify(goalOptimizer).optimizations(eq(operationProgress), eq(false));
        verify(loadMonitor, never()).acquireForModelGeneration(any(OperationProgress.class));
        verify(loadMonitor, never()).clusterModel(anyLong(), anyLong(), any(), anyBoolean(), any(OperationProgress.class));
        verifyNoProposalsExecuted();
    }

    @Test
    public void rebalance_hasProposalToExecute_actualRun() throws Exception {
        when(goalOptimizer.defaultModelCompletenessRequirements()).thenReturn(STRONGER_REQUIREMENTS);
        when(goalOptimizer.modelCompletenessRequirementsForPrecomputing()).thenReturn(REQUIREMENTS);

        Set<ExecutionProposal> oneExecutionProposal = Collections.singleton(executionProposal);
        when(optimizerResult.goalProposals()).thenReturn(oneExecutionProposal);
        when(goalOptimizer.optimizations(any(OperationProgress.class), anyBoolean())).thenReturn(optimizerResult);

        OptimizerResult actualResult = kafkaCruiseControl.rebalance(
                EMPTY_GOALS, false /* dryRun */, REQUIREMENTS, operationProgress, false,
                CONCURRENT_INTER_BROKER_PARTITION_MOVEMENTS, CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS,
                CONCURRENT_LEADER_MOVEMENTS, false, null, null,
                REPLICATION_THROTTLE, UUID, false, false,
                false, false, EMPTY_REQUESTED_DESTINATION_BROKER_IDS, false);

        verify(loadMonitor, never()).acquireForModelGeneration(any(OperationProgress.class));
        verify(loadMonitor, never()).clusterModel(anyLong(), anyLong(), any(), anyBoolean(), any(OperationProgress.class));
        assertEquals(optimizerResult, actualResult);
        verify(goalOptimizer).optimizations(eq(operationProgress), eq(false));
        verify(executor).setExecutionMode(anyBoolean());
        verify(executor).executeProposals(
                eq(oneExecutionProposal), anySet(), isNull(), eq(loadMonitor),
                eq(CONCURRENT_INTER_BROKER_PARTITION_MOVEMENTS), eq(CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS),
                eq(CONCURRENT_LEADER_MOVEMENTS), isNull(), eq(REPLICATION_THROTTLE), eq(UUID), isNull());
    }

    @Test
    public void rebalance_noProposalToExecute() throws Exception {
        when(goalOptimizer.defaultModelCompletenessRequirements()).thenReturn(STRONGER_REQUIREMENTS);
        when(goalOptimizer.modelCompletenessRequirementsForPrecomputing()).thenReturn(REQUIREMENTS);

        when(optimizerResult.goalProposals()).thenReturn(Collections.emptySet());
        when(goalOptimizer.optimizations(any(OperationProgress.class), anyBoolean())).thenReturn(optimizerResult);

        OptimizerResult actualResult = kafkaCruiseControl.rebalance(
                EMPTY_GOALS, false, REQUIREMENTS, operationProgress, false,
                CONCURRENT_INTER_BROKER_PARTITION_MOVEMENTS, CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS,
                CONCURRENT_LEADER_MOVEMENTS, false, null, null,
                REPLICATION_THROTTLE, UUID, false, false,
                false, false, EMPTY_REQUESTED_DESTINATION_BROKER_IDS, false);

        assertEquals(optimizerResult, actualResult);
        verify(goalOptimizer).optimizations(eq(operationProgress), eq(false));
        verify(loadMonitor, never()).acquireForModelGeneration(any(OperationProgress.class));
        verify(loadMonitor, never()).clusterModel(anyLong(), anyLong(), any(), anyBoolean(), any(OperationProgress.class));
        verifyNoProposalsExecuted();
    }

    @Test
    public void getProposals_ignoreProposalCache_explicitlyIgnoreProposalCache() throws Exception {
        when(goalOptimizer.defaultModelCompletenessRequirements()).thenReturn(REQUIREMENTS);
        when(goalOptimizer.modelCompletenessRequirementsForPrecomputing()).thenReturn(REQUIREMENTS);

        when(loadMonitor.clusterModel(
                eq(-1L), eq(MOCK_TIME_NOW), any(ModelCompletenessRequirements.class),
                eq(false), any(OperationProgress.class))).thenReturn(clusterModel);
        when(goalOptimizer.optimizations(
                eq(clusterModel), any(), any(OperationProgress.class), any(),
                any(), any(), anyBoolean(), any(), isNull(), eq(false))).thenReturn(optimizerResult);

        OptimizerResult actualResult = kafkaCruiseControl.getProposals(
                EMPTY_GOALS, REQUIREMENTS, operationProgress, false, false,
                null, false, false,
                true /* ignoreProposalCache */, false,
                EMPTY_REQUESTED_DESTINATION_BROKER_IDS, false);

        assertEquals(optimizerResult, actualResult);
        verify(loadMonitor).acquireForModelGeneration(eq(operationProgress));
        verify(loadMonitor).clusterModel(
                eq(-1L), eq(MOCK_TIME_NOW), any(ModelCompletenessRequirements.class),
                eq(false), eq(operationProgress));
        verify(goalOptimizer).optimizations(
                eq(clusterModel), anyList(), eq(operationProgress), any(), anySet(), anySet(),
                anyBoolean(), eq(EMPTY_REQUESTED_DESTINATION_BROKER_IDS), isNull(), eq(false));
    }

    @Test
    public void getProposals_ignoreProposalCache_weakerRequirements() throws Exception {
        when(goalOptimizer.defaultModelCompletenessRequirements()).thenReturn(REQUIREMENTS);
        when(goalOptimizer.modelCompletenessRequirementsForPrecomputing()).thenReturn(STRONGER_REQUIREMENTS);

        when(loadMonitor.clusterModel(eq(-1L), eq(MOCK_TIME_NOW), any(ModelCompletenessRequirements.class),
                eq(false), any(OperationProgress.class))).thenReturn(clusterModel);
        when(goalOptimizer.optimizations(
                eq(clusterModel), any(), any(OperationProgress.class), any(),
                any(), any(), anyBoolean(), any(), isNull(), eq(false))).thenReturn(optimizerResult);

        OptimizerResult actualResult = kafkaCruiseControl.getProposals(
                EMPTY_GOALS, REQUIREMENTS, operationProgress, false, false, null,
                false, false, false, false,
                Collections.emptySet(), false);

        assertEquals(optimizerResult, actualResult);
        verify(loadMonitor).acquireForModelGeneration(eq(operationProgress));
        verify(loadMonitor).clusterModel(
                eq(-1L), eq(MOCK_TIME_NOW), any(ModelCompletenessRequirements.class),
                eq(false), eq(operationProgress));
        verify(goalOptimizer).optimizations(
                eq(clusterModel), anyList(), eq(operationProgress), any(),
                anySet(), anySet(), eq(false), eq(EMPTY_REQUESTED_DESTINATION_BROKER_IDS), isNull(), eq(false));
    }

    @Test
    public void getProposals_ignoreProposalCache_throwsKafkaCruiseControlException() throws Exception {
        when(goalOptimizer.defaultModelCompletenessRequirements()).thenReturn(REQUIREMENTS);
        when(goalOptimizer.modelCompletenessRequirementsForPrecomputing()).thenReturn(REQUIREMENTS);
        when(loadMonitor.clusterModel(eq(-1L), eq(MOCK_TIME_NOW), any(ModelCompletenessRequirements.class),
                                      eq(false), any(OperationProgress.class)))
                .thenThrow(new NotEnoughValidWindowsException("not enough valid windows"));

        assertThrows(KafkaCruiseControlException.class, () -> kafkaCruiseControl.getProposals(
                EMPTY_GOALS, REQUIREMENTS, operationProgress, false, false,
                null, false, false, true,
                false, Collections.emptySet(), false));
    }

    @Test
    public void getProposals_useProposalCache() throws Exception {
        when(goalOptimizer.defaultModelCompletenessRequirements()).thenReturn(STRONGER_REQUIREMENTS);
        when(goalOptimizer.modelCompletenessRequirementsForPrecomputing()).thenReturn(REQUIREMENTS);

        when(goalOptimizer.optimizations(any(OperationProgress.class), anyBoolean())).thenReturn(optimizerResult);

        OptimizerResult actualResult = kafkaCruiseControl.getProposals(
                Collections.emptyList(), REQUIREMENTS, operationProgress, false, false,
                null, false, false, false,
                false, Collections.emptySet(), false);

        assertEquals(optimizerResult, actualResult);
        verify(goalOptimizer).optimizations(eq(operationProgress), eq(false));
        verify(loadMonitor, never()).acquireForModelGeneration(any(OperationProgress.class));
        verify(loadMonitor, never()).clusterModel(anyLong(), anyLong(), any(), anyBoolean(), any(OperationProgress.class));
    }

    @Test
    public void removeBroker_initialPlanComputationFails() throws Exception {
        when(loadMonitor.acquireForModelGeneration(any())).thenAnswer(invocation -> {
            throw new KafkaCruiseControlException("boom");
        });

        PlanComputationException thrownException = assertThrows(PlanComputationException.class,
            () -> kafkaCruiseControl.removeBroker(BROKER_ID_TO_REMOVE, BROKER_EPOCH_TO_REMOVE,
                mockExecutionCompletionCb, mockRemovalCallback, "").execute(REMOVAL_TIMEOUT)
        );

        verify(mockRemovalCallback).currentState();
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_FAILURE, thrownException);
        verify(mockExecutionCompletionCb, never()).accept(anyBoolean(), any());
        verify(mockShutdownManager, never()).maybeShutdownBroker(anyInt(), any());
        verifyNoProposalsExecuted();
    }

    @Test
    public void removeBroker_brokerShutdownFails() throws Exception {
        TimeoutException exception = new TimeoutException("boom");
        when(mockShutdownManager.maybeShutdownBroker(BROKER_ID_TO_REMOVE, BROKER_EPOCH_TO_REMOVE)).thenThrow(exception);
        when(loadMonitor.clusterModel(anyLong(), any(), any())).thenReturn(clusterModel);

        BalancerOperationFailedException thrownException = assertThrows(BalancerOperationFailedException.class,
            () -> kafkaCruiseControl.removeBroker(BROKER_ID_TO_REMOVE, BROKER_EPOCH_TO_REMOVE,
                                                  mockExecutionCompletionCb, mockRemovalCallback, "").execute(REMOVAL_TIMEOUT)
        );

        assertEquals(exception, thrownException.getCause());
        verify(clusterModel).setBrokerState(BROKER_ID_TO_REMOVE, Broker.State.DEAD); // verify plan-computation
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_FAILURE, thrownException);
        verify(mockExecutionCompletionCb, never()).accept(anyBoolean(), any());
        verifyNoProposalsExecuted();
    }

    @Test
    public void removeBroker_brokerShutdownThrowsInterruptedException() throws Exception {
        InterruptedException expectedException = new InterruptedException("boom");
        when(mockShutdownManager.maybeShutdownBroker(BROKER_ID_TO_REMOVE, BROKER_EPOCH_TO_REMOVE)).thenThrow(expectedException);
        when(loadMonitor.clusterModel(anyLong(), any(), any())).thenReturn(clusterModel);

        assertThrows(InterruptedException.class,
            () -> kafkaCruiseControl.removeBroker(BROKER_ID_TO_REMOVE, BROKER_EPOCH_TO_REMOVE,
                    mockExecutionCompletionCb, mockRemovalCallback, "").execute(REMOVAL_TIMEOUT)
        );

        verify(clusterModel).setBrokerState(BROKER_ID_TO_REMOVE, Broker.State.DEAD); // verify plan-computation
        verify(mockRemovalCallback, times(2)).currentState();
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);
        verifyNoMoreInteractions(mockRemovalCallback);
        verify(mockExecutionCompletionCb, never()).accept(anyBoolean(), any());
        verifyNoProposalsExecuted();
    }

    @Test
    public void removeBroker_canBeCancelled() throws Exception {
        when(mockShutdownManager.maybeShutdownBroker(BROKER_ID_TO_REMOVE, BROKER_EPOCH_TO_REMOVE)).thenAnswer(invocation -> {
            Thread.sleep(10000);
            return null;
        });
        when(loadMonitor.clusterModel(anyLong(), any(), any())).thenReturn(clusterModel);
        AtomicReference<Exception> thrownException = new AtomicReference<>();

        BrokerRemovalFuture removeFuture = kafkaCruiseControl.removeBroker(
            BROKER_ID_TO_REMOVE, BROKER_EPOCH_TO_REMOVE, mockExecutionCompletionCb, mockRemovalCallback, "");
        new Thread(() -> {
            try {
                removeFuture.execute(REMOVAL_TIMEOUT);
            } catch (Throwable throwable) {
                assert throwable instanceof Exception;
                thrownException.set((Exception) throwable);
            }
        }).start();
        removeFuture.cancel();

        TestUtils.waitForCondition(() -> thrownException.get() != null, "Expected the future execution to throw an exception");

        assertEquals(CancellationException.class, thrownException.get().getClass());
    }

    @Test
    public void removeBroker_planRecomputationFails() throws Exception {
        AtomicInteger invocation = new AtomicInteger(1);
        // throw an exception on the second plan computation
        when(loadMonitor.acquireForModelGeneration(any())).thenAnswer(call -> {
            if (invocation.get() == 2) {
                throw new KafkaCruiseControlException("boom");
            }
            invocation.getAndIncrement();
            return null;
        });
        when(loadMonitor.clusterModel(anyLong(), any(), any())).thenReturn(clusterModel);

        PlanComputationException thrownException = assertThrows(PlanComputationException.class,
            () -> kafkaCruiseControl.removeBroker(BROKER_ID_TO_REMOVE, BROKER_EPOCH_TO_REMOVE, mockExecutionCompletionCb, mockRemovalCallback, "").execute(REMOVAL_TIMEOUT)
        );

        verify(clusterModel).setBrokerState(BROKER_ID_TO_REMOVE, Broker.State.DEAD); // first plan-computation
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS);
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_FAILURE, thrownException);
        verify(mockExecutionCompletionCb, never()).accept(anyBoolean(), any());
        verifyNoProposalsExecuted();
    }

    @Test
    public void removeBroker_planExecutionFails() throws NotEnoughValidWindowsException, KafkaCruiseControlException, ClusterModel.NonExistentBrokerException {
        when(loadMonitor.clusterModel(anyLong(), any(), any())).thenReturn(clusterModel);
        // plan execution fails when the proposal has nothing to execute
        when(goalOptimizer.optimizations(
            eq(clusterModel), eq(Collections.emptyList()), any(), isNull(),
            eq(Collections.emptySet()), eq(Collections.emptySet()), eq(false),
            eq(Collections.emptySet()), isNull(), eq(false))).thenReturn(optimizerResult);
        Set<ExecutionProposal> goals = new HashSet<>();
        goals.add(mock(ExecutionProposal.class));
        goals.add(mock(ExecutionProposal.class));
        when(optimizerResult.goalProposals()).thenReturn(goals);

        doAnswer(invocation -> {
            throw new IllegalStateException("boom");
        }).when(executor).setExecutionMode(false);

        BalancerOperationFailedException thrownException = assertThrows(BalancerOperationFailedException.class,
            () -> kafkaCruiseControl.removeBroker(BROKER_ID_TO_REMOVE, BROKER_EPOCH_TO_REMOVE,
                    mockExecutionCompletionCb, mockRemovalCallback, "").execute(REMOVAL_TIMEOUT)
        );

        assertTrue("Expected exception cause to be of type IllegalStateException",
            thrownException.getCause() instanceof IllegalStateException);
        verify(clusterModel, times(2)).setBrokerState(BROKER_ID_TO_REMOVE, Broker.State.DEAD);
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS);
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_SUCCESS);
        // should we wrapped in a KafkaCruiseControlException
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_EXECUTION_FAILURE, thrownException);
        verify(mockExecutionCompletionCb, never()).accept(anyBoolean(), any());
        verifyNoProposalsExecuted();
    }

    @Test
    public void removeBroker_cannotAcquireExecutorReservation() throws Throwable {
        String uuid = "uuid";
        TimeoutException expectedException = new TimeoutException("timeout!");
        when(executor.reserveAndAbortOngoingExecutions(any())).thenThrow(expectedException);

        BalancerOperationFailedException thrownException = assertThrows(BalancerOperationFailedException.class,
            () -> kafkaCruiseControl.removeBroker(BROKER_ID_TO_REMOVE, BROKER_EPOCH_TO_REMOVE, mockExecutionCompletionCb, mockRemovalCallback, uuid).execute(REMOVAL_TIMEOUT)
        );

        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_FAILURE, thrownException);
        verify(mockExecutionCompletionCb, never()).accept(anyBoolean(), any());
    }

    @Test
    public void removeBroker_successfullySubmitted() throws Throwable {
        String uuid = "uuid";
        Set<Integer> brokersToRemove = new HashSet<>();
        brokersToRemove.add(BROKER_ID_TO_REMOVE);
        ArgumentCaptor<BalanceOpExecutionCompletionCallback> callbackCaptor = ArgumentCaptor.forClass(BalanceOpExecutionCompletionCallback.class);
        when(loadMonitor.clusterModel(anyLong(), any(), any())).thenReturn(clusterModel);
        Set<ExecutionProposal> proposals = new HashSet<>();
        proposals.add(mock(ExecutionProposal.class));
        when(optimizerResult.goalProposals()).thenReturn(proposals);
        when(goalOptimizer.optimizations(
            eq(clusterModel), eq(Collections.emptyList()), any(), isNull(),
            eq(Collections.emptySet()), eq(Collections.emptySet()), eq(false),
            eq(Collections.emptySet()), isNull(), eq(false))).thenReturn(optimizerResult);

        kafkaCruiseControl.removeBroker(BROKER_ID_TO_REMOVE, BROKER_EPOCH_TO_REMOVE,
                                        mockExecutionCompletionCb, mockRemovalCallback, uuid).execute(REMOVAL_TIMEOUT);

        verify(clusterModel, times(2)).setBrokerState(BROKER_ID_TO_REMOVE, Broker.State.DEAD);
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS);
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_SUCCESS);
        verify(executor).setExecutionMode(false);
        verify(executor, times(2)).state();
        verify(executor).reserveAndAbortOngoingExecutions(Duration.ofMinutes(1));
        verify(executor).executeProposals(eq(proposals), eq(brokersToRemove), eq(brokersToRemove), eq(loadMonitor),
            isNull(), eq(0), isNull(), isNull(), anyLong(), eq(uuid), callbackCaptor.capture());
        // Since a mock Executor, we *do not* expect the ExecutionCompletionCb to be called.
        verify(mockExecutionCompletionCb, never()).accept(anyBoolean(), any());

        // Now, pretend success
        callbackCaptor.getValue().accept(true, null);
        verify(mockRemovalCallback)
                .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_EXECUTION_SUCCESS);
        verify(mockExecutionCompletionCb).accept(eq(true), isNull());
    }

    /**
     * If a broker is offline (shutdown) and has no replicas on it,
     * it won't be present in the cluster model and the plan computation for its removal will be empty.
     * In such cases, the removal should still complete
     */
    @Test
    public void removeBroker_emptyPlanAndNonExistentBrokerShouldCompleteSuccessfully() throws Throwable {
        String uuid = "uuid";
        when(loadMonitor.clusterModel(anyLong(), any(), any())).thenReturn(clusterModel);
        Set<ExecutionProposal> emptyProposals = new HashSet<>();
        when(optimizerResult.goalProposals()).thenReturn(emptyProposals); // empty plan
        // assume the broker is offline and has no replicas on it
        doThrow(new ClusterModel.NonExistentBrokerException("non existent broker"))
            .when(clusterModel).setBrokerState(BROKER_ID_TO_REMOVE, Broker.State.DEAD);

        when(goalOptimizer.optimizations(
            eq(clusterModel), eq(Collections.emptyList()), any(), isNull(),
            eq(Collections.emptySet()), eq(Collections.emptySet()), eq(false),
            eq(Collections.emptySet()), isNull(), eq(false))).thenReturn(optimizerResult);

        kafkaCruiseControl.removeBroker(BROKER_ID_TO_REMOVE, BROKER_EPOCH_TO_REMOVE,
            mockExecutionCompletionCb, mockRemovalCallback, uuid).execute(REMOVAL_TIMEOUT);

        verify(clusterModel, times(2)).setBrokerState(BROKER_ID_TO_REMOVE, Broker.State.DEAD);
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS);
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_SUCCESS);
        verify(executor).reserveAndAbortOngoingExecutions(Duration.ofMinutes(1));
        verifyNoProposalsExecuted();
        verify(mockExecutionCompletionCb).accept(true, null);
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_EXECUTION_SUCCESS);
    }


    @Test
    public void removeBroker_executionFailure() throws Throwable {
        String uuid = "uuid";
        Set<Integer> brokersToRemove = new HashSet<>();
        brokersToRemove.add(BROKER_ID_TO_REMOVE);
        Exception expectedExecutionException = new Exception("boom");
        ArgumentCaptor<BalanceOpExecutionCompletionCallback> callbackCaptor = ArgumentCaptor.forClass(BalanceOpExecutionCompletionCallback.class);
        when(loadMonitor.clusterModel(anyLong(), any(), any())).thenReturn(clusterModel);
        Set<ExecutionProposal> proposals = new HashSet<>();
        proposals.add(mock(ExecutionProposal.class));
        when(optimizerResult.goalProposals()).thenReturn(proposals);
        when(goalOptimizer.optimizations(
                eq(clusterModel), eq(Collections.emptyList()), any(), isNull(),
                eq(Collections.emptySet()), eq(Collections.emptySet()), eq(false),
                eq(Collections.emptySet()), isNull(), eq(false))).thenReturn(optimizerResult);

        kafkaCruiseControl.removeBroker(BROKER_ID_TO_REMOVE, BROKER_EPOCH_TO_REMOVE,
                mockExecutionCompletionCb, mockRemovalCallback, uuid).execute(REMOVAL_TIMEOUT);

        verify(mockRemovalCallback)
                .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_SUCCESS);
        verify(executor).setExecutionMode(false);
        verify(executor, times(2)).state();
        verify(executor).executeProposals(eq(proposals), eq(brokersToRemove), eq(brokersToRemove), eq(loadMonitor),
                isNull(), eq(0), isNull(), isNull(), anyLong(), eq(uuid), callbackCaptor.capture());
        // Since a mock Executor, we *do not* expect the ExecutionCompletionCb to be called.
        verify(mockExecutionCompletionCb, never()).accept(anyBoolean(), any());

        // Now, a failure
        callbackCaptor.getValue().accept(false, expectedExecutionException);
        verify(mockRemovalCallback)
                .registerEvent(eq(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_EXECUTION_FAILURE), eq(expectedExecutionException));
        verify(mockExecutionCompletionCb).accept(eq(false), eq(expectedExecutionException));
    }

    private void verifyNoProposalsExecuted() {
        verify(executor, never()).executeProposals(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    public void addBroker_metadataNotReady() throws Exception {
        Node mockNode1 = mock(Node.class);
        when(mockNode1.id()).thenReturn(1);

        List<Node> clusterNodes = Arrays.asList(mockNode1);
        Cluster mockCluster = new Cluster("testCluster", clusterNodes, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
        MetadataClient.ClusterAndGeneration mockCandG = mock(MetadataClient.ClusterAndGeneration.class);
        when(loadMonitor.refreshClusterAndGeneration()).thenReturn(mockCandG);
        when(mockCandG.cluster()).thenReturn(mockCluster);

        Set<Integer> newBrokers = new HashSet<>();
        newBrokers.add(1);
        newBrokers.add(2);

        TimeoutException expectedException = new TimeoutException("Exceeded time");

        KafkaCruiseControlException thrownException = assertThrows(KafkaCruiseControlException.class,
                () -> kafkaCruiseControl.addBrokers(newBrokers, null, "testOpId")
        );

        assertEquals(expectedException.getClass(), thrownException.getCause().getClass());
        verifyNoProposalsExecuted();
    }

    @Test
    public void addBroker_metadataNotReadyInitially() throws Exception {
        KafkaCruiseControl kcc = spy(kafkaCruiseControl);
        Node mockNode1 = mock(Node.class);
        when(mockNode1.id()).thenReturn(1);
        Node mockNode2 = mock(Node.class);
        when(mockNode2.id()).thenReturn(2);

        List<Node> clusterNodesInitial = Arrays.asList(mockNode1);
        List<Node> clusterNodesSubsequent = Arrays.asList(mockNode1, mockNode2);
        Cluster mockClusterInitial = new Cluster("testCluster", clusterNodesInitial, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
        Cluster mockClusterSubsequent = new Cluster("testCluster", clusterNodesSubsequent, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
        MetadataClient.ClusterAndGeneration mockCandG = mock(MetadataClient.ClusterAndGeneration.class);
        when(loadMonitor.refreshClusterAndGeneration()).thenReturn(mockCandG);
        when(mockCandG.cluster()).thenReturn(mockClusterInitial).thenReturn(mockClusterInitial).thenReturn(mockClusterSubsequent);

        when(goalOptimizer.defaultModelCompletenessRequirements()).thenReturn(STRONGER_REQUIREMENTS);

        Set<Integer> newBrokers = new HashSet<>();
        newBrokers.add(1);
        newBrokers.add(2);

        when(loadMonitor.clusterModel(anyLong(), any(), any())).thenReturn(clusterModel);
        Set<ExecutionProposal> proposals = new HashSet<>();
        proposals.add(mock(ExecutionProposal.class));
        when(optimizerResult.goalProposals()).thenReturn(proposals);

        when(goalOptimizer.optimizations(
                eq(clusterModel), eq(Collections.emptyList()), any(), isNull(),
                eq(Collections.emptySet()), eq(Collections.emptySet()), eq(false),
                any(), isNull(), eq(false))).thenReturn(optimizerResult);
        when(executor.state()).thenReturn(executorState);
        when(executorState.recentlyDemotedBrokers()).thenReturn(Collections.emptySet());

        kcc.addBrokers(newBrokers, null, "testOpId");
        verify(clusterModel).setBrokerState(1, Broker.State.NEW); // verify plan-computation
        verify(clusterModel).setBrokerState(2, Broker.State.NEW); // verify plan-computation
        verify(executor).dropRecentlyRemovedBrokers(eq(newBrokers));
        verify(executor).executeProposals(eq(proposals), eq(Collections.emptySet()), eq(null), eq(loadMonitor),
                isNull(), any(), isNull(), isNull(), any(), any(), isNull());
        verify(mockCandG, atLeast(3)).cluster();
        verify(kcc, atLeast(3)).brokersAreKnown(newBrokers);
    }

    @Test
    public void addBroker_notEnoughValidWindows() throws Exception {
        KafkaCruiseControl kcc = spy(kafkaCruiseControl);
        Node mockNode1 = mock(Node.class);
        when(mockNode1.id()).thenReturn(1);
        Node mockNode2 = mock(Node.class);
        when(mockNode2.id()).thenReturn(2);

        List<Node> clusterNodes = Arrays.asList(mockNode1, mockNode2);
        Cluster mockCluster = new Cluster("testCluster", clusterNodes, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
        MetadataClient.ClusterAndGeneration mockCandG = mock(MetadataClient.ClusterAndGeneration.class);
        when(loadMonitor.refreshClusterAndGeneration()).thenReturn(mockCandG);
        when(mockCandG.cluster()).thenReturn(mockCluster);

        when(goalOptimizer.defaultModelCompletenessRequirements()).thenReturn(STRONGER_REQUIREMENTS);

        // Simulate not getting enough windows.
        when(loadMonitor.clusterModel(anyLong(), any(), any())).thenThrow(NotEnoughValidWindowsException.class).
                thenThrow(NotEnoughValidWindowsException.class).thenReturn(clusterModel);
        Set<ExecutionProposal> proposals = new HashSet<>();
        proposals.add(mock(ExecutionProposal.class));
        when(optimizerResult.goalProposals()).thenReturn(proposals);

        when(goalOptimizer.optimizations(
                eq(clusterModel), eq(Collections.emptyList()), any(), isNull(),
                eq(Collections.emptySet()), eq(Collections.emptySet()), eq(false),
                any(), isNull(), eq(false))).thenReturn(optimizerResult);
        when(executor.state()).thenReturn(executorState);
        when(executorState.recentlyDemotedBrokers()).thenReturn(Collections.emptySet());

        Set<Integer> newBrokers = new HashSet<>();
        newBrokers.add(1);
        newBrokers.add(2);

        kcc.addBrokers(newBrokers, null, "testOpId");
        verify(clusterModel).setBrokerState(1, Broker.State.NEW); // verify plan-computation
        verify(clusterModel).setBrokerState(2, Broker.State.NEW); // verify plan-computation
        verify(executor).dropRecentlyRemovedBrokers(eq(newBrokers));
        verify(executor).executeProposals(eq(proposals), eq(Collections.emptySet()), eq(null), eq(loadMonitor),
                isNull(), any(), isNull(), isNull(), any(), any(), isNull());
        verify(loadMonitor, atLeast(3)).clusterModel(anyLong(), any(), any());
        verify(mockCandG).cluster();
    }

    @Test
    public void addBroker_otherOptimizationFailure() throws Exception {
        KafkaCruiseControl kcc = spy(kafkaCruiseControl);
        Node mockNode1 = mock(Node.class);
        when(mockNode1.id()).thenReturn(1);
        Node mockNode2 = mock(Node.class);
        when(mockNode2.id()).thenReturn(2);

        List<Node> clusterNodes = Arrays.asList(mockNode1, mockNode2);
        Cluster mockCluster = new Cluster("testCluster", clusterNodes, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
        MetadataClient.ClusterAndGeneration mockCandG = mock(MetadataClient.ClusterAndGeneration.class);
        when(loadMonitor.refreshClusterAndGeneration()).thenReturn(mockCandG);
        when(mockCandG.cluster()).thenReturn(mockCluster);

        when(goalOptimizer.defaultModelCompletenessRequirements()).thenReturn(STRONGER_REQUIREMENTS);

        Set<Integer> newBrokers = new HashSet<>();
        newBrokers.add(1);
        newBrokers.add(2);

        when(loadMonitor.clusterModel(anyLong(), any(), any())).thenReturn(clusterModel);
        when(executor.state()).thenReturn(executorState);
        when(executorState.recentlyDemotedBrokers()).thenReturn(Collections.emptySet());
        when(goalOptimizer.optimizations(
                eq(clusterModel), eq(Collections.emptyList()), any(), isNull(),
                eq(Collections.emptySet()), eq(Collections.emptySet()), eq(false),
                any(), isNull(), eq(false))).thenThrow(KafkaCruiseControlException.class);

        assertThrows(KafkaCruiseControlException.class,
                () -> kcc.addBrokers(newBrokers, null, "testOpId"));

        verify(clusterModel).setBrokerState(1, Broker.State.NEW); // verify plan-computation
        verify(clusterModel).setBrokerState(2, Broker.State.NEW); // verify plan-computation
        verify(executor).dropRecentlyRemovedBrokers(eq(newBrokers));
        verify(executor, never()).executeProposals(any(), eq(Collections.emptySet()), eq(null), eq(loadMonitor),
                isNull(), any(), isNull(), isNull(), any(), any(), isNull());
    }

}
