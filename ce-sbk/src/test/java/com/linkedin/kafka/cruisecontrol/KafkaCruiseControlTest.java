package com.linkedin.kafka.cruisecontrol;


import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetector;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.server.BrokerShutdownManager;
import io.confluent.databalancer.operation.BrokerRemovalCallback;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.utils.Time;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import org.mockito.junit.MockitoJUnitRunner;


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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

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

    @InjectMocks
    private KafkaCruiseControl kafkaCruiseControl;

    @Before
    public void setUp() {
        when(time.milliseconds()).thenReturn(0L);
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
                eq(CONCURRENT_LEADER_MOVEMENTS), isNull(), eq(REPLICATION_THROTTLE), eq(UUID));
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
    public void removeBroker_executorReservationFails() throws KafkaCruiseControlException {
        // TODO: Fill when https://github.com/confluentinc/ce-kafka/pull/1730 is merged
    }

    @Test
    public void removeBroker_initialPlanComputationFails() throws Exception {
        KafkaCruiseControlException expectedException = new KafkaCruiseControlException("boom");
        when(loadMonitor.acquireForModelGeneration(any())).thenAnswer(invocation -> {
            throw expectedException;
        });

        KafkaCruiseControlException thrownException = assertThrows(KafkaCruiseControlException.class,
            () -> kafkaCruiseControl.removeBroker(BROKER_ID_TO_REMOVE, BROKER_EPOCH_TO_REMOVE, mockRemovalCallback, "")
        );

        assertEquals(expectedException, thrownException);
        verify(mockRemovalCallback, only())
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_FAILURE, expectedException);
        verify(mockShutdownManager, never()).maybeShutdownBroker(anyInt(), any());
        verifyNoProposalsExecuted();
    }

    @Test
    public void removeBroker_brokerShutdownFails() throws Exception {
        TimeoutException expectedException = new TimeoutException("boom");
        when(mockShutdownManager.maybeShutdownBroker(BROKER_ID_TO_REMOVE, BROKER_EPOCH_TO_REMOVE)).thenThrow(expectedException);
        when(loadMonitor.clusterModel(anyLong(), any(), any())).thenReturn(clusterModel);

        KafkaCruiseControlException thrownException = assertThrows(KafkaCruiseControlException.class,
            () -> kafkaCruiseControl.removeBroker(BROKER_ID_TO_REMOVE, BROKER_EPOCH_TO_REMOVE, mockRemovalCallback, "")
        );

        assertEquals(expectedException, thrownException.getCause());
        verify(clusterModel).setBrokerState(BROKER_ID_TO_REMOVE, Broker.State.DEAD); // verify plan-computation
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_FAILURE, expectedException);
        verifyNoProposalsExecuted();
    }

    @Test
    public void removeBroker_planRecomputationFails() throws Exception {
        AtomicInteger invocation = new AtomicInteger(1);
        KafkaCruiseControlException expectedException = new KafkaCruiseControlException("boom");
        // throw an exception on the second plan computation
        when(loadMonitor.acquireForModelGeneration(any())).thenAnswer(call -> {
            if (invocation.get() == 2) {
                throw expectedException;
            }
            invocation.getAndIncrement();
            return null;
        });
        when(loadMonitor.clusterModel(anyLong(), any(), any())).thenReturn(clusterModel);

        KafkaCruiseControlException thrownException = assertThrows(KafkaCruiseControlException.class,
            () -> kafkaCruiseControl.removeBroker(BROKER_ID_TO_REMOVE, BROKER_EPOCH_TO_REMOVE, mockRemovalCallback, "")
        );

        assertEquals(expectedException, thrownException);
        verify(clusterModel).setBrokerState(BROKER_ID_TO_REMOVE, Broker.State.DEAD); // first plan-computation
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS);
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_FAILURE, expectedException);
        verifyNoProposalsExecuted();
    }

    @Test
    public void removeBroker_planExecutionFails() throws NotEnoughValidWindowsException, KafkaCruiseControlException {
        when(loadMonitor.clusterModel(anyLong(), any(), any())).thenReturn(clusterModel);
        // plan execution fails when the proposal has nothing to execute
        when(goalOptimizer.optimizations(
            eq(clusterModel), eq(Collections.emptyList()), any(), isNull(),
            eq(Collections.emptySet()), eq(Collections.emptySet()), eq(false),
            eq(Collections.emptySet()), isNull(), eq(false))).thenReturn(optimizerResult);

        KafkaCruiseControlException thrownException = assertThrows(KafkaCruiseControlException.class,
            () -> kafkaCruiseControl.removeBroker(BROKER_ID_TO_REMOVE, BROKER_EPOCH_TO_REMOVE, mockRemovalCallback, "")
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
        verify(mockRemovalCallback)
            .registerEvent(eq(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_EXECUTION_FAILURE), any(IllegalStateException.class));
        verifyNoProposalsExecuted();
    }

    @Test
    public void removeBroker_successfullySubmitted() throws NotEnoughValidWindowsException, KafkaCruiseControlException {
        String uuid = "uuid";
        Set<Integer> brokersToRemove = new HashSet<>();
        brokersToRemove.add(BROKER_ID_TO_REMOVE);
        when(loadMonitor.clusterModel(anyLong(), any(), any())).thenReturn(clusterModel);
        Set<ExecutionProposal> proposals = new HashSet<>();
        proposals.add(mock(ExecutionProposal.class));
        when(optimizerResult.goalProposals()).thenReturn(proposals);
        when(goalOptimizer.optimizations(
            eq(clusterModel), eq(Collections.emptyList()), any(), isNull(),
            eq(Collections.emptySet()), eq(Collections.emptySet()), eq(false),
            eq(Collections.emptySet()), isNull(), eq(false))).thenReturn(optimizerResult);

        kafkaCruiseControl.removeBroker(BROKER_ID_TO_REMOVE, BROKER_EPOCH_TO_REMOVE, mockRemovalCallback, uuid);

        verify(clusterModel, times(2)).setBrokerState(BROKER_ID_TO_REMOVE, Broker.State.DEAD);
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS);
        verify(mockRemovalCallback)
            .registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_SUCCESS);
        verify(executor).setExecutionMode(false);
        verify(executor, times(2)).state();
        verify(executor).executeRemoveBrokerProposals(eq(proposals), eq(brokersToRemove), eq(brokersToRemove), eq(loadMonitor),
            isNull(), eq(0), isNull(), isNull(), anyLong(), eq(uuid), eq(mockRemovalCallback));
    }

    private void verifyNoProposalsExecuted() {
        verify(executor, never()).executeProposals(anySet(), anySet(), any(), any(), any(), any(), any(), any(), any(), any());
    }
}
