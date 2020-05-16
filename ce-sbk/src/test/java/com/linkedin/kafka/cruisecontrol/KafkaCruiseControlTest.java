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
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.util.List;
import java.util.Set;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
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
    public static final ClusterModel CLUSTER_MODEL = new ClusterModel(
            new ModelGeneration(0, 0), 0.5);
    public static final Set<Integer> EMPTY_REQUESTED_DESTINATION_BROKER_IDS = Collections.emptySet();
    public static final List<String> EMPTY_GOALS = Collections.emptyList();

    @Mock
    private KafkaCruiseControlConfig cruiseControlConfig;

    @Mock
    private LoadMonitor loadMonitor;

    @Mock
    private ExecutorService executorService;

    @Mock
    private Executor executor;

    @Mock
    private AnomalyDetector anomalyDetector;

    @Mock
    private GoalOptimizer goalOptimizer;

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
        verify(executor, never()).setExecutionMode(anyBoolean());
        verify(executor, never()).executeProposals(anySet(), anySet(), any(), any(), any(), any(), any(), any(), any(), any());
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
        verify(executor, never()).setExecutionMode(anyBoolean());
        verify(executor, never()).executeProposals(anySet(), anySet(), any(), any(), any(), any(), any(), any(), any(), any());
    }


    @Test
    public void getProposals_ignoreProposalCache_explicitlyIgnoreProposalCache() throws Exception {
        when(goalOptimizer.defaultModelCompletenessRequirements()).thenReturn(REQUIREMENTS);
        when(goalOptimizer.modelCompletenessRequirementsForPrecomputing()).thenReturn(REQUIREMENTS);

        when(loadMonitor.clusterModel(
                eq(-1L), eq(0L), any(ModelCompletenessRequirements.class),
                eq(false), any(OperationProgress.class))).thenReturn(CLUSTER_MODEL);
        when(goalOptimizer.optimizations(
                any(ClusterModel.class), any(), any(OperationProgress.class), any(),
                any(), any(), anyBoolean(), any(), isNull(), eq(false))).thenReturn(optimizerResult);

        OptimizerResult actualResult = kafkaCruiseControl.getProposals(
                EMPTY_GOALS, REQUIREMENTS, operationProgress, false, false,
                null, false, false,
                true /* ignoreProposalCache */, false,
                EMPTY_REQUESTED_DESTINATION_BROKER_IDS, false);

        assertEquals(optimizerResult, actualResult);
        verify(loadMonitor).acquireForModelGeneration(eq(operationProgress));
        verify(loadMonitor).clusterModel(
                eq(-1L), eq(0L), any(ModelCompletenessRequirements.class),
                eq(false), eq(operationProgress));
        verify(goalOptimizer).optimizations(
                eq(CLUSTER_MODEL), anyList(), eq(operationProgress), any(), anySet(), anySet(),
                anyBoolean(), eq(EMPTY_REQUESTED_DESTINATION_BROKER_IDS), isNull(), eq(false));
    }

    @Test
    public void getProposals_ignoreProposalCache_weakerRequirements() throws Exception {
        when(goalOptimizer.defaultModelCompletenessRequirements()).thenReturn(REQUIREMENTS);
        when(goalOptimizer.modelCompletenessRequirementsForPrecomputing()).thenReturn(STRONGER_REQUIREMENTS);

        when(loadMonitor.clusterModel(eq(-1L), eq(0L), any(ModelCompletenessRequirements.class),
                eq(false), any(OperationProgress.class))).thenReturn(CLUSTER_MODEL);
        when(goalOptimizer.optimizations(
                any(ClusterModel.class), any(), any(OperationProgress.class), any(),
                any(), any(), anyBoolean(), any(), isNull(), eq(false))).thenReturn(optimizerResult);

        OptimizerResult actualResult = kafkaCruiseControl.getProposals(
                EMPTY_GOALS, REQUIREMENTS, operationProgress, false, false, null,
                false, false, false, false,
                Collections.emptySet(), false);

        assertEquals(optimizerResult, actualResult);
        verify(loadMonitor).acquireForModelGeneration(eq(operationProgress));
        verify(loadMonitor).clusterModel(
                eq(-1L), eq(0L), any(ModelCompletenessRequirements.class),
                eq(false), eq(operationProgress));
        verify(goalOptimizer).optimizations(
                eq(CLUSTER_MODEL), anyList(), eq(operationProgress), any(),
                anySet(), anySet(), eq(false), eq(EMPTY_REQUESTED_DESTINATION_BROKER_IDS), isNull(), eq(false));
    }

    @Test
    public void getProposals_ignoreProposalCache_throwsKafkaCruiseControlException() throws Exception {
        when(goalOptimizer.defaultModelCompletenessRequirements()).thenReturn(REQUIREMENTS);
        when(goalOptimizer.modelCompletenessRequirementsForPrecomputing()).thenReturn(REQUIREMENTS);
        when(loadMonitor.clusterModel(eq(-1L), eq(0L), any(ModelCompletenessRequirements.class),
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
}
