package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.DescribeBrokerRemovalsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DescribeBrokerRemovalsResponseTest {
    @Test
    public void testErrorCountsWithTopLevelError() {
        DescribeBrokerRemovalsResponseData data = new DescribeBrokerRemovalsResponseData()
                .setErrorCode(Errors.NOT_CONTROLLER.code())
                .setErrorMessage("This broker is not the controller")
                .setRemovedBrokers(Arrays.asList(
                        new DescribeBrokerRemovalsResponseData.BrokerRemovalResponse().setBrokerId(1)
                                .setRemovalErrorCode(Errors.PLAN_COMPUTATION_FAILED.code()),
                        new DescribeBrokerRemovalsResponseData.BrokerRemovalResponse().setBrokerId(2)
                ));
        Map<Errors, Integer> errorCounts = new DescribeBrokerRemovalsResponse(data).errorCounts();
        assertEquals(1, errorCounts.size());
        assertEquals(Integer.valueOf(1), errorCounts.get(Errors.NOT_CONTROLLER));
    }

    /**
     * Removal error codes do not indicate errors with the API but rather the removal operation
     */
    @Test
    public void testErrorCountsWithNoTopLevelErrorShouldReturnZeroCountOfErrors() {
        DescribeBrokerRemovalsResponseData data = new DescribeBrokerRemovalsResponseData()
                .setErrorCode(Errors.NONE.code())
                .setRemovedBrokers(Arrays.asList(
                        new DescribeBrokerRemovalsResponseData.BrokerRemovalResponse().setBrokerId(0)
                                .setRemovalErrorCode(Errors.PLAN_COMPUTATION_FAILED.code()),
                        new DescribeBrokerRemovalsResponseData.BrokerRemovalResponse().setBrokerId(1)
                                .setRemovalErrorCode(Errors.PLAN_COMPUTATION_FAILED.code()),
                        new DescribeBrokerRemovalsResponseData.BrokerRemovalResponse().setBrokerId(2)
                                .setRemovalErrorCode(Errors.NONE.code()),
                        new DescribeBrokerRemovalsResponseData.BrokerRemovalResponse().setBrokerId(1)
                                .setRemovalErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                ));
        Map<Errors, Integer> errorCounts = new DescribeBrokerRemovalsResponse(data).errorCounts();
        assertEquals(1, errorCounts.size());
        assertEquals(Integer.valueOf(1), errorCounts.get(Errors.NONE));
    }
}
