// (Copyright) [2020 - 2020] Confluent, Inc.
package org.apache.kafka.trogdor.workload.partitioner;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class GaussianPartitionerConfig extends AbstractConfig {
    private static final ConfigDef CONFIG;

    static final String MEAN_CONFIG = "confluent.gaussian.partitioner.mean";
    static final String STD_CONFIG = "confluent.gaussian.partitioner.std";

    static {
        CONFIG = new ConfigDef()
            .define(MEAN_CONFIG, Type.INT, Importance.LOW,
                "The mean must be a int between 0 and the number of partitions. It indicates "
                + "the mean of the Gaussian which is used to distributed the records. Approx."
                + "40% of the records will be routed to that partition.")
            .define(STD_CONFIG, Type.INT, Importance.LOW,
                "The standard deviation indicates the standard deviation of the Gaussian which "
                + "is used to distribute the records. Approx. 68% of the records will be within "
                + "the standard deviation from the mean.");
    }

    public GaussianPartitionerConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }
}
