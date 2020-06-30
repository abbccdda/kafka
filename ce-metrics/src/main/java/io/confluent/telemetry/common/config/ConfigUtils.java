package io.confluent.telemetry.common.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// adapted from https://github.com/confluentinc/common/blob/master/config/src/main/java/io/confluent/common/config/ConfigUtils.java
// since we cannot depend on proprietary code or confluent-common in ce-metrics
// the plan is to move this code to AK as part of KIP-629, then we can remove this.
public class ConfigUtils {
    private static final Logger log = LoggerFactory.getLogger(ConfigUtils.class);

    public static <T> Map<String, T> translateDeprecated(Map<String, T> configs, String[][] aliasGroups) {
        Set<String> aliasSet = Stream.of(aliasGroups).flatMap(Stream::of).collect(Collectors.toSet());

        Map<String, T> newConfigs = configs.entrySet().stream()
            .filter(e -> !aliasSet.contains(e.getKey()))
            // filter out null values
            .filter(e -> Objects.nonNull(e.getValue()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Stream.of(aliasGroups).forEachOrdered(aliasGroup -> {
            String target = aliasGroup[0];

            List<String> deprecated = Stream.of(aliasGroup)
                .skip(1) // skip target
                .filter(configs::containsKey)
                .collect(Collectors.toList());

            if (deprecated.isEmpty()) {
                // No deprecated key(s) found.
                if (configs.containsKey(target)) {
                    newConfigs.put(target, configs.get(target));
                }
                return;
            }

            String aliasString = String.join(", ", deprecated);

            if (configs.containsKey(target)) {
                // Ignore the deprecated key(s) because the actual key was set.
                log.error(target + " was configured, as well as the deprecated alias(es) " +
                          aliasString + ".  Using the value of " + target);
                newConfigs.put(target, configs.get(target));
            } else if (deprecated.size() > 1) {
                log.error("The configuration keys " + aliasString + " are deprecated and may be " +
                          "removed in the future.  Additionally, this configuration is ambigous because " +
                          "these configuration keys are all aliases for " + target + ".  Please update " +
                          "your configuration to have only " + target + " set.");
                newConfigs.put(target, configs.get(deprecated.get(0)));
            } else {
                log.warn("Configuration key " + deprecated.get(0) + " is deprecated and may be removed " +
                         "in the future.  Please update your configuration to use " + target + " instead.");
                newConfigs.put(target, configs.get(deprecated.get(0)));
            }
        });

        return newConfigs;
    }
}
